# -*- coding: utf-8 -*-
"""
統合スクリプト：
1. Yahoo!ニュース検索結果から記事リストを取得。
2. そのリストを単一スプレッドシートの「Yahoo」シートに追記。
3. 追記された記事リストから、前日15:00〜当日14:59:59の分を抽出し、
   記事本文とコメントを取得。
4. 取得した詳細データを同じスプレッドシートの当日日付タブに書き込み。

認証: GitHub Secretsの GOOGLE_CREDENTIALS または GCP_SERVICE_ACCOUNT_KEY を使用。
"""

import os
import json
import time
import re
import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Set

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# ====== 設定 ======
# 指定された単一のスプレッドシートID
SHARED_SPREADSHEET_ID = "1vy0quUujxw5hagwATB5W87wD4tVTd5d8qxgt8IekRTY" 

# 【ステップ1, 2】ニュースリスト取得用の設定
KEYWORD = "日産"
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"

# 【ステップ3, 4】本文・コメントの出力先
DEST_SPREADSHEET_ID = SHARED_SPREADSHEET_ID

# 本文・コメント取得設定
MAX_BODY_PAGES = 10
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_TOTAL_COMMENTS = 5000

TZ_JST = timezone(timedelta(hours=9))

# ====== 共通ユーティリティ ======

def jst_now() -> datetime:
    return datetime.now(TZ_JST)

def format_datetime(dt_obj) -> str:
    """datetimeオブジェクトを指定の形式で文字列に変換します (YYYY/MM/DD HH:MM)"""
    return dt_obj.strftime("%Y/%m/%d %H:%M")

def format_yy_m_d_hm(dt: datetime) -> str:
    """yy/m/d HH:MM に整形（先頭ゼロの月日を避ける）"""
    yy = dt.strftime("%y")
    m = str(int(dt.strftime("%m")))
    d = str(int(dt.strftime("%d")))
    hm = dt.strftime("%H:%M")
    return f"{yy}/{m}/{d} {hm}"

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    """
    ソースC列（投稿日）を JST datetime に変換
    許容: "MM/DD HH:MM"（年は当年補完）, "YYYY/MM/DD HH:MM", "YYYY/MM/DD HH:MM:SS", Excelシリアル
    """
    if raw is None: return None
    if isinstance(raw, str):
        s = raw.strip()
        for fmt in ("%m/%d %H:%M", "%Y/%m/%d %H:%M", "%Y/%m/%d %H:%M:%S"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M":
                    dt = dt.replace(year=today_jst.year)
                return dt.replace(tzinfo=TZ_JST)
            except ValueError:
                pass
        return None
    if isinstance(raw, (int, float)):
        epoch = datetime(1899, 12, 30, tzinfo=TZ_JST)  # Excel起点
        return epoch + timedelta(days=float(raw))
    if isinstance(raw, datetime):
        return raw.astimezone(TZ_JST) if raw.tzinfo else raw.replace(tzinfo=TZ_JST)
    return None

# ====== 認証 ======

def build_gspread_client() -> gspread.Client:
    """
    gspreadクライアントを構築します。
    環境変数 GOOGLE_CREDENTIALS または GCP_SERVICE_ACCOUNT_KEY のいずれか、
    またはローカルの credentials.json を使用します。
    """
    try:
        # main2.pyの認証方式 (GOOGLE_CREDENTIALS) を優先
        creds_str = os.environ.get("GOOGLE_CREDENTIALS")
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        if creds_str:
            info = json.loads(creds_str)
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)
            return gspread.authorize(credentials)
        else:
            # main1.pyの認証方式 (GCP_SERVICE_ACCOUNT_KEY または ローカルファイル)
            creds_str_alt = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
            if creds_str_alt:
                credentials = json.loads(creds_str_alt)
            else:
                credentials = json.load(open('credentials.json'))
                
            return gspread.service_account_from_dict(credentials)
            
    except Exception as e:
        raise RuntimeError(f"Google認証に失敗: {e}")


# =========================================================================
# 【ステップ1】 Yahoo!ニュースリスト取得 (main1.pyのロジック)
# =========================================================================

def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    """
    Seleniumを使用してYahoo!ニュースから指定されたキーワードの記事を取得します。
    """
    print("🚀 Yahoo!ニュース検索開始...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1280,1024")

    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    except Exception as e:
        print(f"❌ WebDriverの初期化に失敗しました: {e}")
        return []
        
    search_url = f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local"
    driver.get(search_url)
    time.sleep(5) 

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    articles = soup.find_all("li", class_=re.compile("sc-1u4589e-0"))
    articles_data = []

    for article in articles:
        try:
            # タイトル
            title_tag = article.find("div", class_=re.compile("sc-3ls169-0"))
            title = title_tag.text.strip() if title_tag else ""
            
            # URL
            link_tag = article.find("a", href=True)
            url = link_tag["href"] if link_tag else ""
            
            # 投稿日
            time_tag = article.find("time")
            date_str = time_tag.text.strip() if time_tag else ""
            formatted_date = ""
            if date_str:
                date_str = re.sub(r'\([月火水木金土日]\)', '', date_str).strip()
                try:
                    dt_obj = datetime.strptime(date_str, "%Y/%m/%d %H:%M")
                    formatted_date = format_datetime(dt_obj)
                except:
                    formatted_date = date_str

            # 引用元（ソース）
            source_text = ""
            source_tag = article.find("div", class_="sc-n3vj8g-0 yoLqH")
            if source_tag:
                inner = source_tag.find("div", class_="sc-110wjhy-8 bsEjY")
                if inner and inner.span:
                    candidate = inner.span.text.strip()
                    if not candidate.isdigit():
                        source_text = candidate
            if not source_text or source_text.isdigit():
                alt_spans = article.find_all(["span", "div"], string=True)
                for s in alt_spans:
                    text = s.text.strip()
                    if 2 <= len(text) <= 20 and not text.isdigit() and re.search(r'[ぁ-んァ-ン一-龥A-Za-z]', text):
                        source_text = text
                        break

            if title and url:
                articles_data.append({
                    "タイトル": title,
                    "URL": url,
                    "投稿日": formatted_date if formatted_date else "取得不可",
                    "引用元": source_text
                })
        except Exception:
            continue

    print(f"✅ Yahoo!ニュース件数: {len(articles_data)} 件取得")
    return articles_data

def write_news_list_to_source(gc: gspread.Client, articles: list[dict]):
    """
    【ステップ2】取得した記事リストをSOURCE_SPREADSHEETの'Yahoo'シートに追記します。
    """
    for attempt in range(5):
        try:
            sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
            try:
                worksheet = sh.worksheet(SOURCE_SHEET_NAME)
            except gspread.exceptions.WorksheetNotFound:
                worksheet = sh.add_worksheet(title=SOURCE_SHEET_NAME, rows="1", cols="4")
                worksheet.append_row(['タイトル', 'URL', '投稿日', '引用元'])

            existing_data = worksheet.get_all_values()
            existing_urls = set(row[1] for row in existing_data[1:] if len(row) > 1)

            # A:タイトル / B:URL / C:投稿日 / D:引用元 の形式で新しいデータを作成
            new_data = [[a['タイトル'], a['URL'], a['投稿日'], a['引用元']] for a in articles if a['URL'] not in existing_urls]
            
            if new_data:
                worksheet.append_rows(new_data, value_input_option='USER_ENTERED')
                print(f"✅ SOURCEシートに {len(new_data)} 件追記しました。")
            else:
                print("⚠️ SOURCEシートに追記すべき新しいデータはありません。")
            return
            
        except gspread.exceptions.APIError as e:
            print(f"⚠️ Google API Error (attempt {attempt + 1}/5): {e}")
            time.sleep(5 + random.random() * 5)

    raise RuntimeError("❌ SOURCEスプレッドシートへの書き込みに失敗しました（5回試行しても成功せず）")


# =========================================================================
# 【ステップ3, 4】 本文・コメント取得 (main2.pyのロジック)
# =========================================================================

# --- DESTシート操作 (main2.py) ---
def ensure_today_sheet(sh: gspread.Spreadsheet, today_tab: str) -> gspread.Worksheet:
    """当日タブが存在しない場合は作成します"""
    try:
        ws = sh.worksheet(today_tab)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=today_tab, rows="3000", cols="300")
    return ws

def get_existing_urls(ws: gspread.Worksheet) -> Set[str]:
    """DESTシートのC列（URL）から既存URLを取得"""
    vals = ws.col_values(3)
    return set(vals[1:] if len(vals) > 1 else [])

def ensure_ae_header(ws: gspread.Worksheet) -> None:
    """A〜E列のヘッダーを保証"""
    head = ws.row_values(1)
    target = ["ソース", "タイトル", "URL", "投稿日", "掲載元"]
    if head[:len(target)] != target:
        ws.update('A1', [target])

def ensure_body_comment_headers(ws: gspread.Worksheet, max_comments: int) -> None:
    """F列以降の本文・コメントヘッダーを保証"""
    current = ws.row_values(1)
    base = ["ソース", "タイトル", "URL", "投稿日", "掲載元"]
    body_headers = [f"本文({i}ページ)" for i in range(1, 11)]
    comments_count = ["コメント数"]
    comment_headers = [f"コメント{i}" for i in range(1, max(1, max_comments) + 1)]
    target = base + body_headers + comments_count + comment_headers
    if current != target:
        ws.update('A1', [target])


# --- データ転送 (main2.py) ---
def transfer_a_to_e(gc: gspread.Client, dest_ws: gspread.Worksheet) -> int:
    """
    SOURCEシートから「前日15:00〜当日14:59:59」のデータをDESTシートのA〜E列に転送
    """
    sh_src = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    ws_src = sh_src.worksheet(SOURCE_SHEET_NAME)
    rows = ws_src.get('A:D')

    now = jst_now()
    # 前日15:00:00 JST
    start = (now - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
    # 当日14:59:59 JST
    end = now.replace(hour=14, minute=59, second=59, microsecond=0)

    ensure_ae_header(dest_ws)
    existing = get_existing_urls(dest_ws)

    to_append: List[List[str]] = []
    for i, r in enumerate(rows):
        if i == 0: continue
        title = r[0].strip() if len(r) > 0 and r[0] else ""
        url = r[1].strip() if len(r) > 1 and r[1] else ""
        posted_raw = r[2] if len(r) > 2 else ""
        site = r[3].strip() if len(r) > 3 and r[3] else ""
        if not title or not url: continue
        
        dt = parse_post_date(posted_raw, now)
        if not dt or not (start <= dt <= end):
            continue # 時間範囲外
            
        if url in existing:
            continue # DESTシートに重複

        # A:ソース / B:タイトル / C:URL / D:投稿日 / E:掲載元
        to_append.append(["Yahoo", title, url, format_yy_m_d_hm(dt), site])

    if to_append:
        dest_ws.append_rows(to_append, value_input_option="USER_ENTERED")
    return len(to_append)


# --- 本文・コメント取得 (main2.py) ---
def fetch_article_pages(base_url: str) -> Tuple[str, str, List[str]]:
    """記事本文を取得します"""
    title = "取得不可"
    article_date = "取得不可"
    bodies: List[str] = []
    for page in range(1, MAX_BODY_PAGES + 1):
        url = base_url if page == 1 else f"{base_url}?page={page}"
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            res.raise_for_status()
        except Exception:
            break
        soup = BeautifulSoup(res.text, "html.parser")
        
        if page == 1:
            t = soup.find("title")
            if t and t.get_text(strip=True):
                title = t.get_text(strip=True).replace(" - Yahoo!ニュース", "")
            time_tag = soup.find("time")
            if time_tag:
                article_date = time_tag.get_text(strip=True)

        body_text = ""
        article = soup.find("article")
        if article:
            ps = article.find_all("p")
            body_text = "\n".join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))
        
        if not body_text:
            main = soup.find("main")
            if main:
                ps = main.find_all("p")
                body_text = "\n".join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))

        if not body_text or (bodies and body_text == bodies[-1]):
            break
        bodies.append(body_text)
    return title, article_date, bodies

def fetch_comments_with_selenium(base_url: str) -> List[str]:
    """記事コメントをSeleniumで取得します"""
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280,2000")
    
    try:
        driver = webdriver.Chrome(options=options)
    except Exception:
        try:
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        except Exception as e:
            print(f"❌ WebDriver初期化エラー: {e}")
            return []

    comments: List[str] = []
    last_tail: Optional[str] = None
    page = 1
    try:
        while True:
            c_url = f"{base_url}/comments?page={page}"
            driver.get(c_url)
            time.sleep(2.0)

            soup = BeautifulSoup(driver.page_source, "html.parser")

            selectors = [
                "p.sc-169yn8p-10",
                "p[data-ylk*='cm_body']",
                "p[class*='comment']",
                "div.commentBody, p.commentBody",
                "div[data-ylk*='cm_body']"
            ]

            p_candidates = []
            for sel in selectors:
                p_candidates.extend(soup.select(sel))

            page_comments = [p.get_text(strip=True) for p in p_candidates if p.get_text(strip=True)]
            page_comments = list(dict.fromkeys(page_comments))

            if not page_comments: break

            if last_tail is not None and page_comments and page_comments[0] == last_tail: break

            comments.extend(page_comments)

            if len(comments) >= MAX_TOTAL_COMMENTS:
                comments = comments[:MAX_TOTAL_COMMENTS]
                break

            last_tail = page_comments[-1]
            page += 1

    finally:
        driver.quit()

    return comments

def write_bodies_and_comments(ws: gspread.Worksheet) -> None:
    """
    DESTシートのF列以降に本文とコメントを書き込み
    """
    urls = ws.col_values(3)[1:]
    total = len(urls)
    print(f"🔎 本文・コメント取得対象URL: {total} 件")
    if total == 0: return

    rows_data: List[List[str]] = []
    max_comments = 0
    # スプレッドシートの行番号(2から開始)
    for row_idx, url in enumerate(urls, start=2):
        print(f"  - ({row_idx-1}/{total}) {url}")
        try:
            _title, _date, bodies = fetch_article_pages(url)
            comments = fetch_comments_with_selenium(url)

            body_cells = bodies[:MAX_BODY_PAGES] + [""] * (MAX_BODY_PAGES - len(bodies))
            cnt = len(comments)
            row = body_cells + [cnt] + comments
            rows_data.append(row)
            if cnt > max_comments:
                max_comments = cnt
        except Exception as e:
            print(f"    ! Error: {e}")
            rows_data.append(([""] * MAX_BODY_PAGES) + [0])

    # データ行の長さを最大コメント数に合わせて調整
    need_cols = MAX_BODY_PAGES + 1 + max_comments
    for i in range(len(rows_data)):
        if len(rows_data[i]) < need_cols:
            rows_data[i].extend([""] * (need_cols - len(rows_data[i])))

    # ヘッダー整備
    ensure_body_comment_headers(ws, max_comments=max_comments)

    # F2 から一括更新
    if rows_data:
        ws.update("F2", rows_data)
        print(f"✅ F列以降に本文・コメントを書き込み完了: {len(rows_data)} 行")


# =========================================================================
# 【メイン処理】
# =========================================================================

def main():
    gc = build_gspread_client()
    
    # 1. Yahoo!ニュースリストを取得
    yahoo_news_articles = get_yahoo_news_with_selenium(KEYWORD)
    
    if not yahoo_news_articles:
        print("💡 新しい記事が見つかりませんでした。処理を終了します。")
        return

    # 2. リストを一時的なSOURCEシートに書き込み
    print(f"\n📄 Spreadsheet ID: {SHARED_SPREADSHEET_ID} / Sheet: {SOURCE_SHEET_NAME}")
    write_news_list_to_source(gc, yahoo_news_articles)
    
    # 3. DESTシートを準備
    dest_sh = gc.open_by_key(DEST_SPREADSHEET_ID)
    today_tab = jst_now().strftime("%y%m%d") # yymmdd 形式のタブ名
    ws = ensure_today_sheet(dest_sh, today_tab)
    print(f"\n📄 DEST Sheet: {today_tab}")

    # 4. SOURCEからDESTへ「前日15:00〜当日14:59:59」のデータを転送 (A〜E列)
    added = transfer_a_to_e(gc, ws)
    print(f"📝 DESTシートに新規追加: {added} 行")
    
    # 5. DESTシートの記事に対して本文・コメントを取得 (F列以降)
    # 当日タブの全行に対して実行し、前回の未完了分もカバーします。
    if ws.get_all_values(value_render_option='UNFORMATTED_VALUE'):
        write_bodies_and_comments(ws)
    else:
        print("⚠️ 当日シートに行が存在しないため、本文・コメントの取得をスキップします。")


if __name__ == "__main__":
    main()
