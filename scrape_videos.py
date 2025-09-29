import requests
from bs4 import BeautifulSoup
import threading
import queue
import time
from urllib.parse import urljoin
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os
import json
import re

# Headers
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.google.com/'
}

# Locks
sheets_lock = threading.Lock()

# Queues and flags
page_queue = queue.Queue()
detail_queue = queue.Queue()
stop_scraping = False
queueing_complete = False
all_video_data = []
page1_ids = set()

# Debug log file
DEBUG_LOG = "debug_log.txt"


# Write to debug log
def write_debug_log(message):
    with open(DEBUG_LOG, 'a', encoding='utf-8') as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')}: {message}\n")


# Load config from config.json
def load_config():
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        write_debug_log(f"Lỗi khi đọc config.json: {e}")
        return {}


# Load existing data from data.txt
def load_existing_data(config):
    data_file = config.get('DATA_TXT', 'data.txt')
    if os.path.exists(data_file):
        try:
            with open(data_file, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    write_debug_log("data.txt rỗng, khởi tạo mảng rỗng.")
                    return []
                existing_data = json.loads(content)
                # Ensure IDs are strings
                for v in existing_data:
                    if 'id' in v:
                        v['id'] = str(v['id'])
                return [v for v in existing_data if v.get('id') != 'N/A']
        except json.JSONDecodeError as e:
            write_debug_log(f"Lỗi khi đọc data.txt: {e}. Khởi tạo mảng rỗng.")
            with open(data_file, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False)
            return []
    else:
        write_debug_log(f"data.txt không tồn tại, tạo file mới.")
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False)
        return []


# Function to extract meta refresh URL
def get_meta_refresh_url(soup):
    meta = soup.find('meta', attrs={'http-equiv': 'refresh'})
    if meta and 'content' in meta.attrs:
        content = meta['content']
        parts = content.split(';')
        if len(parts) > 1:
            url_part = parts[1].strip().lower()
            if url_part.startswith('url='):
                return url_part[4:].strip()
    return None


# Function to handle requests with redirect following (HTTP and meta refresh)
def get_page_with_redirects(url, headers, max_redirects=10, delay=1):
    session = requests.Session()
    redirect_count = 0
    current_url = url

    while redirect_count < max_redirects:
        try:
            response = session.get(current_url, headers=headers, allow_redirects=True, timeout=10)
            response.raise_for_status()
            write_debug_log(f"Status code for {current_url}: {response.status_code}")

            # Parse the content to check for meta refresh
            soup = BeautifulSoup(response.text, 'html.parser')
            redirect_url = get_meta_refresh_url(soup)
            if redirect_url:
                redirect_url = urljoin(current_url, redirect_url)
                write_debug_log(f"Detected meta refresh redirect to: {redirect_url}")
                current_url = redirect_url
                redirect_count += 1
                time.sleep(delay)
                continue

            return response, soup, current_url
        except requests.exceptions.RequestException as e:
            write_debug_log(f"Lỗi khi truy cập {current_url}: {e}")
            return None, None, None

        time.sleep(delay)

    write_debug_log(f"Exceeded max redirects ({max_redirects}) for {url}")
    return None, None, None


# Scrape pagination page
def scrape_page(page_num, config, update_global=True):
    global stop_scraping
    if stop_scraping:
        return []
    url = f"{config['DOMAIN']}/" if page_num == 1 else f"{config['DOMAIN']}/page/{page_num}"
    print(f"Scraping page {page_num}")
    write_debug_log(f"Đang truy cập URL: {url}")

    response, soup, base_url = get_page_with_redirects(url, headers)
    if not response:
        return []

    html_content = response.text
    write_debug_log(f"Kích thước HTML: {len(html_content)} bytes")

    # Check for JavaScript rendering
    if 'application/json' in html_content or 'script' in html_content.lower():
        write_debug_log("Cảnh báo: Trang có thể yêu cầu JavaScript rendering.")

    # Find video items using post-\d+ class
    items = soup.find_all('div', class_=re.compile(r'post-\d+'))
    write_debug_log(f"Tìm thấy {len(items)} video-item với class post-*")

    # Debug: Log all div classes
    divs = soup.find_all('div')
    div_classes = [div.get('class', []) for div in divs]
    write_debug_log(f"Tổng số thẻ div: {len(divs)}, Classes: {div_classes[:10]}")

    # Save HTML for debugging
    with open(f'debug_page_{page_num}.html', 'w', encoding='utf-8') as f:
        f.write(html_content)

    if not items:
        write_debug_log(f"Không tìm thấy video-item trên trang {page_num}.")
        stop_scraping = True
        return []

    write_debug_log(f"Trang {page_num}: Tìm thấy {len(items)} video-item")

    video_data = []
    for item in items:
        # Get video ID from id="post-XXXXX"
        video_id = str(item.get('id', '').replace('post-', '')) if item.get('id') else 'N/A'
        if video_id == 'N/A':
            write_debug_log(f"Skipping item with no ID on page {page_num}")
            continue
        # Get title and link
        a_tag = item.find('a', class_='thumbnail-link')
        title = a_tag.get('title', 'N/A') if a_tag else 'N/A'
        code = title.split(' ')[0] if title != 'N/A' else 'N/A'
        link = urljoin(base_url, a_tag.get('href', 'N/A')) if a_tag else 'N/A'

        # Get ribbons
        ribboni = item.find('p', class_='ribboni')
        ribboni = ribboni.text.strip() if ribboni else 'N/A'
        ribbons = item.find('p', class_='ribbons')
        ribbons = ribbons.text.strip() if ribbons else 'N/A'

        # Get categories from class
        classes = item.get('class', [])
        categories = [cls.replace('category-', '') for cls in classes if cls.startswith('category-')]

        data = {
            'page': page_num,
            'id': video_id,
            'code': code,
            'link': link,
            'ribbons': ribbons,
            'ribboni': ribboni,
            'categories': categories if categories else ['N/A']
        }
        write_debug_log(f"Scraped video from page {page_num}: {json.dumps(data, ensure_ascii=False)}")
        video_data.append(data)
        if update_global:
            with sheets_lock:
                existing_index = next((i for i, v in enumerate(all_video_data) if v['id'] == video_id), None)
                if existing_index is not None:
                    update_data = {k: v for k, v in data.items() if k != 'page'}
                    all_video_data[existing_index].update(update_data)
                else:
                    all_video_data.append(data)

    # Check for overlap with page 1 IDs
    if page_num > 1:
        current_ids = {v['id'] for v in video_data if v['id'] != 'N/A'}
        overlap = len(current_ids & page1_ids)
        if overlap >= 15:
            stop_scraping = True
            print(f"Stopped at page {page_num} due to overlap with page 1 ({overlap} IDs)")
            write_debug_log(f"Detected loop back on page {page_num}, overlap {overlap}, stopping.")

    return video_data


# Check if page 1 has new videos by comparing video IDs
def has_new_videos_page1(config):
    global page1_ids
    existing_ids = {str(v['id']) for v in all_video_data if v['id'] != 'N/A'}
    write_debug_log(f"Existing IDs: {existing_ids}")
    temp_video_data = scrape_page(1, config, update_global=False)
    page1_ids = {str(v['id']) for v in temp_video_data if v['id'] != 'N/A'}
    write_debug_log(f"New IDs from page 1: {page1_ids}")
    new_videos = page1_ids - existing_ids
    write_debug_log(f"New video IDs detected: {new_videos}")
    return bool(new_videos)


# Worker for pagination
def worker(config):
    while not stop_scraping:
        try:
            page_num = page_queue.get_nowait()
        except queue.Empty:
            break

        scrape_page(page_num, config)
        page_queue.task_done()
        time.sleep(0.5)


# Convert likes/dislikes to number
def convert_likes_dislikes(value):
    try:
        return int(value.replace('.', ''))
    except (ValueError, AttributeError):
        return 0


# Scrape detail page
def scrape_detail(detail_link):
    response, soup, base_url = get_page_with_redirects(detail_link, headers)
    if not response:
        return None

    video_div = soup.find('div', id='video')
    if not video_div:
        write_debug_log(f"Không tìm thấy div#video trong {detail_link}")
        return None

    detail_data = {}
    detail_data['video_id'] = str(video_div.get('data-id', 'N/A'))
    if detail_data['video_id'] == 'N/A':
        write_debug_log(f"Không tìm thấy data-id trong div#video cho {detail_link}")
        return None

    comment_count = soup.find('span', class_='comment-count')
    detail_data['comment_count'] = convert_likes_dislikes(comment_count.text.strip()) if comment_count else 0

    meta_tags = soup.find_all('meta')
    detail_data['meta_description'] = next(
        (meta.get('content', 'N/A') for meta in meta_tags if meta.get('name') == 'description'), 'N/A')

    return detail_data


# Worker for details
def detail_worker(config):
    while not (queueing_complete and detail_queue.empty()):
        try:
            detail_link = detail_queue.get(timeout=5)
            if detail_link is None:
                detail_queue.task_done()
                break
            detail_data = scrape_detail(detail_link)
            if detail_data:
                write_debug_log(f"Scraped detail: {detail_link}, Data: {json.dumps(detail_data, ensure_ascii=False)}")
                with sheets_lock:
                    for video in all_video_data:
                        if video['id'] == detail_data['video_id'] and video['link'] == detail_link:
                            update_data = {k: v for k, v in detail_data.items() if k != 'video_id'}
                            video.update(update_data)
                            break
            detail_queue.task_done()
            time.sleep(config.get('DETAIL_DELAY', 1))
        except queue.Empty:
            if queueing_complete:
                break
        except Exception as e:
            write_debug_log(f"Lỗi trong detail_worker: {e}")
            detail_queue.task_done()


# Save data.txt as JSON, sorted by page (asc) and id (desc)
def save_data_txt(config):
    try:
        df = pd.DataFrame(all_video_data)
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        df = df.drop_duplicates(subset=['id', 'link'], keep='last')
        df = df.sort_values(by=['page', 'id'], ascending=[True, False])
        # Ensure only specified columns are saved
        columns = ['page', 'id', 'code', 'link', 'ribbons', 'ribboni', 'categories', 'comment_count',
                   'meta_description']
        df = df[[col for col in columns if col in df.columns]]
        sorted_data = df.to_dict('records')
        with open(config.get('DATA_TXT', 'data.txt'), 'w', encoding='utf-8') as f:
            json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        write_debug_log(f"Lỗi khi lưu data.txt: {e}")


# Update Google Sheets
def update_google_sheets(config):
    if not os.path.exists(config.get('CREDENTIALS_FILE', '')):
        write_debug_log("Không tìm thấy CREDENTIALS_FILE, bỏ qua Google Sheets.")
        return
    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(config['CREDENTIALS_FILE'], config.get('SCOPE', []))
        client = gspread.authorize(creds)
        sheet = client.open_by_key(config.get('SHEET_ID', '')).sheet1

        if all_video_data:
            df = pd.DataFrame(all_video_data)
            df['id'] = pd.to_numeric(df['id'], errors='coerce')
            df = df.drop_duplicates(subset=['id', 'link'], keep='last')
            df = df.sort_values(by=['page', 'id'], ascending=[True, False])
            # Ensure only specified columns are saved
            columns = ['page', 'id', 'code', 'link', 'ribbons', 'ribboni', 'categories', 'comment_count',
                       'meta_description']
            df = df[[col for col in columns if col in df.columns]]
            # Convert list columns to strings for Sheets
            for col in ['categories']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
            values = [df.columns.values.tolist()] + df.values.tolist()

            df.to_csv(config.get('TEMP_CSV', 'temp.csv'), index=False, encoding='utf-8')
            with sheets_lock:
                sheet.clear()
                sheet.update(values=values, range_name='A1')
    except Exception as e:
        write_debug_log(f"Lỗi khi cập nhật Google Sheets: {e}")


# Main function
def main():
    global stop_scraping, queueing_complete
    start_total = time.time()
    steps = []
    # Clear debug log
    if os.path.exists(DEBUG_LOG):
        os.remove(DEBUG_LOG)
    # Load config
    config = load_config()
    if not config:
        write_debug_log("Không thể đọc config.json, thoát!")
        return
    # Step 1: Load existing data from data.txt
    all_video_data.extend(load_existing_data(config))
    steps.append(f"Đã đọc file data.txt, load {len(all_video_data)} video.")
    # Step 2: Determine run mode
    run_mode = config.get('RUN_MODE', 'real')
    if run_mode == 'test':
        max_pages = 1
        steps.append("Chạy ở chế độ TEST: chỉ quét trang 1.")
    else:
        # Step 3: Check page 1 for new videos
        has_new = has_new_videos_page1(config)
        if has_new:
            steps.append("Quét trang 1, có video mới.")
            max_pages = config.get('MAX_PAGES', 100)
            steps.append(f"Quét pagination từ trang 1 đến {max_pages}.")
        else:
            steps.append("Quét trang 1, không có video mới.")
            max_pages = 3
            steps.append("Quét pagination trang 1 đến 3.")
    # Step 4: Scrape pagination
    for page_num in range(1, max_pages + 1):
        page_queue.put(page_num)
    threads = []
    for _ in range(config.get('NUM_THREADS', 5)):
        t = threading.Thread(target=worker, args=(config,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    # Step 5: Prepare pending details
    pending_links = [video['link'] for video in all_video_data if video.get('link', 'N/A') != 'N/A']
    pending_links = list(set(pending_links))
    steps.append(f"Quét details cho {len(pending_links)} video.")
    write_debug_log(f"Total detail links to scrape: {len(pending_links)}")
    # Step 6: Queue detail links
    for link in pending_links:
        detail_queue.put(link)
    queueing_complete = True
    # Step 7: Scrape details with multiple threads
    detail_threads_list = []
    for _ in range(min(config.get('DETAIL_THREADS', 3), len(pending_links))):
        t = threading.Thread(target=detail_worker, args=(config,))
        t.start()
        detail_threads_list.append(t)

    for t in detail_threads_list:
        t.join()
    while not detail_queue.empty():
        try:
            detail_queue.get_nowait()
            detail_queue.task_done()
        except queue.Empty:
            break
    # Summary
    total_pages = len(set(video['page'] for video in all_video_data if video['page'] != 'N/A'))
    total_videos = len(all_video_data)
    total_detailed = len([video for video in all_video_data if 'comment_count' in video])
    elapsed_total = time.time() - start_total
    print("Tổng quát các bước thực hiện:")
    for step in steps:
        print("- " + step)
    print(f"Tổng kết: {total_pages} trang, {total_videos} video, {total_detailed} video chi tiết, {elapsed_total:.2f}s")
    write_debug_log(
        f"Tổng kết: {total_pages} trang, {total_videos} video, {total_detailed} video chi tiết, {elapsed_total:.2f}s")
    if all_video_data:
        save_data_txt(config)
        update_google_sheets(config)


if __name__ == "__main__":
    main()
