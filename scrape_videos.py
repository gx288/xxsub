import requests
from bs4 import BeautifulSoup
import threading
import queue
import time
from urllib.parse import urljoin, urlparse
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
import os
import json
import re
import random

# Locks
sheets_lock = threading.Lock()

# Queues and flags
page_queue = queue.Queue()
detail_queue = queue.Queue()
stop_scraping = False
queueing_complete = False
all_video_data = []

# Debug log file
DEBUG_LOG = "debug_log.txt"

# List of User-Agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:130.0) Gecko/20100101 Firefox/130.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
]

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
        print(f"Lỗi khi đọc config.json: {e}")
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
                    print("data.txt rỗng, khởi tạo mảng rỗng.")
                    write_debug_log("data.txt rỗng, khởi tạo mảng rỗng.")
                    return []
                existing_data = json.loads(content)
                for v in existing_data:
                    if 'id' in v:
                        v['id'] = str(v['id'])
                return [v for v in existing_data if v.get('id') != 'N/A']
        except json.JSONDecodeError as e:
            print(f"Lỗi khi đọc data.txt: {e}. Khởi tạo mảng rỗng.")
            write_debug_log(f"Lỗi khi đọc data.txt: {e}. Khởi tạo mảng rỗng.")
            with open(data_file, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False)
            return []
    else:
        print(f"data.txt không tồn tại, tạo file mới.")
        write_debug_log(f"data.txt không tồn tại, tạo file mới.")
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False)
        return []

# Follow redirects (HTTP and meta refresh)
def follow_redirects(url, max_redirects=5, headers=None):
    session = requests.Session()
    session.max_redirects = max_redirects
    redirects_followed = 0
    current_url = url
    while redirects_followed < max_redirects:
        try:
            headers = headers or {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
                'Connection': 'keep-alive',
                'Accept-Encoding': 'gzip, deflate, br',
                'Upgrade-Insecure-Requests': '1'
            }
            response = session.get(current_url, headers=headers, timeout=10, allow_redirects=False)
            write_debug_log(f"Request to {current_url}, Status: {response.status_code}")
            if 300 <= response.status_code < 400:
                current_url = response.headers.get('Location')
                if not current_url:
                    write_debug_log(f"No Location header in redirect for {current_url}")
                    return None, None
                current_url = urljoin(current_url, current_url)
                redirects_followed += 1
                write_debug_log(f"HTTP Redirect to {current_url}")
                continue
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            meta_refresh = soup.find('meta', attrs={'http-equiv': re.compile(r'refresh', re.I)})
            if meta_refresh and 'content' in meta_refresh.attrs:
                content = meta_refresh['content']
                match = re.search(r'url=(.+)', content, re.I)
                if match:
                    current_url = urljoin(current_url, match.group(1).strip())
                    redirects_followed += 1
                    write_debug_log(f"Meta Refresh Redirect to {current_url}")
                    continue
            return response, current_url
        except requests.exceptions.RequestException as e:
            write_debug_log(f"Error following redirect for {current_url}: {e}")
            return None, None
    write_debug_log(f"Max redirects ({max_redirects}) reached for {url}")
    return None, None

# Scrape pagination page
def scrape_page(page_num, config, update_global=True):
    global stop_scraping
    if stop_scraping:
        return []
    url = f"{config['DOMAIN']}/" if page_num == 1 else f"{config['DOMAIN']}/new/{page_num}/"
    print(f"Đang truy cập URL: {url}")
    write_debug_log(f"Đang truy cập URL: {url}")
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
            'Referer': config['DOMAIN'],
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate, br',
            'Upgrade-Insecure-Requests': '1'
        }
        response, final_url = follow_redirects(url, headers=headers)
        if not response:
            print(f"Lỗi khi truy cập trang {page_num}: Không thể theo dõi redirect")
            write_debug_log(f"Lỗi khi truy cập trang {page_num}: Không thể theo dõi redirect")
            return []
        print(f"Status code: {response.status_code}, Final URL: {final_url}")
        write_debug_log(f"Status code: {response.status_code}, Final URL: {final_url}")
        html_content = response.text
        print(f"Kích thước HTML: {len(html_content)} bytes")
        write_debug_log(f"Kích thước HTML: {len(html_content)} bytes")
        if 'application/json' in html_content or 'script' in html_content.lower():
            print("Cảnh báo: Trang có thể yêu cầu JavaScript rendering.")
            write_debug_log("Cảnh báo: Trang có thể yêu cầu JavaScript rendering.")
    except Exception as e:
        print(f"Lỗi khi truy cập trang {page_num}: {e}")
        write_debug_log(f"Lỗi khi truy cập trang {page_num}: {e}")
        return []

    soup = BeautifulSoup(html_content, 'html.parser')
    items = soup.find_all('div', class_='ht_grid_1_4 ht_grid_m_1_2')
    if not items:
        print("Không tìm thấy video-item với class 'ht_grid_1_4 ht_grid_m_1_2'.")
        write_debug_log("Không tìm thấy video-item với class 'ht_grid_1_4 ht_grid_m_1_2'.")
        items = soup.find_all('div', class_=re.compile(r'post-\d+'))
        print(f"Thử tìm với class post-*: Tìm thấy {len(items)} video-item")
        write_debug_log(f"Thử tìm với class post-*: Tìm thấy {len(items)} video-item")

    divs = soup.find_all('div')
    div_classes = [div.get('class', []) for div in divs]
    print(f"Tổng số thẻ div: {len(divs)}, Classes: {div_classes[:10]}")
    write_debug_log(f"Tổng số thẻ div: {len(divs)}, Classes: {div_classes[:10]}")
    with open(f'debug_page_{page_num}.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    if not items:
        print(f"Không tìm thấy video-item trên trang {page_num}. Lưu HTML để debug.")
        write_debug_log(f"Không tìm thấy video-item trên trang {page_num}.")
        stop_scraping = True
        return []

    print(f"Trang {page_num}: Tìm thấy {len(items)} video-item")
    write_debug_log(f"Trang {page_num}: Tìm thấy {len(items)} video-item")

    video_data = []
    for item in items:
        video_id = str(item.get('id', '').replace('post-', '')) if item.get('id') else 'N/A'
        if video_id == 'N/A':
            continue
        a_tag = item.find('a', class_='thumbnail-link')
        title = a_tag.get('title', 'N/A') if a_tag else 'N/A'
        link = urljoin(config['DOMAIN'], a_tag.get('href', 'N/A')) if a_tag else 'N/A'
        img_tag = item.find('img')
        source_tag = item.find('source', type='image/webp')
        thumbnail = source_tag.get('srcset', img_tag.get('src', 'N/A')) if source_tag or img_tag else 'N/A'
        thumbnail = urljoin(config['DOMAIN'], thumbnail.split(' ')[0]) if thumbnail != 'N/A' and not thumbnail.startswith('http') else thumbnail.split(' ')[0]
        ribboni = item.find('p', class_='ribboni')
        ribboni = ribboni.text.strip() if ribboni else 'N/A'
        ribbons = item.find('p', class_='ribbons')
        ribbons = ribbons.text.strip() if ribbons else 'N/A'
        ribbont = item.find('span', class_='ribbont')
        duration = ribbont.text.strip() if ribbont else 'N/A'
        classes = item.get('class', [])
        categories = [cls.replace('category-', '') for cls in classes if cls.startswith('category-')]
        tags = [cls.replace('tag-', '') for cls in classes if cls.startswith('tag-')]
        date = 'N/A'
        views = 0

        data = {
            'page': page_num,
            'id': video_id,
            'title': title,
            'link': link,
            'thumbnail': thumbnail,
            'ribboni': ribboni,
            'ribbons': ribbons,
            'duration': duration,
            'date': date,
            'categories': categories if categories else ['N/A'],
            'tags': tags if tags else ['N/A'],
            'views_pagination': views
        }
        print(f"Scraped video from page {page_num}: {data}")
        write_debug_log(f"Scraped video from page {page_num}: {json.dumps(data, ensure_ascii=False)}")
        video_data.append(data)
        if update_global:
            with sheets_lock:
                existing_index = next((i for i, v in enumerate(all_video_data) if v['id'] == video_id), None)
                if existing_index is not None:
                    all_video_data[existing_index].update(data)
                else:
                    all_video_data.append(data)
    return video_data

# Check if page 1 has new videos
def has_new_videos_page1(config):
    existing_ids = {str(v['id']) for v in all_video_data if v['id'] != 'N/A'}
    print(f"Existing IDs: {existing_ids}")
    write_debug_log(f"Existing IDs: {existing_ids}")
    temp_video_data = scrape_page(1, config, update_global=False)
    new_ids = {str(v['id']) for v in temp_video_data if v['id'] != 'N/A'}
    print(f"New IDs from page 1: {new_ids}")
    write_debug_log(f"New IDs from page 1: {new_ids}")
    new_videos = new_ids - existing_ids
    print(f"New video IDs detected: {new_videos}")
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

# Convert views to number
def convert_views(views_str):
    try:
        views_str = views_str.lower().replace(',', '')
        if 'k' in views_str:
            return int(float(views_str.replace('k', '')) * 1000)
        elif 'm' in views_str:
            return int(float(views_str.replace('m', '')) * 1000000)
        return int(views_str)
    except (ValueError, AttributeError):
        return 0

# Convert likes/dislikes to number
def convert_likes_dislikes(value):
    try:
        return int(value.replace('.', ''))
    except (ValueError, AttributeError):
        return 0

# Convert rating to number
def convert_rating(rating_str):
    try:
        return int(rating_str.replace('%', ''))
    except (ValueError, AttributeError):
        return 0

# Scrape detail page
def scrape_detail(detail_link):
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
            'Referer': detail_link,
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate, br',
            'Upgrade-Insecure-Requests': '1'
        }
        response, final_url = follow_redirects(detail_link, headers=headers)
        if not response:
            write_debug_log(f"Lỗi khi truy cập detail {detail_link}: Không thể theo dõi redirect")
            return None
        write_debug_log(f"Scraped detail URL: {final_url}, Status: {response.status_code}")
    except Exception as e:
        write_debug_log(f"Lỗi khi truy cập detail {detail_link}: {e}")
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    video_div = soup.find('div', id='video')
    if not video_div:
        write_debug_log(f"Không tìm thấy div#video trong {final_url}")
        return None
    detail_data = {}
    detail_data['video_id'] = str(video_div.get('data-id', 'N/A'))
    if detail_data['video_id'] == 'N/A':
        write_debug_log(f"Không tìm thấy data-id trong div#video cho {final_url}")
        return None
    stats_div = soup.find('div', class_='video-stats')
    if stats_div:
        detail_data['likes'] = convert_likes_dislikes(stats_div.find('span', class_='likes').text.strip() if stats_div.find('span', class_='likes') else '0')
        detail_data['dislikes'] = convert_likes_dislikes(stats_div.find('span', class_='dislikes').text.strip() if stats_div.find('span', class_='dislikes') else '0')
        detail_data['rating'] = convert_rating(stats_div.find('span', class_='rating').text.strip() if stats_div.find('span', class_='rating') else '0')
        detail_data['views'] = convert_views(stats_div.find('span', class_='views').text.strip() if stats_div.find('span', class_='views') else '0')
    info_div = soup.find('div', class_='video-info')
    if info_div:
        detail_data['video_code'] = info_div.find('span', class_='video-code').text.strip() if info_div.find('span', class_='video-code') else 'N/A'
        detail_data['video_link'] = info_div.find('span', class_='video-link').text.strip() if info_div.find('span', class_='video-link') else 'N/A'
    desc_div = soup.find('div', class_='video-description')
    detail_data['description'] = desc_div.text.strip()[:1000] + '...' if desc_div and len(desc_div.text.strip()) > 1000 else (desc_div.text.strip() if desc_div else 'N/A')
    actress_div = soup.find('div', class_='actress-tag')
    detail_data['actress'] = actress_div.find('a').get('title', 'N/A') if actress_div and actress_div.find('a') else 'N/A'
    tags_div = soup.find('div', class_='tags')
    if tags_div:
        tags = [tag.text.strip() for tag in tags_div.find_all(['a', 'span'])]
        detail_data['tags'] = tags if tags else ['N/A']
    else:
        detail_data['tags'] = ['N/A']
    duration = soup.find('span', class_='duration')
    detail_data['duration'] = duration.text.strip() if duration else 'N/A'
    release_date = soup.find('span', class_='release-date')
    detail_data['release_date'] = release_date.text.strip() if release_date else 'N/A'
    category = soup.find('span', class_='category')
    detail_data['category'] = category.text.strip() if category else 'N/A'
    quality = soup.find('span', class_='quality')
    detail_data['quality'] = quality.text.strip() if quality else 'N/A'
    language = soup.find('span', class_='language')
    detail_data['language'] = language.text.strip() if language else 'N/A'
    comment_count = soup.find('span', class_='comment-count')
    detail_data['comment_count'] = convert_likes_dislikes(comment_count.text.strip()) if comment_count else 0
    meta_tags = soup.find_all('meta')
    detail_data['meta_description'] = next((meta.get('content', 'N/A') for meta in meta_tags if meta.get('name') == 'description'), 'N/A')
    detail_data['meta_keywords'] = next((meta.get('content', 'N/A') for meta in meta_tags if meta.get('name') == 'keywords'), 'N/A')
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
                print(f"Scraped detail: {detail_link}")
                write_debug_log(f"Scraped detail: {detail_link}, Data: {json.dumps(detail_data, ensure_ascii=False)}")
                with sheets_lock:
                    for video in all_video_data:
                        if video['id'] == detail_data['video_id'] and video['link'] == detail_link:
                            video.update(detail_data)
                            break
            detail_queue.task_done()
            time.sleep(config.get('DETAIL_DELAY', 1))
        except queue.Empty:
            if queueing_complete:
                break
        except Exception as e:
            write_debug_log(f"Lỗi trong detail_worker: {e}")
            detail_queue.task_done()

# Save data.txt as JSON
def save_data_txt(config):
    try:
        df = pd.DataFrame(all_video_data)
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        df = df.drop_duplicates(subset=['id', 'link'], keep='last')
        df = df.sort_values(by=['page', 'id'], ascending=[True, False])
        sorted_data = df.to_dict('records')
        with open(config.get('DATA_TXT', 'data.txt'), 'w', encoding='utf-8') as f:
            json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Lỗi khi lưu data.txt: {e}")
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
            for col in ['categories', 'tags']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
            values = [df.columns.values.tolist()] + df.values.tolist()
            df.to_csv(config.get('TEMP_CSV', 'temp.csv'), index=False, encoding='utf-8')
            with sheets_lock:
                sheet.clear()
                sheet.update(values=values, range_name='A1')
    except Exception as e:
        print(f"Lỗi khi cập nhật Google Sheets: {e}")
        write_debug_log(f"Lỗi khi cập nhật Google Sheets: {e}")

# Main function
def main():
    global stop_scraping, queueing_complete
    start_total = time.time()
    steps = []
    if os.path.exists(DEBUG_LOG):
        os.remove(DEBUG_LOG)
    config = load_config()
    if not config:
        print("Không thể đọc config.json, thoát!")
        write_debug_log("Không thể đọc config.json, thoát!")
        return
    all_video_data.extend(load_existing_data(config))
    print(f"Đã load {len(all_video_data)} video từ data.txt")
    steps.append(f"Đã đọc file data.txt, load {len(all_video_data)} video.")
    run_mode = config.get('RUN_MODE', 'real')
    if run_mode == 'test':
        max_pages = 1
        steps.append("Chạy ở chế độ TEST: chỉ quét trang 1.")
    else:
        has_new = has_new_videos_page1(config)
        if has_new:
            print("Có video mới trên trang 1.")
            steps.append("Quét trang 1, có video mới.")
            max_pages = config.get('MAX_PAGES', 100)
            steps.append(f"Quét pagination từ trang 1 đến {max_pages}.")
        else:
            print("Không có video mới trên trang 1.")
            steps.append("Quét trang 1, không có video mới.")
            max_pages = 3
            steps.append("Quét pagination trang 1 đến 3.")
    for page_num in range(1, max_pages + 1):
        page_queue.put(page_num)
    threads = []
    for _ in range(config.get('NUM_THREADS', 5)):
        t = threading.Thread(target=worker, args=(config,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    pending_links = [video['link'] for video in all_video_data if video.get('link', 'N/A') != 'N/A']
    pending_links = list(set(pending_links))
    steps.append(f"Quét details cho {len(pending_links)} video.")
    print(f"Total detail links to scrape: {len(pending_links)}")
    write_debug_log(f"Total detail links to scrape: {len(pending_links)}")
    for link in pending_links:
        detail_queue.put(link)
    queueing_complete = True
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
    total_pages = len(set(video['page'] for video in all_video_data if video['page'] != 'N/A'))
    total_videos = len(all_video_data)
    total_detailed = len([video for video in all_video_data if 'views' in video])
    elapsed_total = time.time() - start_total
    print("Tổng quát các bước thực hiện:")
    for step in steps:
        print("- " + step)
    print(f"Tổng kết: {total_pages} trang, {total_videos} video, {total_detailed} video chi tiết, {elapsed_total:.2f}s")
    write_debug_log(f"Tổng kết: {total_pages} trang, {total_videos} video, {total_detailed} video chi tiết, {elapsed_total:.2f}s")
    if all_video_data:
        save_data_txt(config)
        update_google_sheets(config)

if __name__ == "__main__":
    main()
