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

# Locks
sheets_lock = threading.Lock()

# Queues and flags
page_queue = queue.Queue()
detail_queue = queue.Queue()
stop_scraping = False
queueing_complete = False
all_video_data = []

# Load config from config.json
def load_config():
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Lỗi khi đọc config.json: {e}")
        return {}

# Load existing data from data.txt
def load_existing_data(config):
    if os.path.exists(config.get('DATA_TXT', 'data.txt')):
        try:
            with open(config['DATA_TXT'], 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
            # Ensure IDs are strings
            for v in existing_data:
                if 'id' in v:
                    v['id'] = str(v['id'])
            return [v for v in existing_data if v.get('id') != 'N/A']
        except Exception:
            return []
    return []

# Scrape pagination page
def scrape_page(page_num, config, update_global=True):
    global stop_scraping
    if stop_scraping:
        return []
    url = f"{config['DOMAIN']}/" if page_num == 1 else f"{config['DOMAIN']}/new/{page_num}/"
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi truy cập trang {page_num}: {e}")
        return []
    soup = BeautifulSoup(response.text, 'html.parser')
    items = soup.find_all('div', class_='video-item')
    print(f"Trang {page_num}: Tìm thấy {len(items)} video-item")
    if not items:
        print(f"Không tìm thấy video-item trên trang {page_num}. Lưu HTML để debug.")
        with open(f'debug_page_{page_num}.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
        stop_scraping = True
        return []
    video_data = []
    for item in items:
        video_id = str(item.get('id', '').replace('video-', '')) if item.get('id') else 'N/A'
        if video_id == 'N/A':
            continue
        a_tag = item.find('a')
        title = a_tag.get('title', 'N/A') if a_tag else 'N/A'
        link = urljoin(config['DOMAIN'], a_tag.get('href', 'N/A')) if a_tag else 'N/A'
       
        img_tag = item.find('img', class_='video-image')
        thumbnail = img_tag.get('data-original', img_tag.get('src', 'N/A')) if img_tag else 'N/A'
        thumbnail = urljoin(config['DOMAIN'], thumbnail) if thumbnail != 'N/A' and not thumbnail.startswith('http') else thumbnail
       
        ribbon_div = item.find('div', class_='ribbon')
        ribbon = ribbon_div.text.strip() if ribbon_div else 'N/A'
       
        # Additional info from pagination
        duration = item.find('span', class_='duration')
        duration = duration.text.strip() if duration else 'N/A'
        date = item.find('span', class_='date')
        date = date.text.strip() if date else 'N/A'
        category = item.find('div', class_='category')
        category = category.text.strip() if category else 'N/A'
        views = item.find('span', class_='views')
        views = convert_views(views.text.strip()) if views else 0
        
        data = {
            'page': page_num,
            'id': video_id,
            'title': title,
            'link': link,
            'thumbnail': thumbnail,
            'ribbon': ribbon,
            'duration': duration,
            'date': date,
            'category': category,
            'views_pagination': views
        }
        video_data.append(data)
        if update_global:
            with sheets_lock:
                existing_index = next((i for i, v in enumerate(all_video_data) if v['id'] == video_id), None)
                if existing_index is not None:
                    all_video_data[existing_index].update(data)
                else:
                    all_video_data.append(data)
    return video_data

# Check if page 1 has new videos by comparing video IDs
def has_new_videos_page1(config):
    existing_ids = {str(v['id']) for v in all_video_data if v['id'] != 'N/A'}
    print(f"Existing IDs: {existing_ids}")
    temp_video_data = scrape_page(1, config, update_global=False)
    new_ids = {str(v['id']) for v in temp_video_data if v['id'] != 'N/A'}
    print(f"New IDs from page 1: {new_ids}")
    new_videos = new_ids - existing_ids
    print(f"New video IDs detected: {new_videos}")
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
        response = requests.get(detail_link, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException:
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    video_div = soup.find('div', id='video')
    if not video_div:
        return None
    detail_data = {}
    detail_data['video_id'] = str(video_div.get('data-id', 'N/A'))
    if detail_data['video_id'] == 'N/A':
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
    # Tags
    tags_div = soup.find('div', class_='tags')
    if tags_div:
        tags = [tag.text.strip() for tag in tags_div.find_all(['a', 'span'])]
        detail_data['tags'] = tags if tags else ['N/A']
    else:
        detail_data['tags'] = ['N/A']
    # Additional info from detail page
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
    # Meta tags in <head>
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
                print(f"Scraped: {detail_link}")
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
        except Exception:
            detail_queue.task_done()

# Save data.txt as JSON, sorted by page (asc) and id (desc)
def save_data_txt(config):
    try:
        df = pd.DataFrame(all_video_data)
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        df = df.drop_duplicates(subset=['id', 'link'], keep='last')
        df = df.sort_values(by=['page', 'id'], ascending=[True, False])
        sorted_data = df.to_dict('records')
        with open(config.get('DATA_TXT', 'data.txt'), 'w', encoding='utf-8') as f:
            json.dump(sorted_data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# Update Google Sheets
def update_google_sheets(config):
    if not os.path.exists(config.get('CREDENTIALS_FILE', '')):
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
            # Convert list columns to strings for Sheets
            if 'tags' in df.columns:
                df['tags'] = df['tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)
            values = [df.columns.values.tolist()] + df.values.tolist()
           
            df.to_csv(config.get('TEMP_CSV', 'temp.csv'), index=False, encoding='utf-8')
            with sheets_lock:
                sheet.clear()
                sheet.update(values=values, range_name='A1')
    except Exception:
        pass

# Main function
def main():
    global stop_scraping, queueing_complete
    start_total = time.time()
    steps = []
    # Load config
    config = load_config()
    if not config:
        print("Không thể đọc config.json, thoát!")
        return
    # Step 1: Load existing data from data.txt
    all_video_data.extend(load_existing_data(config))
    print(f"Đã load {len(all_video_data)} video từ data.txt")
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
            print("Có video mới trên trang 1.")
            steps.append("Quét trang 1, có video mới.")
            max_pages = config.get('MAX_PAGES', 100)
            steps.append(f"Quét pagination từ trang 1 đến {max_pages}.")
        else:
            print("Không có video mới trên trang 1.")
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
    print(f"Total detail links to scrape: {len(pending_links)}")
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
    total_detailed = len([video for video in all_video_data if 'views' in video])
    elapsed_total = time.time() - start_total
    print("Tổng quát các bước thực hiện:")
    for step in steps:
        print("- " + step)
    print(f"Tổng kết: {total_pages} trang, {total_videos} video, {total_detailed} video chi tiết, {elapsed_total:.2f}s")
    if all_video_data:
        save_data_txt(config)
        update_google_sheets(config)

if __name__ == "__main__":
    main()
