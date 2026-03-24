import json
import os
import re
import glob
import requests
import signal
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- WINDOWS KEYBOARD INPUT (Bypassed for Codespaces safely) ---
try:
    import msvcrt
    WINDOWS_OS = True
except ImportError:
    WINDOWS_OS = False

# --- CONFIG ---
DOWNLOAD_MEDIA = True 
ITEM_LIMIT = 5000 
MAX_WORKERS = 20
INPUT_FOLDER = "./datasets"
OUTPUT_FOLDER = "./ig_downloads"

os.makedirs(OUTPUT_FOLDER, exist_ok=True)
shutdown_flag = False

def signal_handler(sig, frame):
    global shutdown_flag
    print("\n\n🛑 [CTRL+C DETECTED] Please wait, safely saving current data and stopping...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)

# --- SHARED UTILS ---
def sanitize(text, max_length=40):
    if not text: return ""
    text = str(text).replace('\n', ' ').replace('\r', '')
    text = re.sub(r'[\\/*?:"<>|.,\[\]\(\)\'!@#$%\^&\-+=`~؛،؟\'""]', '', text)
    text = text.strip().replace(' ', '_')
    text = re.sub(r'_+', '_', text)
    return text[:max_length].strip('_')

def format_timestamp(ts):
    if not ts: return "0000-00-00_00-00-00"
    try:
        ts_int = int(str(ts)[:10])
        dt = datetime.fromtimestamp(ts_int)
        return dt.strftime('%Y-%m-%d_%H-%M-%S')
    except ValueError:
        ts_str = str(ts).replace('T', '_').replace('Z', '').replace(':', '-')
        return ts_str[:19]

def format_time_stoe(ts_string):
    try:
        dt = datetime.strptime(ts_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.strftime("%Y-%m-%d_%H-%M-%S")
    except ValueError:
        return "unknown-time"

# --- FAST PARALLEL DOWNLOADER ---
def download_task(item_data):
    url, path = item_data
    if not url or os.path.exists(path) or not DOWNLOAD_MEDIA: return False
    if not isinstance(url, str) or not url.startswith('http'): return False
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}
    try:
        with requests.Session() as s:
            resp = s.get(url, headers=headers, stream=True, timeout=15)
            if resp.status_code == 200:
                with open(path, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        if chunk: f.write(chunk)
                return True
    except: pass
    return False

# --- GLOBAL AVATAR CACHE ---
global_avatar_cache = {}

def update_avatar_cache(user_dict):
    if not isinstance(user_dict, dict): return
    username = user_dict.get('username')
    if not username: return
    
    current_best = global_avatar_cache.get(username, {'url': None, 'width': 0})
    
    hd_info = user_dict.get('hd_profile_pic_url_info')
    if isinstance(hd_info, dict) and hd_info.get('url'):
        w = hd_info.get('width', 1080)
        if w > current_best['width']:
            global_avatar_cache[username] = {'url': hd_info['url'], 'width': w}
            
    hd_versions = user_dict.get('hd_profile_pic_versions')
    if isinstance(hd_versions, list) and len(hd_versions) > 0:
        best_pic = sorted(hd_versions, key=lambda x: x.get('width', 0), reverse=True)[0]
        w = best_pic.get('width', 0)
        if best_pic.get('url') and w > current_best['width']:
            global_avatar_cache[username] = {'url': best_pic['url'], 'width': w}
            
    normal_url = user_dict.get('profile_pic_url') or user_dict.get('profilePicUrl')
    if normal_url and current_best['width'] == 0:
        global_avatar_cache[username] = {'url': normal_url, 'width': 150}

def get_best_avatar(username):
    return global_avatar_cache.get(username, {}).get('url')

# ==============================================================================
# 1. RAWHI LOGIC (100% Original Code Retained)
# ==============================================================================
def extract_video_from_node_raw(node):
    if node.get('video_url'): return node['video_url']
    if node.get('video_versions') and isinstance(node['video_versions'], list) and len(node['video_versions']) > 0:
        return node['video_versions'][0].get('url')
    return None

def extract_image_from_node_raw(node):
    if node.get('image_versions2') and isinstance(node['image_versions2'], dict):
        candidates = node['image_versions2'].get('candidates', [])
        if candidates and isinstance(candidates, list) and len(candidates) > 0:
            return candidates[0].get('url')
    if node.get('display_url'): return node['display_url']
    if node.get('thumbnail_src'): return node['thumbnail_src']
    if node.get('display_resources') and isinstance(node['display_resources'], list) and len(node['display_resources']) > 0:
        return node['display_resources'][-1].get('src') 
    return None

def get_urls_raw_unified(item):
    media_items = [] 
    if 'edge_sidecar_to_children' in item:
        edges = item['edge_sidecar_to_children'].get('edges', [])
        for edge in edges:
            node = edge.get('node', {})
            is_vid = node.get('is_video') or node.get('video_versions') or node.get('media_type') == 2
            if is_vid:
                vid_url = extract_video_from_node_raw(node)
                if vid_url: media_items.append({"url": vid_url, "type": "mp4", "role": "item"})
                img_url = extract_image_from_node_raw(node)
                if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "item"})
            else:
                img_url = extract_image_from_node_raw(node)
                if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "item"})
                
    elif 'carousel_media' in item:
        for child in item['carousel_media']:
            is_vid = child.get('is_video') or child.get('video_versions') or child.get('media_type') == 2
            if is_vid:
                vid_url = extract_video_from_node_raw(child)
                if vid_url: media_items.append({"url": vid_url, "type": "mp4", "role": "item"})
                img_url = extract_image_from_node_raw(child)
                if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "item"})
            else:
                img_url = extract_image_from_node_raw(child)
                if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "item"})

    else:
        is_vid = item.get('is_video') or item.get('product_type') == 'clips' or item.get('video_versions') or item.get('media_type') == 2
        if is_vid:
            vid_url = extract_video_from_node_raw(item)
            if vid_url: media_items.append({"url": vid_url, "type": "mp4", "role": "reels"})
            img_url = extract_image_from_node_raw(item)
            if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "thumbs"})
        else:
            img_url = extract_image_from_node_raw(item)
            if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "item"})

    final_media = []
    seen_urls = set()
    for m in media_items:
        if m['url'] and m['url'] not in seen_urls:
            seen_urls.add(m['url'])
            final_media.append(m)
    return final_media

def get_caption_raw(item):
    try:
        edges = item.get('edge_media_to_caption', {}).get('edges', [])
        if edges and len(edges) > 0:
            return edges[0].get('node', {}).get('text', '')
    except: pass
    if 'text' in item and isinstance(item['text'], str): return item['text']
    if 'caption' in item and isinstance(item['caption'], dict) and 'text' in item['caption']: return item['caption']['text']
    return ""

def extract_all_comments_globally(data):
    comments_map = {}
    seen_hashes = set()
    
    def add_comment(post_id, c):
        if not post_id or not isinstance(c, dict): return
        c_id = str(c.get('id') or c.get('pk') or "")
        hash_key = c_id if c_id else str(c.get('text', ''))
        if hash_key and hash_key not in seen_hashes:
            seen_hashes.add(hash_key)
            if post_id not in comments_map: comments_map[post_id] = []
            comments_map[post_id].append(c)
            
    def traverse(obj, current_post_id=None):
        if isinstance(obj, dict):
            if 'username' in obj and ('profile_pic_url' in obj or 'hd_profile_pic_url_info' in obj):
                update_avatar_cache(obj)
            if 'owner' in obj: update_avatar_cache(obj['owner'])
            if 'user' in obj: update_avatar_cache(obj['user'])

            pid = str(obj.get('id') or obj.get('pk') or "")
            if '_' in pid and not pid.startswith('item_'): pid = pid.split('_')[0]
            
            is_post_node = any(k in obj for k in ['display_url', 'video_url', 'image_versions2']) and not ('text' in obj and 'created_at' in obj)
            if pid and is_post_node:
                current_post_id = pid
            
            if 'text' in obj and 'created_at' in obj and ('owner' in obj or 'user' in obj):
                c_post_id = str(obj.get('media_id') or current_post_id or "")
                if '_' in c_post_id and not c_post_id.startswith('item_'): c_post_id = c_post_id.split('_')[0]
                add_comment(c_post_id, obj)
            
            for k, v in obj.items(): traverse(v, current_post_id)
        elif isinstance(obj, list):
            for v in obj: traverse(v, current_post_id)
                
    traverse(data)
    return comments_map

def find_all_raw_posts(data):
    found_posts = []
    def traverse(obj, in_carousel=False):
        if isinstance(obj, dict):
            if in_carousel:
                for k, v in obj.items(): traverse(v, in_carousel=True)
                return
            pid = str(obj.get('id') or obj.get('pk') or obj.get('shortcode') or "")
            is_comment = 'text' in obj and 'created_at' in obj
            has_media = any(k in obj for k in ['display_url', 'video_url', 'image_versions2', 'carousel_media', 'edge_sidecar_to_children', 'taken_at_timestamp'])
            owner = obj.get('owner') or obj.get('user') or {}
            has_owner = isinstance(owner, dict) and ('username' in owner or 'id' in owner)
            
            if pid and not is_comment and has_media and has_owner:
                found_posts.append(obj)
                has_carousel = 'edge_sidecar_to_children' in obj or 'carousel_media' in obj
                for k, v in obj.items():
                    if k not in ['edge_media_to_comment', 'edge_media_preview_comment']:
                        traverse(v, in_carousel=has_carousel)
            else:
                for k, v in obj.items():
                    if k not in ['edge_media_to_comment', 'edge_media_preview_comment']:
                        traverse(v, in_carousel=False)
        elif isinstance(obj, list):
            for item in obj: traverse(item, in_carousel=in_carousel)
                
    traverse(data, in_carousel=False)
    return found_posts

def process_raw_file(filepath):
    global shutdown_flag
    if shutdown_flag: return
    print(f"\n🔥 PROCESSING RAW DATA: {os.path.basename(filepath)}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f: data = json.load(f)
    except: return

    out_dir = os.path.join(OUTPUT_FOLDER, "EXTRACTED_" + sanitize(os.path.basename(filepath).replace('.json', ''), 50))
    os.makedirs(out_dir, exist_ok=True)
    
    global_comments_map = extract_all_comments_globally(data)
    master_queue = []

    acc = data.get('accountInfo') or data.get('user') if isinstance(data, dict) else None
    if isinstance(acc, dict) and ('username' in acc or 'Username' in acc):
        u = acc.get('username') or acc.get('Username')
        print(f"   👤 Account: @{u}")
        with open(os.path.join(out_dir, "PROFILE_INFO.json"), 'w', encoding='utf-8') as f: 
            json.dump(acc, f, indent=4, ensure_ascii=False)
            
        update_avatar_cache(acc)
        p_url = get_best_avatar(u)
        if p_url: master_queue.append((p_url, os.path.join(out_dir, f"@{u}_avatar.jpg")))

    raw_posts = find_all_raw_posts(data)
    unique_posts = {}
    for post_node in raw_posts:
        numeric_id = str(post_node.get('id') or post_node.get('pk') or "")
        if '_' in numeric_id and not numeric_id.startswith('item_'): numeric_id = numeric_id.split('_')[0]
        if not numeric_id: numeric_id = str(post_node.get('shortcode') or post_node.get('code') or "")
        if not numeric_id: continue
        
        cap_new = get_caption_raw(post_node)
        score_new = len(json.dumps(post_node)) + (50000 if cap_new else 0)
        
        if numeric_id not in unique_posts:
            unique_posts[numeric_id] = post_node
        else:
            existing_node = unique_posts[numeric_id]
            cap_old = get_caption_raw(existing_node)
            score_old = len(json.dumps(existing_node)) + (50000 if cap_old else 0)
            if score_new > score_old: unique_posts[numeric_id] = post_node

    posts_to_process = list(unique_posts.values())
    if not posts_to_process:
        print("   ⚠️ No posts found in this file.")
        return

    print(f"   📦 Found {len(posts_to_process)} UNIQUE posts")
    count = 0
    for post_node in posts_to_process:
        if shutdown_flag: break
        if count >= ITEM_LIMIT: break
        
        numeric_id = str(post_node.get('id') or post_node.get('pk') or "")
        if '_' in numeric_id and not numeric_id.startswith('item_'): numeric_id = numeric_id.split('_')[0]

        shortcode = str(post_node.get('shortcode') or post_node.get('code') or numeric_id)
        if '_' in shortcode and not shortcode.startswith('item_'): shortcode = shortcode.split('_')[0]

        owner_dict = post_node.get('owner') or post_node.get('user') or {}
        user_val = owner_dict.get('username') or "unknown"
        user = sanitize(user_val, 30)
        if user == "unknown": continue

        is_highlight_or_story = post_node.get('product_type') == 'story' or 'highlights_info' in post_node
        if is_highlight_or_story:
            extracted_title = ""
            h_info = post_node.get('highlights_info', {}).get('added_to', [])
            if h_info and isinstance(h_info, list) and len(h_info) > 0:
                extracted_title = h_info[0].get('title', '')
            h_title = sanitize(extracted_title, 40) if extracted_title else "Story"
            base_name = f"@{user}_Highlight_{h_title}".strip('_')
        else:
            cap_raw = get_caption_raw(post_node)
            cap = sanitize(cap_raw, 40)
            if not cap: cap = "No_Caption"
            base_name = f"@{user}_{cap}"

        ts_raw = post_node.get('taken_at_timestamp') or post_node.get('taken_at')
        ts = format_timestamp(ts_raw)
        suffix = f"{ts}_{shortcode}"

        if is_highlight_or_story:
            meta_name = f"{base_name}_Highlight_meta-{suffix}.json"
            comments_name = None 
        else:
            meta_name = f"{base_name}_meta_{suffix}.json"
            comments_name = f"{base_name}_comments_{suffix}.json"

        meta_path = os.path.join(out_dir, meta_name)
        if comments_name:
            comments_path = os.path.join(out_dir, comments_name)
            skip_condition = os.path.exists(meta_path) and os.path.exists(comments_path)
        else:
            comments_path = None
            skip_condition = os.path.exists(meta_path)

        if skip_condition:
            print(f"      [{count+1}] -> {meta_name} ... ⏭️ SKIPPED (Already Extracted)")
            count += 1
            continue

        print(f"      [{count+1}] -> {meta_name} ... JSON Saved.")
        
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(post_node, f, indent=4, ensure_ascii=False)

        post_comments = global_comments_map.get(numeric_id, [])
        if comments_path:
            with open(comments_path, 'w', encoding='utf-8') as f:
                json.dump(post_comments, f, indent=4, ensure_ascii=False)

        media_list = get_urls_raw_unified(post_node)
        item_counter = 1
        for item in media_list[:15]: 
            file_extension = item['type']
            role = item.get('role', 'item')
            if is_highlight_or_story:
                if role == 'reels' or file_extension == 'mp4': item_name = f"{base_name}_Highlight_reel-{suffix}.{file_extension}"
                elif role == 'thumbs': item_name = f"{base_name}_Highlight_reel_thumb-{suffix}.{file_extension}"
                else: item_name = f"{base_name}_Highlight_img-{suffix}.{file_extension}"
            else:
                if role == 'reels': media_label = "reels"
                elif role == 'thumbs': media_label = "thumbs"
                else:
                    media_label = f"item{item_counter}"
                    item_counter += 1
                item_name = f"{base_name}_{media_label}_{suffix}.{file_extension}"
            
            master_queue.append((item['url'], os.path.join(out_dir, item_name)))
        count += 1

    if master_queue:
        print(f"   🚀 Starting Parallel Download for {len(master_queue)} items...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(download_task, task) for task in master_queue]
            for _ in as_completed(futures):
                if shutdown_flag: executor.shutdown(wait=False); break


# ==============================================================================
# 2. CLEAN EXTRACTOR LOGIC (100% Original Code Retained)
# ==============================================================================
def get_urls_clean_unified(item):
    media_items = []
    item_type = item.get('type', '')
    if item_type == 'Sidecar' or item.get('carouselSlides'):
        slides = item.get('carouselSlides') or []
        if not slides and item.get('images'):
            for i, img in enumerate(item.get('images')): media_items.append({"url": img, "type": "jpg", "label": f"slide{i+1}"})
        else:
            for i, slide in enumerate(slides):
                s_type = slide.get('type', '')
                slide_num = i + 1
                if s_type == 'Video' or slide.get('videoUrl'):
                    if slide.get('videoUrl'): media_items.append({"url": slide.get('videoUrl'), "type": "mp4", "label": f"slide{slide_num}"})
                    thumb = slide.get('displayUrl') or slide.get('thumbnail_src') or slide.get('thumbnailUrl')
                    if thumb: media_items.append({"url": thumb, "type": "jpg", "label": f"slide{slide_num}_thumb"})
                else:
                    img_url = slide.get('displayUrl') or slide.get('url') or slide.get('thumbnail_src')
                    if img_url: media_items.append({"url": img_url, "type": "jpg", "label": f"slide{slide_num}"})
    elif item_type == 'Video' or item.get('videoUrl') or item.get('is_video'):
        vid_url = item.get('videoUrl') or item.get('video_url')
        if vid_url: media_items.append({"url": vid_url, "type": "mp4", "label": "reels"})
        thumb_url = item.get('displayUrl') or item.get('thumbnail_src') or item.get('display_url') or item.get('thumbnailUrl')
        if thumb_url: media_items.append({"url": thumb_url, "type": "jpg", "label": "thumbs"})
    else:
        img_url = item.get('displayUrl') or item.get('display_url') or item.get('thumbnail_src')
        if not img_url and item.get('images') and len(item.get('images')) > 0: img_url = item['images'][0]
        if img_url: media_items.append({"url": img_url, "type": "jpg", "label": "slide1"})

    final_media = []
    seen_urls = set()
    for m in media_items:
        if m['url'] and m['url'] not in seen_urls:
            seen_urls.add(m['url'])
            final_media.append(m)
    return final_media

def get_comments_clean(item):
    comments = []
    seen_ids = set()
    def add_comment(c):
        if not isinstance(c, dict): return
        c_id = c.get('id') or c.get('pk')
        hash_key = str(c_id) if c_id else str(c.get('text', ''))
        if hash_key and hash_key not in seen_ids:
            seen_ids.add(hash_key)
            comments.append(c)
    for k in ['comments', 'latestComments', 'extractedComments']:
        if k in item and isinstance(item[k], list):
            for c in item[k]: add_comment(c)
    return comments

def process_clean_file(filepath):
    global shutdown_flag
    if shutdown_flag: return
    print(f"\n✨ PROCESSING CLEAN DATA: {os.path.basename(filepath)}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f: data = json.load(f)
    except: return
    
    out_dir = os.path.join(OUTPUT_FOLDER, "EXTRACTED_" + sanitize(os.path.basename(filepath).replace('.json', ''), 50))
    posts = []
    master_queue = []

    if isinstance(data, dict):
        if 'scrapedPosts' in data and isinstance(data['scrapedPosts'], list): posts.extend(data['scrapedPosts'])
        if 'feedPosts' in data and isinstance(data['feedPosts'], list): posts.extend(data['feedPosts'])
        if 'posts' in data and isinstance(data['posts'], list): posts.extend(data['posts'])
        if 'highlights' in data and isinstance(data['highlights'], list):
            for h in data['highlights']:
                h_title = sanitize(h.get('title', 'Story'), 40)
                if not h_title: h_title = "Highlight"
                for item in h.get('items', []):
                    item['_is_highlight'] = True
                    item['_highlight_title'] = h_title
                    posts.append(item)
        if not posts and ('id' in data or 'shortCode' in data): posts = [data]
    elif isinstance(data, list):
        posts = data

    if not posts or len(posts) == 0:
        print(" ❌ No posts found in file.")
        return

    global_u = "unknown"
    global_avatar = None
    first_post = posts[0] if isinstance(posts[0], dict) else {}
    acc_main = first_post.get('owner') or first_post.get('user')
    
    if isinstance(acc_main, dict) and ('username' in acc_main or 'Username' in acc_main):
        global_u = acc_main.get('username') or acc_main.get('Username')
        global_avatar = acc_main.get('profilePicUrl') or acc_main.get('profile_pic_url')
    else:
        acc_info = data.get('accountInfo') or data.get('user') if isinstance(data, dict) else None
        if isinstance(acc_info, dict) and ('username' in acc_info or 'Username' in acc_info):
            global_u = acc_info.get('username') or acc_info.get('Username')
            global_avatar = acc_info.get('profilePicUrl') or acc_info.get('profile_pic_url')
            
    os.makedirs(out_dir, exist_ok=True)
    print(f"   📦 Found {len(posts)} UNIQUE posts")
    
    count = 0
    for post_node in posts:
        if shutdown_flag: return
        if count >= ITEM_LIMIT: break
        if not isinstance(post_node, dict): continue

        pid = str(post_node.get('id') or post_node.get('shortCode') or "")
        if not pid and "type" not in post_node: continue 
        
        post_owner = post_node.get('owner') or post_node.get('user') or post_node.get('author') or {}
        if isinstance(post_owner, dict) and ('username' in post_owner or 'Username' in post_owner):
            post_u = post_owner.get('username') or post_owner.get('Username')
        else: post_u = global_u
            
        clean_username = sanitize(post_u, 50)
            
        if post_owner and not post_node.get('_is_highlight'):
            profile_info_name = f"@{clean_username}_PROFILE_INFO.json"
            profile_info_path = os.path.join(out_dir, profile_info_name)
            if not os.path.exists(profile_info_path):
                with open(profile_info_path, 'w', encoding='utf-8') as f: json.dump(post_owner, f, indent=4, ensure_ascii=False)
            
        shortcode = str(post_node.get('shortCode') or post_node.get('shortcode') or pid)
        timestamp = format_timestamp(post_node.get('timestamp') or post_node.get('taken_at'))
        is_highlight = post_node.get('_is_highlight', False)
        
        if is_highlight:
            h_title_val = post_node.get('_highlight_title', 'Story')
            base_name = f"@{clean_username}_Highlight_{h_title_val}".strip('_')
        else:
            caption_text = post_node.get('caption') or post_node.get('text') or ""
            caption_clean = sanitize(caption_text, 30) if caption_text else "NoCaption"
            base_name = f"@{clean_username}_{caption_clean}"
            
        suffix = f"{timestamp}_{shortcode}"
        
        if is_highlight:
            meta_name = f"{base_name}_Highlight_meta-{suffix}.json"
            comments_name = None
        else:
            meta_name = f"{base_name}_meta_{suffix}.json"
            comments_name = f"{base_name}_comments_{suffix}.json"

        meta_path = os.path.join(out_dir, meta_name)
        if comments_name:
            comments_path = os.path.join(out_dir, comments_name)
            skip_condition = os.path.exists(meta_path) and os.path.exists(comments_path)
        else:
            comments_path = None
            skip_condition = os.path.exists(meta_path)

        if skip_condition:
            print(f"      [{count+1}] -> {meta_name} ... ⏭️ SKIPPED (Already Extracted)")
            count += 1
            continue

        print(f"      [{count+1}] -> {meta_name} ... JSON Saved.", flush=True)

        with open(meta_path, 'w', encoding='utf-8') as f:
            save_node = post_node.copy()
            save_node.pop('_is_highlight', None)
            save_node.pop('_highlight_title', None)
            json.dump(save_node, f, indent=4, ensure_ascii=False)

        if comments_path:
            post_comments = get_comments_clean(post_node)
            with open(comments_path, 'w', encoding='utf-8') as f: json.dump(post_comments, f, indent=4, ensure_ascii=False)

        media_items = get_urls_clean_unified(post_node)
        
        avatar_url = post_owner.get('profilePicUrl') or post_owner.get('profilePicUrlHd') or post_owner.get('profile_pic_url') or global_avatar
        if avatar_url and count == 0: 
            media_items.append({"url": avatar_url, "type": "jpg", "label": "avatar", "is_avatar": True})
            
        for item in media_items:
            media_label = item['label']
            file_extension = item['type']
            
            if item.get("is_avatar"): item_name = f"@{clean_username}_avatar.{file_extension}"
            else:
                if is_highlight:
                    if file_extension == 'mp4' or media_label == 'reels': item_name = f"{base_name}_Highlight_reel-{suffix}.{file_extension}"
                    elif media_label == 'thumbs' or media_label.endswith('_thumb'): item_name = f"{base_name}_Highlight_reel_thumb-{suffix}.{file_extension}"
                    else: item_name = f"{base_name}_Highlight_img-{suffix}.{file_extension}"
                else: item_name = f"{base_name}_{media_label}_{suffix}.{file_extension}"
            
            master_queue.append((item['url'], os.path.join(out_dir, item_name)))
        count += 1

    if master_queue:
        print(f"   🚀 Starting Parallel Download for {len(master_queue)} items...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(download_task, task) for task in master_queue]
            for _ in as_completed(futures):
                if shutdown_flag: executor.shutdown(wait=False); break


# ==============================================================================
# 3. STOE LOGIC (100% Original Code Retained)
# ==============================================================================
def process_stoe_file(filename):
    global shutdown_flag
    if shutdown_flag: return
    print(f"\n🌟 PROCESSING STORIES DATA: {os.path.basename(filename)}")
    
    try:
        with open(filename, 'r', encoding='utf-8') as file: data = json.load(file)
    except: return

    account_info = data.get("accountInfo", {})
    username = account_info.get("username", "unknown_user")
    all_media = []
    
    for post in data.get("feedPosts", []):
        post['source_type'] = 'Post'
        all_media.append(post)
    for hl in data.get("highlights", []):
        hl['source_type'] = 'Highlight'
        all_media.append(hl)
    for st in data.get("activeStories", []):
        st['source_type'] = 'Story'
        all_media.append(st)

    if not all_media:
        print(" ❌ No posts, highlights, or stories found in file.")
        return

    extracted_folder = os.path.join(OUTPUT_FOLDER, f"EXTRACTED_{username}")
    os.makedirs(extracted_folder, exist_ok=True)
    
    print(f" 📂 Found {len(all_media)} total items. Downloading to {extracted_folder}...")
    master_queue = []

    for item in all_media:
        item_id = item.get("id", "unknown_id")
        media_type = item.get("type", "Unknown")
        source_type = item.get("source_type", "Media")
        raw_timestamp = item.get("timestamp", "")
        formatted_time = format_time_stoe(raw_timestamp)
        
        display_url = item.get("displayUrl")
        video_url = item.get("videoUrl")

        filename_base = f"@{username}_{source_type}_{formatted_time}_{item_id}"
        
        if media_type == "Video" and video_url:
            ext = ".mp4"
            target_url = video_url
        elif display_url:
            ext = ".jpg"
            target_url = display_url
        else:
            continue

        filepath = os.path.join(extracted_folder, filename_base + ext)
        if not os.path.exists(filepath):
            master_queue.append((target_url, filepath))

    if master_queue:
        print(f"   🚀 Starting Parallel Download for {len(master_queue)} items...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(download_task, task) for task in master_queue]
            for _ in as_completed(futures):
                if shutdown_flag: executor.shutdown(wait=False); break


# ==============================================================================
# MASTER CONTROLLER / ROUTER
# ==============================================================================
if __name__ == "__main__":
    files = [f for f in glob.glob(f"{INPUT_FOLDER}/**/*.json", recursive=True) if "EXTRACTED_" not in f]
    print(f"🚀 ULTRA-FAST UNIVERSAL EXTRACTOR STARTING ({len(files)} files)")
    print("---------------------------------------------------------")
    
    for f in files:
        if shutdown_flag: break
        fname = os.path.basename(f).lower()
        
        if "stoe" in fname or "story" in fname:
            process_stoe_file(f)
        elif "clean" in fname:
            process_clean_file(f)
        else:
            process_raw_file(f)
            
    if shutdown_flag:
        print("\n🛑 Script stopped manually.")
    else:
        print("\n🏁 ALL DATA PROCESSED! Check the ./ig_downloads folder.")
