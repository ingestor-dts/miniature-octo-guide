import json
import os
import re
import glob
import requests
import signal
import sys
from datetime import datetime

# --- WINDOWS KEYBOARD INPUT ---
try:
    import msvcrt
    WINDOWS_OS = True
except ImportError:
    WINDOWS_OS = False

# --- CONFIG ---
DOWNLOAD_MEDIA = True 
ITEM_LIMIT = 50 

shutdown_flag = False

def signal_handler(sig, frame):
    global shutdown_flag
    print("\n\n🛑 [CTRL+C DETECTED] Please wait, safely saving current data and stopping...")
    shutdown_flag = True

signal.signal(signal.SIGINT, signal_handler)

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

def download(url, path):
    if not url or os.path.exists(path) or not DOWNLOAD_MEDIA: return False
    if not isinstance(url, str) or not url.startswith('http'): return False
    try:
        resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=15)
        if resp.status_code == 200:
            with open(path, 'wb') as f:
                f.write(resp.content)
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

# --- SMART MEDIA EXTRACTORS ---
def extract_video_from_node(node):
    if node.get('video_url'): return node['video_url']
    if node.get('video_versions') and isinstance(node['video_versions'], list) and len(node['video_versions']) > 0:
        return node['video_versions'][0].get('url')
    return None

def extract_image_from_node(node):
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
    
    # 1. CAROUSEL
    if 'edge_sidecar_to_children' in item:
        edges = item['edge_sidecar_to_children'].get('edges', [])
        for edge in edges:
            node = edge.get('node', {})
            is_vid = node.get('is_video') or node.get('video_versions') or node.get('media_type') == 2
            if is_vid:
                vid_url = extract_video_from_node(node)
                if vid_url: media_items.append({"url": vid_url, "type": "mp4", "role": "item"})
                
                img_url = extract_image_from_node(node)
                if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "item"})
            else:
                img_url = extract_image_from_node(node)
                if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "item"})
                
    elif 'carousel_media' in item:
        for child in item['carousel_media']:
            is_vid = child.get('is_video') or child.get('video_versions') or child.get('media_type') == 2
            if is_vid:
                vid_url = extract_video_from_node(child)
                if vid_url: media_items.append({"url": vid_url, "type": "mp4", "role": "item"})
                
                img_url = extract_image_from_node(child)
                if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "item"})
            else:
                img_url = extract_image_from_node(child)
                if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "item"})

    # 2. SINGLE MEDIA
    else:
        is_vid = item.get('is_video') or item.get('product_type') == 'clips' or item.get('video_versions') or item.get('media_type') == 2
        
        if is_vid:
            vid_url = extract_video_from_node(item)
            if vid_url: media_items.append({"url": vid_url, "type": "mp4", "role": "reels"})
            
            img_url = extract_image_from_node(item)
            if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "thumbs"})
        else:
            img_url = extract_image_from_node(item)
            if img_url: media_items.append({"url": img_url, "type": "jpg", "role": "item"})

    # Remove duplicates but keep sequence
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
            
            for k, v in obj.items():
                traverse(v, current_post_id)
        elif isinstance(obj, list):
            for v in obj:
                traverse(v, current_post_id)
                
    traverse(data)
    return comments_map

def find_all_raw_posts(data):
    found_posts = []
    def traverse(obj, in_carousel=False):
        if isinstance(obj, dict):
            if in_carousel:
                for k, v in obj.items():
                    traverse(v, in_carousel=True)
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
            for item in obj:
                traverse(item, in_carousel=in_carousel)
                
    traverse(data, in_carousel=False)
    return found_posts

def process_raw_file(filepath):
    global shutdown_flag
    if shutdown_flag: return
    if "EXTRACTED_" in filepath or "CleanData" in filepath: return

    print(f"\n🔥 PROCESSING RAW DATA: {os.path.basename(filepath)}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f: data = json.load(f)
    except: return

    out_dir = "EXTRACTED_" + sanitize(os.path.basename(filepath).replace('.json', ''), 50)
    os.makedirs(out_dir, exist_ok=True)
    
    global_comments_map = extract_all_comments_globally(data)

    acc = data.get('accountInfo') or data.get('user') if isinstance(data, dict) else None
    if isinstance(acc, dict) and ('username' in acc or 'Username' in acc):
        u = acc.get('username') or acc.get('Username')
        print(f"   👤 Account: @{u}")
        with open(os.path.join(out_dir, "PROFILE_INFO.json"), 'w', encoding='utf-8') as f: 
            json.dump(acc, f, indent=4, ensure_ascii=False)
            
        update_avatar_cache(acc)
        p_url = get_best_avatar(u)
        if p_url:
            download(p_url, os.path.join(out_dir, f"@{u}_avatar.jpg"))

    raw_posts = find_all_raw_posts(data)
    unique_posts = {}
    
    for post_node in raw_posts:
        numeric_id = str(post_node.get('id') or post_node.get('pk') or "")
        if '_' in numeric_id and not numeric_id.startswith('item_'): numeric_id = numeric_id.split('_')[0]
        if not numeric_id:
            numeric_id = str(post_node.get('shortcode') or post_node.get('code') or "")
        if not numeric_id: continue
        
        cap_new = get_caption_raw(post_node)
        score_new = len(json.dumps(post_node)) + (50000 if cap_new else 0)
        
        if numeric_id not in unique_posts:
            unique_posts[numeric_id] = post_node
        else:
            existing_node = unique_posts[numeric_id]
            cap_old = get_caption_raw(existing_node)
            score_old = len(json.dumps(existing_node)) + (50000 if cap_old else 0)
            if score_new > score_old:
                unique_posts[numeric_id] = post_node

    posts_to_process = list(unique_posts.values())
    
    if not posts_to_process:
        print("   ⚠️ No posts found in this file.")
        return

    print(f"   📦 Found {len(posts_to_process)} UNIQUE posts (Processing max {ITEM_LIMIT})")
    print("   💡 TIP: Press 'S' to Skip current Post. Press 'F' to Skip current File.")

    count = 0
    for post_node in posts_to_process:
        if shutdown_flag:
            print("   ⛔ Stopped processing further posts due to Ctrl+C.")
            break
            
        # --- SKIP LOGIC BEFORE POST STARTS ---
        if WINDOWS_OS and msvcrt.kbhit():
            key = msvcrt.getch().decode('utf-8', errors='ignore').lower()
            if key == 'f':
                print("\n   ⏭️ [ACTION] USER SKIPPED THIS ENTIRE FILE. Moving to next...")
                return
            elif key == 's':
                print(f"      [{count+1}] -> ⏭️ [ACTION] USER SKIPPED THIS POST.")
                count += 1
                continue

        if count >= ITEM_LIMIT: break
        
        numeric_id = str(post_node.get('id') or post_node.get('pk') or "")
        if '_' in numeric_id and not numeric_id.startswith('item_'): numeric_id = numeric_id.split('_')[0]

        shortcode = str(post_node.get('shortcode') or post_node.get('code') or numeric_id)
        if '_' in shortcode and not shortcode.startswith('item_'): shortcode = shortcode.split('_')[0]

        owner_dict = post_node.get('owner') or post_node.get('user') or {}
        user_val = owner_dict.get('username') or "unknown"
        user = sanitize(user_val, 30)
        
        if user == "unknown": continue

        post_avatar_url = get_best_avatar(user_val)
        avatar_path = os.path.join(out_dir, f"@{user}_avatar.jpg")
        if post_avatar_url and not os.path.exists(avatar_path):
            download(post_avatar_url, avatar_path)

        # --- HIGHLIGHT / STORY DETECTION LOGIC ---
        is_highlight_or_story = post_node.get('product_type') == 'story' or 'highlights_info' in post_node
        
        if is_highlight_or_story:
            extracted_title = ""
            h_info = post_node.get('highlights_info', {}).get('added_to', [])
            if h_info and isinstance(h_info, list) and len(h_info) > 0:
                extracted_title = h_info[0].get('title', '')
            
            h_title = sanitize(extracted_title, 40) if extracted_title else "Story"
            base_name = f"@{user}_Highlight_{h_title}".strip('_')
        else:
            # Regular Post
            cap_raw = get_caption_raw(post_node)
            cap = sanitize(cap_raw, 40)
            if not cap: cap = "No_Caption"
            base_name = f"@{user}_{cap}"
        # -----------------------------------------

        ts_raw = post_node.get('taken_at_timestamp') or post_node.get('taken_at')
        ts = format_timestamp(ts_raw)
        suffix = f"{ts}_{shortcode}"

        # Setup Names based on whether it's highlight or not
        if is_highlight_or_story:
            meta_name = f"{base_name}_Highlight_meta-{suffix}.json"
            comments_name = None  # Highlights don't have comments
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

        print(f"      [{count+1}] -> {meta_name} ...", end=" ", flush=True)
        
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(post_node, f, indent=4, ensure_ascii=False)

        # Save comments only if it's not a highlight
        post_comments = global_comments_map.get(numeric_id, [])
        if comments_path:
            with open(comments_path, 'w', encoding='utf-8') as f:
                json.dump(post_comments, f, indent=4, ensure_ascii=False)

        media_list = get_urls_raw_unified(post_node)
        done = False
        post_skipped_midway = False
        
        item_counter = 1
        for item in media_list[:15]: 
            if shutdown_flag: break
            
            # --- SKIP LOGIC DURING MEDIA DOWNLOAD ---
            if WINDOWS_OS and msvcrt.kbhit():
                key = msvcrt.getch().decode('utf-8', errors='ignore').lower()
                if key == 'f':
                    print("\n   ⏭️ [ACTION] USER SKIPPED THIS ENTIRE FILE. Moving to next...")
                    return
                elif key == 's':
                    print(" ⏭️ [ACTION] MEDIA SKIPPED", end="")
                    post_skipped_midway = True
                    break

            file_extension = item['type']
            role = item.get('role', 'item')
            
            if is_highlight_or_story:
                if role == 'reels' or file_extension == 'mp4':
                    item_name = f"{base_name}_Highlight_reel-{suffix}.{file_extension}"
                elif role == 'thumbs':
                    item_name = f"{base_name}_Highlight_reel_thumb-{suffix}.{file_extension}"
                else:
                    item_name = f"{base_name}_Highlight_img-{suffix}.{file_extension}"
            else:
                if role == 'reels':
                    media_label = "reels"
                elif role == 'thumbs':
                    media_label = "thumbs"
                else:
                    media_label = f"item{item_counter}"
                    item_counter += 1
                item_name = f"{base_name}_{media_label}_{suffix}.{file_extension}"
            
            if download(item['url'], os.path.join(out_dir, item_name)): 
                done = True
        
        if not post_skipped_midway:
            if is_highlight_or_story:
                print("✅ (Highlight)" if done else "Meta Only (Highlight)")
            else:
                comment_status = f"(+{len(post_comments)} Comments)" if post_comments else "(0 Comments)"
                print(f"✅ {comment_status}" if done else f"Meta Only {comment_status}")
        else:
            print(" (Moved to next post)")
            
        count += 1

if __name__ == "__main__":
    files = [f for f in glob.glob("**/*.json", recursive=True) if "EXTRACTED_" not in f and "CleanData" not in f and "claude" not in f]
    print(f"🚀 CLAUDE IG RAW MASTER STARTING ({len(files)} files)")
    print("---------------------------------------------------------")
    print("⌨️  HOTKEYS: Press 'S' to Skip a Post | Press 'F' to Skip a File")
    print("---------------------------------------------------------\n")
    
    for f in files:
        if shutdown_flag: break
        process_raw_file(os.path.abspath(f))
        
    if shutdown_flag:
        print("\n🛑 Script stopped manually. Data safely saved.")
    else:
        print("\n🏁 ALL RAW DATA PROCESSED. Check the EXTRACTED_ folders.")