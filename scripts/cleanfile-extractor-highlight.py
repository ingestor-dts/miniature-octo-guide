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
ITEM_LIMIT = 5000 

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

def get_urls_clean_unified(item):
    media_items = []
    item_type = item.get('type', '')
    
    # 1. CAROUSEL (Sidecar)
    if item_type == 'Sidecar' or item.get('carouselSlides'):
        slides = item.get('carouselSlides') or []
        if not slides and item.get('images'):
            for i, img in enumerate(item.get('images')):
                media_items.append({"url": img, "type": "jpg", "label": f"slide{i+1}"})
        else:
            for i, slide in enumerate(slides):
                s_type = slide.get('type', '')
                slide_num = i + 1
                if s_type == 'Video' or slide.get('videoUrl'):
                    if slide.get('videoUrl'):
                        media_items.append({"url": slide.get('videoUrl'), "type": "mp4", "label": f"slide{slide_num}"})
                    thumb = slide.get('displayUrl') or slide.get('thumbnail_src') or slide.get('thumbnailUrl')
                    if thumb:
                        media_items.append({"url": thumb, "type": "jpg", "label": f"slide{slide_num}_thumb"})
                else:
                    img_url = slide.get('displayUrl') or slide.get('url') or slide.get('thumbnail_src')
                    if img_url:
                        media_items.append({"url": img_url, "type": "jpg", "label": f"slide{slide_num}"})
                        
    # 2. SINGLE VIDEO / REELS
    elif item_type == 'Video' or item.get('videoUrl') or item.get('is_video'):
        vid_url = item.get('videoUrl') or item.get('video_url')
        if vid_url:
            media_items.append({"url": vid_url, "type": "mp4", "label": "reels"})
            
        thumb_url = item.get('displayUrl') or item.get('thumbnail_src') or item.get('display_url') or item.get('thumbnailUrl')
        if thumb_url:
            media_items.append({"url": thumb_url, "type": "jpg", "label": "thumbs"})
            
    # 3. SINGLE IMAGE
    else:
        img_url = item.get('displayUrl') or item.get('display_url') or item.get('thumbnail_src')
        if not img_url and item.get('images') and len(item.get('images')) > 0:
            img_url = item['images'][0]
        if img_url:
            media_items.append({"url": img_url, "type": "jpg", "label": "slide1"})

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
    if "EXTRACTED_" in filepath or "RawData" in filepath or "claude" in filepath: return
    
    print(f"\n✨ PROCESSING CLEAN DATA: {os.path.basename(filepath)}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except: return
    
    out_dir = "EXTRACTED_" + sanitize(os.path.basename(filepath).replace('.json', ''), 50)
    
    # ---------------------------------------------------------
    # JSON STRUCTURE FIX (Aggregating all posts regardless of empty fields)
    # ---------------------------------------------------------
    posts = []
    if isinstance(data, dict):
        if 'scrapedPosts' in data and isinstance(data['scrapedPosts'], list):
            posts.extend(data['scrapedPosts'])
            
        if 'feedPosts' in data and isinstance(data['feedPosts'], list):
            posts.extend(data['feedPosts'])
            
        if 'posts' in data and isinstance(data['posts'], list):
            posts.extend(data['posts'])
            
        if 'highlights' in data and isinstance(data['highlights'], list):
            for h in data['highlights']:
                h_title = sanitize(h.get('title', 'Story'), 40)
                if not h_title: h_title = "Highlight"
                for item in h.get('items', []):
                    item['_is_highlight'] = True
                    item['_highlight_title'] = h_title
                    posts.append(item)
                    
        # Fallback agar file direct ek dictionary ho
        if not posts and ('id' in data or 'shortCode' in data):
            posts = [data]
            
    elif isinstance(data, list):
        posts = data

    if not posts or len(posts) == 0:
        print(" ❌ No posts found in file.")
        return

    # Main account info fallback
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

    print(f"   📦 Found {len(posts)} UNIQUE posts (Processing max {ITEM_LIMIT})")
    
    count = 0
    for post_node in posts:
        global shutdown_flag
        if shutdown_flag: return
        if count >= ITEM_LIMIT: break
        if not isinstance(post_node, dict): continue

        post_skipped_midway = False
        if WINDOWS_OS and msvcrt.kbhit():
            key = msvcrt.getch().decode('utf-8', 'ignore').lower()
            if key == 's':
                print("   ⏭️  [SKIPPED] Post skipped by user.")
                continue
            elif key == 'f':
                print("   ⏭️  [SKIPPED] File skipped by user.")
                return

        pid = str(post_node.get('id') or post_node.get('shortCode') or "")
        if not pid and "type" not in post_node: continue # Invalid node skip
        
        # EXTRACT USERNAME
        post_owner = post_node.get('owner') or post_node.get('user') or post_node.get('author') or {}
        if isinstance(post_owner, dict) and ('username' in post_owner or 'Username' in post_owner):
            post_u = post_owner.get('username') or post_owner.get('Username')
        else:
            post_u = global_u
            
        clean_username = sanitize(post_u, 50)
            
        # PROFILE INFO SAVING PER USER
        if post_owner and not post_node.get('_is_highlight'):
            profile_info_name = f"@{clean_username}_PROFILE_INFO.json"
            profile_info_path = os.path.join(out_dir, profile_info_name)
            if not os.path.exists(profile_info_path):
                with open(profile_info_path, 'w', encoding='utf-8') as f:
                    json.dump(post_owner, f, indent=4, ensure_ascii=False)
            
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

        print(f"      [{count+1}] -> {meta_name} ... ", end='', flush=True)

        with open(meta_path, 'w', encoding='utf-8') as f:
            # Clean up the injected flags before saving meta
            save_node = post_node.copy()
            save_node.pop('_is_highlight', None)
            save_node.pop('_highlight_title', None)
            json.dump(save_node, f, indent=4, ensure_ascii=False)

        # COMMENTS SAVING
        post_comments = []
        if comments_path:
            post_comments = get_comments_clean(post_node)
            with open(comments_path, 'w', encoding='utf-8') as f:
                json.dump(post_comments, f, indent=4, ensure_ascii=False)

        media_items = get_urls_clean_unified(post_node)
        
        # ADD AVATAR TO DOWNLOAD QUEUE
        avatar_url = post_owner.get('profilePicUrl') or post_owner.get('profilePicUrlHd') or post_owner.get('profile_pic_url') or global_avatar
        if avatar_url and count == 0:  # Only do it once to prevent spam
            media_items.append({"url": avatar_url, "type": "jpg", "label": "avatar", "is_avatar": True})
            
        done = False
        
        for item in media_items:
            if shutdown_flag: break
            
            if WINDOWS_OS and msvcrt.kbhit():
                key = msvcrt.getch().decode('utf-8', 'ignore').lower()
                if key == 's':
                    post_skipped_midway = True
                    break
                elif key == 'f':
                    print("   ⏭️  [SKIPPED] File skipped by user.")
                    return

            media_label = item['label']
            file_extension = item['type']
            
            if item.get("is_avatar"):
                item_name = f"@{clean_username}_avatar.{file_extension}"
            else:
                if is_highlight:
                    if file_extension == 'mp4' or media_label == 'reels':
                        item_name = f"{base_name}_Highlight_reel-{suffix}.{file_extension}"
                    elif media_label == 'thumbs' or media_label.endswith('_thumb'):
                        item_name = f"{base_name}_Highlight_reel_thumb-{suffix}.{file_extension}"
                    else:
                        item_name = f"{base_name}_Highlight_img-{suffix}.{file_extension}"
                else:
                    item_name = f"{base_name}_{media_label}_{suffix}.{file_extension}"
            
            if download(item['url'], os.path.join(out_dir, item_name)): 
                done = True
        
        if not post_skipped_midway:
            if is_highlight:
                print("✅ (Highlight)" if done else "Meta Only (Highlight)")
            else:
                comment_status = f"(+{len(post_comments)} Comments)" if post_comments else "(0 Comments)"
                print(f"✅ {comment_status}" if done else f"Meta Only {comment_status}")
        else:
            print(" (Moved to next post)")
            
        count += 1

if __name__ == "__main__":
    files = [f for f in glob.glob("**/*.json", recursive=True) if "EXTRACTED_" not in f and "RawData" not in f and "claude" not in f]
    print(f"🚀 CLAUDE IG CLEAN MASTER STARTING ({len(files)} files)")
    print("---------------------------------------------------------")
    print("⌨️  HOTKEYS: Press 'S' to Skip a Post | Press 'F' to Skip a File")
    print("---------------------------------------------------------\n")
    
    for f in files:
        if shutdown_flag: break
        process_clean_file(os.path.abspath(f))
        
    if shutdown_flag:
        print("\n🛑 Script stopped manually. Data safely saved.")
    else:
        print("\n🏁 ALL CLEAN DATA PROCESSED. Check the EXTRACTED_ folders.")