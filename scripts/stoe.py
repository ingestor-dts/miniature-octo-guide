import os
import json
import msvcrt
import time
import requests
from datetime import datetime

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

# Timestamp ko theek format mein laane ka function (e.g., 2026-03-23_10-25-34)
def format_time(ts_string):
    try:
        # Pura time parse karega
        dt = datetime.strptime(ts_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.strftime("%Y-%m-%d_%H-%M-%S")
    except ValueError:
        return "unknown-time"

# File internet se download karne ka function
def download_file(url, filepath):
    try:
        response = requests.get(url, stream=True, timeout=15)
        response.raise_for_status()
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        print(f"   ❌ Download failed: {e}")
        return False

def process_files():
    clear_screen()
    
    # Current directory mein mojood tamam JSON files dhoondein
    json_files = [f for f in os.listdir('.') if f.endswith('.json')]
    
    print(f"🚀 CLAUDE IG CLEAN MASTER STARTING ({len(json_files)} files)")
    print("-" * 57)
    print("⌨️  HOTKEYS: Press 'S' to Skip a Post | Press 'F' to Skip a File")
    print("-" * 57)

    if not json_files:
        print("\n❌ Koi JSON file nahi mili is folder mein.")
        return

    for filename in json_files:
        print(f"\n✨ PROCESSING CLEAN DATA: {filename}")
        
        with open(filename, 'r', encoding='utf-8') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                print(f" ❌ Error reading {filename}. Invalid JSON.")
                continue

        # Account info aur username nikalna
        account_info = data.get("accountInfo", {})
        username = account_info.get("username", "unknown_user")
        
        # Data arrays check karna aur unki type mark karna
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
            continue

        # Extraction folder banana
        extracted_folder = f"EXTRACTED_{username}"
        os.makedirs(extracted_folder, exist_ok=True)
        
        print(f" 📂 Found {len(all_media)} total items. Downloading to {extracted_folder}...")

        skip_file = False
        
        for item in all_media:
            # Hotkey Logic (Windows ke liye)
            if msvcrt.kbhit():
                key = msvcrt.getch().decode('utf-8').upper()
                if key == 'S':
                    print("   ⏭️ Skipped current item.")
                    continue
                elif key == 'F':
                    print("   ⏭️ Skipped entire file.")
                    skip_file = True
                    break

            item_id = item.get("id", "unknown_id")
            media_type = item.get("type", "Unknown")
            source_type = item.get("source_type", "Media")
            raw_timestamp = item.get("timestamp", "")
            
            # Timestamp format karna
            formatted_time = format_time(raw_timestamp)
            
            display_url = item.get("displayUrl")
            video_url = item.get("videoUrl")

            # Naming Convention Banan (e.g., @xaditi_Story_2026-03-23_02-14-49_3858860922378939466)
            filename_base = f"@{username}_{source_type}_{formatted_time}_{item_id}"
            
            if media_type == "Video" and video_url:
                ext = ".mp4"
                target_url = video_url
            elif display_url:
                ext = ".jpg"
                target_url = display_url
            else:
                print(f"   ⚠️ No valid media found for ID: {item_id}")
                continue

            filepath = os.path.join(extracted_folder, filename_base + ext)
            
            # Check karna ke file pehle se mojood toh nahi
            if os.path.exists(filepath):
                print(f"   ⏩ Already exists, skipping: {filename_base}{ext}")
                continue

            print(f"   ⏳ Downloading: {filename_base}{ext} ...")
            
            if download_file(target_url, filepath):
                print(f"   ✅ Saved {source_type} ({ext}) successfully.")
                
            time.sleep(0.5) # Server ko zyada requests ek sath na bhejne ke liye thora delay

        if skip_file:
            continue

    print("\n🏁 ALL CLEAN DATA PROCESSED. Check the EXTRACTED_ folders.")

if __name__ == "__main__":
    process_files()