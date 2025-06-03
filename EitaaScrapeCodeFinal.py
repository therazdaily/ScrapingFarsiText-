import os
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from requests.exceptions import RequestException

# Customize your channels here
channels = [
    "police_mazandaran"

   # "police_khj",
    # "@policehamedan",
   # "police_mazandaran",

]

# Scraper settings
MAX_PAGES = 20000
AUTOSAVE_EVERY = 10
OUTPUT_FOLDER = os.path.expanduser("~/Downloads/Eitaa_Scraped_Data/Round 2")
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Retry-safe request
def safe_get(url, headers, max_retries=5):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=10)
            r.raise_for_status()
            return r
        except RequestException as e:
            print(f" Attempt {attempt+1}/{max_retries} failed: {e}")
            time.sleep(2 * (attempt + 1))
    print("Max retries exceeded.")
    exit()

# Main scraping loop for each channel
for channel_slug in channels:
    base_url = f"https://eitaa.com/{channel_slug}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    }
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(OUTPUT_FOLDER, f"{channel_slug}_messages_{timestamp}.csv")

    print(f"\n📡 Starting scrape for channel: {channel_slug}")
    all_data = []
    seen_ids = set()
    before_id = None

    for page in range(1, MAX_PAGES - 1):
        url = base_url + (f"?before={before_id}" if before_id else "")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 Fetching: {url}")

        response = safe_get(url, headers)
        soup = BeautifulSoup(response.text, "html.parser")
        message_blocks = soup.find_all("div", class_= "etme_widget_message_wrap js-widget_message_wrap")

        if not message_blocks:
            print(f"No more messages in {channel_slug}. Ending.")
            break

        new_this_round = 0
        for msg in message_blocks:
            msg_id = msg.get("id")
            if not msg_id or msg_id in seen_ids:
                continue

            seen_ids.add(msg_id)
            before_id = msg_id

            text_block = msg.find("div", class_="etme_widget_message_text")
            message_text = text_block.get_text(strip=True) if text_block else "No Text"

            date_block = msg.find("a", class_="etme_widget_message_date")
            time_tag = date_block.find("time") if date_block else None
            post_date = time_tag["datetime"] if time_tag and time_tag.has_attr("datetime") else "Unknown"

            all_data.append({
                "Scrape Index": len(all_data) + 1,
                "Post ID": msg_id,
                "PostD ate": post_date,
                "Date Scraped": datetime.now().isoformat(),
                "Message": message_text,
                "Link": f"{base_url}/{msg_id}",
            })
            new_this_round += 1

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Added {new_this_round} messages | Total: {len(all_data)}")
        time.sleep(1.5)

        # Autosave
        if page % AUTOSAVE_EVERY == 0:
            df = pd.DataFrame(all_data)
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Autosaved {len(df)} posts")

    # Final save per channel
    df = pd.DataFrame(all_data)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Finished {channel_slug} — saved {len(df)} posts to {output_file}")
