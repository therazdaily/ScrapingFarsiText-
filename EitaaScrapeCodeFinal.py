import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from datetime import datetime
import re
from hashlib import sha256

# Define the Eitaa channels to scrape
channels = ["ENTER CHANNEL NAME"]

# Define threshold for number of posts to scrape
POST_THRESHOLD = 3000  # Adjust this to your desired number of posts
CHECKPOINT_INTERVAL = 500  # Save checkpoint every 100 messages

# Setup ChromeDriver
options = Options()
options.add_argument("--incognito")
options.add_argument("--headless=new")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=options)

# Create a folder for saving files
downloads_folder = os.path.expanduser("~/Downloads/Eitaa_Scraped_Data")
os.makedirs(downloads_folder, exist_ok=True)

# Define a function to save a checkpoint of the data
def save_checkpoint(data, output_file, channel_name):
    if data:
        df = pd.DataFrame(data)
        df.to_csv(output_file, mode='a', index=False, encoding="utf-8-sig", header=not os.path.exists(output_file))
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Checkpoint saved for {channel_name}: {len(df)} messages.")

# Data collection from multiple channels
for channel_name in channels:
    base_url = f"https://eitaa.com/{channel_name}"
    output_file = os.path.join(downloads_folder, f"{channel_name}_messages.csv")

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🔍 Starting Scraping: {channel_name}...")

    driver.get(base_url)
    time.sleep(10)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Page loaded successfully for {channel_name}")

    all_data = []  # Store messages for this channel
    seen_message_ids = set()  # Set to track already scraped message IDs
    message_counter = 1  # Track numbering in a more readable format

    # Scroll down first to load older messages
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(10)

    try:
        while len(all_data) < POST_THRESHOLD:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            message_blocks = soup.find_all("div", class_="etme_widget_message_wrap")

            if not message_blocks:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] No more messages found. Stopping scrape.")
                break

            print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔎 Extracting messages... Current count: {len(all_data)}")
            new_messages_found = False

            for msg_block in message_blocks:
                try:
                    message_id = msg_block.get("id")

                    # Extract message text and date
                    text_block = msg_block.find("div", class_="etme_widget_message_text")
                    message_text = text_block.get_text(strip=True) if text_block else "No Text Available"

                    date_block = msg_block.find("a", class_="etme_widget_message_date")
                    message_date = date_block.find("time")["datetime"] if date_block else "Unknown Date"

                    # If ID is missing, generate a unique hash from text + date
                    if not message_id:
                        unique_hash = sha256((message_text + message_date).encode()).hexdigest()
                        message_id = f"MSG-{message_counter:05d}"  # Prettier numbering
                        message_counter += 1
                    else:
                        message_id = f"MSG-{int(message_id):05d}"  # Ensure consistent format

                    # Skip duplicate messages
                    if message_id in seen_message_ids:
                        continue
                    seen_message_ids.add(message_id)
                    new_messages_found = True

                    # Extract views correctly
                    view_block = msg_block.find("span", class_="etme_widget_message_views")
                    views_text = view_block.get_text(strip=True) if view_block else "0"
                    if "هزار" in views_text:
                        views = int(float(re.sub(r'[^0-9.]', '', views_text)) * 1000)
                    elif views_text.isdigit():
                        views = int(re.sub(r'[^0-9]', '', views_text))
                    else:
                        views = 0

                    # Store extracted data
                    all_data.append({
                        "Channel": channel_name,
                        "Number": message_id,
                        "Date": message_date,
                        "Message": message_text,
                        "Views": views
                    })

                    # Save checkpoint periodically
                    if len(all_data) % CHECKPOINT_INTERVAL == 0:
                        save_checkpoint(all_data, output_file, channel_name)

                except Exception as e:
                    print(f"Error processing message: {e}")

            # If no new messages were found, keep scrolling up until new messages appear
            while not new_messages_found:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 🔄 No new messages. Scrolling up...")
                driver.execute_script("window.scrollBy(0, -1000);")
                time.sleep(5)
                soup = BeautifulSoup(driver.page_source, "html.parser")
                message_blocks = soup.find_all("div", class_="etme_widget_message_wrap")
                for msg_block in message_blocks:
                    message_id = msg_block.get("id")
                    if message_id and message_id not in seen_message_ids:
                        new_messages_found = True
                        break

            # Scroll **down** first, then **up** to load older messages
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(10)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(10)

    except Exception as e:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Error during scraping: {e}")
        # Save checkpoint if an error occurs
        save_checkpoint(all_data, output_file, channel_name)
    finally:
        # Final save after all messages are scraped or if an error occurred
        save_checkpoint(all_data, output_file, channel_name)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Finished scraping {channel_name}. Total messages: {len(all_data)}")

# Close ChromeDriver
driver.quit()
print(f"[{datetime.now().strftime('%H:%M:%S')}] Scraping completed for all channels!")
