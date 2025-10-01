#!/usr/bin/env python3
"""
Social Media Scraper using Bright Data API
Scrapes Facebook posts from company pages
"""

import requests
import time
import pandas as pd
import os
import ast
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import config

# === Utility Functions ===
def determine_platform(url: str) -> str:
    """Determine platform from URL"""
    if "facebook.com" in url:
        return "facebook"
    else:
        raise ValueError(f"Unsupported URL format: {url}")

def format_brightdata_dates(platform: str, date_obj) -> str:
    """Format dates for Bright Data API based on platform"""
    if not date_obj:
        return ""
    if platform == "facebook":
        return date_obj.strftime("%m-%d-%Y")
    return ""

def get_date_range(days_back: int = None) -> Tuple[datetime, datetime]:
    """Get start and end dates for scraping"""
    if days_back is None:
        days_back = config.DAYS_BACK
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    return start_date, end_date

# === Bright Data API Functions ===
def trigger_brightdata_scrape(urls: List[str], start_date_obj=None, end_date_obj=None) -> Dict:
    """
    Trigger Bright Data scraping for Facebook posts
    
    Args:
        urls: List of company page URLs
        start_date_obj: Start date for scraping
        end_date_obj: End date for scraping
    
    Returns:
        Dictionary with triggered job information
    """
    # Group URLs by platform
    grouped_urls = {"facebook": []}
    
    for url in urls:
        platform = determine_platform(url)
        start_date = format_brightdata_dates(platform, start_date_obj)
        end_date = format_brightdata_dates(platform, end_date_obj)
        
        if platform == "facebook":
            grouped_urls["facebook"].append({
                "url": url,
                "num_of_posts": config.MAX_POSTS,
                "start_date": start_date,
                "end_date": end_date
            })

    triggered_jobs = {}
    
    for platform, data in grouped_urls.items():
        if not data:
            continue
            
        params = {
            "dataset_id": config.BRIGHTDATA_DATASET_IDS[platform],
            "include_errors": "true"
        }

        headers = {
            "Authorization": f"Bearer {config.BRIGHTDATA_API_TOKEN}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(
                "https://api.brightdata.com/datasets/v3/trigger",
                headers=headers,
                params=params,
                json=data,
                timeout=30
            )
            response.raise_for_status()
            result = response.json()
            triggered_jobs[platform] = result
            print(f"🚀 Triggered {platform} scrape: {result}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Error triggering {platform} scrape: {e}")
            triggered_jobs[platform] = {"error": str(e)}
    
    return triggered_jobs

def wait_for_snapshot_ready(snapshot_id: str, max_wait_minutes: int = None) -> bool:
    """
    Wait for Bright Data snapshot to become ready
    
    Args:
        snapshot_id: ID of the snapshot to monitor
        max_wait_minutes: Maximum wait time in minutes
    
    Returns:
        True if snapshot is ready, False if timeout
    """
    if max_wait_minutes is None:
        max_wait_minutes = config.MAX_WAIT_MINUTES
        
    headers = {"Authorization": f"Bearer {config.BRIGHTDATA_API_TOKEN}"}
    progress_url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
    elapsed_seconds = 0

    print(f"⏳ Waiting for snapshot {snapshot_id} to become ready...")

    while True:
        try:
            response = requests.get(progress_url, headers=headers, timeout=30)
            response.raise_for_status()
            
            progress_data = response.json()
            snapshot_status = progress_data.get("status")

            print(f"🔎 Snapshot {snapshot_id} status: {snapshot_status} (checked at {elapsed_seconds // 60} min {elapsed_seconds % 60} sec)")

            if snapshot_status == "ready":
                print(f"✅ Snapshot {snapshot_id} is now ready for download!")
                return True

            if elapsed_seconds >= max_wait_minutes * 60:
                print(f"❌ Snapshot {snapshot_id} not ready after {max_wait_minutes} minutes.")
                return False

            time.sleep(20)
            elapsed_seconds += 20
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error checking snapshot progress: {e}")
            return False

def download_brightdata_snapshot(snapshot_id: str, max_retries: int = 5) -> List[Dict]:
    """
    Download data from Bright Data snapshot with retry mechanism
    
    Args:
        snapshot_id: ID of the snapshot to download
        max_retries: Maximum number of retry attempts
    
    Returns:
        List of scraped posts
    """
    headers = {"Authorization": f"Bearer {config.BRIGHTDATA_API_TOKEN}"}
    url = f"https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"
    params = {"format": "json"}

    print(f"📥 Downloading snapshot {snapshot_id}...")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                print(f"⚠️ Snapshot {snapshot_id} is ready but contains no data.")
                return []
            
            # Check if this is a building/error response
            if isinstance(data, dict) and 'status' in data and 'message' in data:
                status = data['status']
                message = data['message']
                
                if status == 'building':
                    if attempt < max_retries - 1:
                        wait_time = 60 * (attempt + 1)  # Increasing wait time: 60s, 120s, 180s, 240s
                        print(f"⏳ Snapshot still building: {message}. Waiting {wait_time}s before retry {attempt + 2}/{max_retries}...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"❌ Snapshot still building after {max_retries} attempts: {message}")
                        return []
                else:
                    print(f"⚠️ API returned status response: {status} - {message}")
                    return []
            
            # Process the actual data
            if isinstance(data, dict):
                # If it's a dict, try to find the actual posts data
                if 'data' in data:
                    posts_data = data['data']
                elif 'results' in data:
                    posts_data = data['results']
                elif 'items' in data:
                    posts_data = data['items']
                else:
                    # If it's a single post dict, wrap it in a list
                    posts_data = [data]
            elif isinstance(data, list):
                posts_data = data
            else:
                print(f"⚠️ Unexpected data type: {type(data)}")
                return []
            
            print(f"✅ Downloaded {len(posts_data)} posts from snapshot {snapshot_id}")
            
            # Debug: Check the type of first item
            if posts_data and len(posts_data) > 0:
                first_item = posts_data[0]
                print(f"🔍 Debug: First item type: {type(first_item)}")
                if isinstance(first_item, dict):
                    print(f"🔍 Debug: First item keys: {list(first_item.keys())[:5]}...")
                else:
                    print(f"🔍 Debug: First item content: {str(first_item)[:100]}...")
            
            return posts_data
            
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"❌ Error downloading snapshot {snapshot_id} (attempt {attempt + 1}): {e}. Retrying...")
                time.sleep(10)
            else:
                print(f"❌ Error downloading snapshot {snapshot_id} after {max_retries} attempts: {e}")
                return []
    
    return []

# === Main Scraping Functions ===
def scrape_platform_posts(platform: str, urls: List[str], start_date=None, end_date=None) -> List[Dict]:
    """
    Scrape posts from a specific platform
    
    Args:
        platform: Platform name ('facebook')
        urls: List of URLs to scrape
        start_date: Start date for scraping
        end_date: End date for scraping
    
    Returns:
        List of scraped posts
    """
    if not urls:
        return []
    
    print(f"🚀 Starting {platform} scraping for {len(urls)} URLs...")
    
    # Trigger scraping
    triggered = trigger_brightdata_scrape(urls, start_date, end_date)
    
    if platform not in triggered:
        print(f"❌ No {platform} scraping triggered")
        return []
    
    if "error" in triggered[platform]:
        print(f"❌ {platform} scraping failed: {triggered[platform]['error']}")
        return []
    
    snapshot_id = triggered[platform].get('snapshot_id')
    if not snapshot_id:
        print(f"❌ No snapshot ID returned for {platform}")
        return []
    
    # Wait for snapshot to be ready
    if not wait_for_snapshot_ready(snapshot_id):
        print(f"❌ {platform} snapshot not ready within timeout")
        return []
    
    # Download data
    posts = download_brightdata_snapshot(snapshot_id)
    
    # Add platform information to each post
    processed_posts = []
    for post in posts:
        # Ensure post is a dictionary
        if isinstance(post, dict):
            post['platform'] = platform
            post['scraped_at'] = datetime.now().isoformat()
            processed_posts.append(post)
        else:
            print(f"⚠️ Warning: Skipping non-dict post: {type(post)}")
    
    return processed_posts

def scrape_all_social_media(urls: List[str] = None, start_date=None, end_date=None) -> List[Dict]:
    """
    Scrape all social media posts from the configured URLs (Facebook only)
    
    Args:
        urls: List of URLs to scrape (uses config.ALL_URLS if None)
        start_date: Start date for scraping (uses last DAYS_BACK days if None)
        end_date: End date for scraping (uses today if None)
    
    Returns:
        List of all scraped posts
    """
    if urls is None:
        urls = config.ALL_URLS
    
    if start_date is None or end_date is None:
        start_date, end_date = get_date_range()
    
    print(f"🌐 Starting Facebook scraping for {len(urls)} URLs")
    print(f"📅 Date range: {start_date.date()} to {end_date.date()}")
    
    # Filter URLs for Facebook only
    facebook_urls = [url for url in urls if "facebook.com" in url]
    
    all_posts = []
    
    # Scrape Facebook posts
    if facebook_urls:
        facebook_posts = scrape_platform_posts("facebook", facebook_urls, start_date, end_date)
        all_posts.extend(facebook_posts)
        print(f"📘 Facebook: {len(facebook_posts)} posts scraped")
    
    print(f"🎉 Total posts scraped: {len(all_posts)}")
    return all_posts

def scrape_facebook_posts(urls: List[str] = None, start_date=None, end_date=None) -> List[Dict]:
    """Scrape Facebook posts only"""
    if urls is None:
        urls = config.FACEBOOK_URLS
    
    facebook_urls = [url for url in urls if "facebook.com" in url]
    return scrape_platform_posts("facebook", facebook_urls, start_date, end_date)


if __name__ == "__main__":
    # Test scraping
    print("🧪 Testing social media scraping...")
    
    # Check if API token is available
    if not config.BRIGHTDATA_API_TOKEN:
        print("❌ BRIGHTDATA_API_TOKEN not found in environment variables")
        exit(1)
    
    # Scrape a small sample for testing
    test_urls = config.FACEBOOK_URLS
    posts = scrape_all_social_media(test_urls)
    
    if posts:
        print(f"✅ Test successful: {len(posts)} posts scraped")
        # Save test data
        df = pd.DataFrame(posts)
        df.to_excel("test_social_media_scraping.xlsx", index=False)
        print("💾 Test data saved to test_social_media_scraping.xlsx")
    else:
        print("❌ Test failed: No posts scraped")