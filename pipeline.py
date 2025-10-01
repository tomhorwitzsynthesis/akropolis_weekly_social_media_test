#!/usr/bin/env python3
"""
Social Media Intelligence Pipeline
Main pipeline for scraping, processing, and analyzing social media posts
"""

import os
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import config
from scraper import scrape_all_social_media, scrape_facebook_posts
from transform import process_social_media_data
from storage import merge_with_existing_data, save_with_backup, print_data_summary
from gpt_labeler import label_posts_with_gpt, print_cluster_stats
from summary_generator import generate_all_summaries

def main():
    """
    Main pipeline function for social media intelligence
    """
    print("🚀 Starting Social Media Intelligence Pipeline")
    print("=" * 60)
    
    # Check API token
    if not config.BRIGHTDATA_API_TOKEN:
        raise SystemExit("❌ Set BRIGHTDATA_API_TOKEN environment variable or add it to config.py")
    
    # Check OpenAI API key for GPT labeling
    if config.ENABLE_GPT_LABELING and not config.OPENAI_API_KEY:
        print("⚠️ Warning: OPENAI_API_KEY not found. GPT labeling will be skipped.")
        config.ENABLE_GPT_LABELING = False
    
    try:
        # Step 1: Scrape Facebook posts
        print("\n📡 Step 1: Scraping Facebook posts...")
        print(f"🌐 Scraping {len(config.ALL_URLS)} URLs")
        print(f"📅 Date range: Last {config.DAYS_BACK} days")
        
        all_posts = scrape_all_social_media()
        
        if not all_posts:
            print("❌ No posts scraped. Pipeline terminated.")
            return
        
        print(f"✅ Successfully scraped {len(all_posts)} posts")
        
        # Step 2: Transform and standardize data
        print("\n🔄 Step 2: Transforming and standardizing data...")
        df = process_social_media_data(all_posts)
        
        if df.empty:
            print("❌ No valid posts after transformation. Pipeline terminated.")
            return
        
        print(f"✅ Successfully processed {len(df)} posts")
        
        # Step 3: GPT Labeling (if enabled)
        if config.ENABLE_GPT_LABELING and not df.empty:
            print("\n🤖 Step 3: GPT labeling and categorization...")
            df = label_posts_with_gpt(df, config.GPT_MAX_WORKERS)
            print_cluster_stats(df)
        else:
            print("\n⏭️ Step 3: GPT labeling skipped (disabled or no data)")
        
        # Step 4: Merge with existing data and deduplicate
        print("\n💾 Step 4: Merging with existing data...")
        master_path = Path(config.MASTER_XLSX)
        combined_df = merge_with_existing_data(df, master_path)
        
        # Step 5: Save updated data
        print("\n💾 Step 5: Saving updated data...")
        save_with_backup(combined_df, master_path)
        
        # Step 6: Generate weekly summaries (if enabled)
        if config.ENABLE_WEEKLY_SUMMARIES:
            print("\n📝 Step 6: Generating weekly summaries...")
            try:
                summary_path = generate_all_summaries()
                print(f"✅ Weekly summaries generated: {summary_path}")
            except Exception as e:
                print(f"❌ Failed to generate summaries: {e}")
        else:
            print("\n⏭️ Step 6: Weekly summaries disabled in config")
        
        # Final summary
        print("\n🎉 Pipeline completed successfully!")
        print("=" * 60)
        print_data_summary(combined_df)
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        raise

def scrape_facebook_only():
    """
    Scrape Facebook posts only
    """
    print("📘 Scraping Facebook posts only...")
    
    if not config.BRIGHTDATA_API_TOKEN:
        raise SystemExit("❌ Set BRIGHTDATA_API_TOKEN environment variable")
    
    posts = scrape_facebook_posts()
    
    if posts:
        df = process_social_media_data(posts)
        master_path = Path(config.MASTER_XLSX)
        combined_df = merge_with_existing_data(df, master_path)
        save_with_backup(combined_df, master_path)
        print_data_summary(combined_df)
    else:
        print("❌ No Facebook posts scraped")

def continue_from_snapshot(snapshot_id):
    """
    Continue pipeline from existing Bright Data snapshot
    """
    print(f"📥 Continuing pipeline from snapshot: {snapshot_id}")
    
    if not config.BRIGHTDATA_API_TOKEN:
        raise SystemExit("❌ Set BRIGHTDATA_API_TOKEN environment variable")
    
    # Check OpenAI API key for GPT labeling
    if config.ENABLE_GPT_LABELING and not config.OPENAI_API_KEY:
        print("⚠️ Warning: OPENAI_API_KEY not found. GPT labeling will be skipped.")
        config.ENABLE_GPT_LABELING = False
    
    try:
        # Import here to avoid circular imports
        from scraper import download_brightdata_snapshot
        
        # Step 1: Download the snapshot
        print("\n📥 Step 1: Downloading snapshot data...")
        posts = download_brightdata_snapshot(snapshot_id)
        
        if not posts:
            print("❌ No posts downloaded from snapshot. Pipeline terminated.")
            return
        
        print(f"✅ Successfully downloaded {len(posts)} posts from snapshot")
        
        # Add platform information to each post
        processed_posts = []
        for post in posts:
            if isinstance(post, dict):
                post['platform'] = 'facebook'
                post['scraped_at'] = datetime.now().isoformat()
                processed_posts.append(post)
            else:
                print(f"⚠️ Warning: Skipping non-dict post: {type(post)}")
        
        if not processed_posts:
            print("❌ No valid posts after processing. Pipeline terminated.")
            return
        
        # Step 2: Transform and standardize data
        print("\n🔄 Step 2: Transforming and standardizing data...")
        df = process_social_media_data(processed_posts)
        
        if df.empty:
            print("❌ No valid posts after transformation. Pipeline terminated.")
            return
        
        print(f"✅ Successfully processed {len(df)} posts")
        
        # Step 3: GPT Labeling (if enabled)
        if config.ENABLE_GPT_LABELING and not df.empty:
            print("\n🤖 Step 3: GPT labeling and categorization...")
            df = label_posts_with_gpt(df, config.GPT_MAX_WORKERS)
            print_cluster_stats(df)
        else:
            print("\n⏭️ Step 3: GPT labeling skipped (disabled or no data)")
        
        # Step 4: Merge with existing data and deduplicate
        print("\n💾 Step 4: Merging with existing data...")
        master_path = Path(config.MASTER_XLSX)
        combined_df = merge_with_existing_data(df, master_path)
        
        # Step 5: Save updated data
        print("\n💾 Step 5: Saving updated data...")
        save_with_backup(combined_df, master_path)
        
        # Step 6: Generate summaries (if enabled)
        if config.ENABLE_WEEKLY_SUMMARIES:
            print("\n📝 Step 6: Generating summaries...")
            try:
                summary_path = generate_all_summaries()
                print(f"✅ Summaries generated: {summary_path}")
            except Exception as e:
                print(f"❌ Failed to generate summaries: {e}")
        else:
            print("\n⏭️ Step 6: Summaries disabled in config")
        
        # Final summary
        print("\n🎉 Pipeline completed successfully!")
        print("=" * 60)
        print_data_summary(combined_df)
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        raise


def process_existing_data():
    """
    Process existing data without scraping (useful for re-running GPT labeling)
    """
    print("🔄 Processing existing data...")
    
    master_path = Path(config.MASTER_XLSX)
    if not master_path.exists():
        print("❌ No existing data file found")
        return
    
    # Load existing data
    df = pd.read_excel(master_path)
    print(f"📂 Loaded {len(df)} existing posts")
    
    # Apply GPT labeling if enabled
    if config.ENABLE_GPT_LABELING and not df.empty:
        print("🤖 Applying GPT labeling to existing data...")
        df = label_posts_with_gpt(df, config.GPT_MAX_WORKERS)
        print_cluster_stats(df)
        
        # Save updated data
        save_with_backup(df, master_path)
        print("✅ Updated data saved")
    else:
        print("⏭️ GPT labeling skipped")
    
    # Generate summaries
    if config.ENABLE_WEEKLY_SUMMARIES:
        print("📝 Generating summaries...")
        try:
            summary_path = generate_all_summaries()
            print(f"✅ Summaries generated: {summary_path}")
        except Exception as e:
            print(f"❌ Failed to generate summaries: {e}")

def generate_summaries_only():
    """
    Generate summaries only (useful for updating summaries without re-scraping)
    """
    print("📝 Generating summaries only...")
    
    if config.ENABLE_WEEKLY_SUMMARIES:
        try:
            summary_path = generate_all_summaries()
            print(f"✅ Summaries generated: {summary_path}")
        except Exception as e:
            print(f"❌ Failed to generate summaries: {e}")
    else:
        print("⏭️ Weekly summaries disabled in config")

def get_pipeline_status():
    """
    Get current status of the pipeline and data
    """
    print("📊 Pipeline Status")
    print("=" * 40)
    
    # Check configuration
    print(f"🔧 Configuration:")
    print(f"  - Days back: {config.DAYS_BACK}")
    print(f"  - Max posts per company: {config.MAX_POSTS}")
    print(f"  - GPT labeling: {'Enabled' if config.ENABLE_GPT_LABELING else 'Disabled'}")
    print(f"  - Weekly summaries: {'Enabled' if config.ENABLE_WEEKLY_SUMMARIES else 'Disabled'}")
    
    # Check API tokens
    print(f"\n🔑 API Tokens:")
    print(f"  - Bright Data: {'✅ Set' if config.BRIGHTDATA_API_TOKEN else '❌ Missing'}")
    print(f"  - OpenAI: {'✅ Set' if config.OPENAI_API_KEY else '❌ Missing'}")
    
    # Check data files
    print(f"\n📁 Data Files:")
    master_path = Path(config.MASTER_XLSX)
    summaries_path = Path(config.SUMMARIES_XLSX)
    
    if master_path.exists():
        df = pd.read_excel(master_path)
        print(f"  - Master data: ✅ {len(df)} posts")
        if 'created_date' in df.columns:
            dates = pd.to_datetime(df['created_date'], errors='coerce').dropna()
            if not dates.empty:
                print(f"    Latest post: {dates.max().date()}")
                print(f"    Oldest post: {dates.min().date()}")
    else:
        print(f"  - Master data: ❌ Not found")
    
    if summaries_path.exists():
        print(f"  - Summaries: ✅ Available")
    else:
        print(f"  - Summaries: ❌ Not found")
    
    # Check URLs
    print(f"\n🌐 URLs to scrape:")
    print(f"  - Facebook: {len(config.FACEBOOK_URLS)} URLs")
    print(f"  - Total: {len(config.ALL_URLS)} URLs")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "facebook":
            scrape_facebook_only()
        elif command == "process":
            process_existing_data()
        elif command == "summaries":
            generate_summaries_only()
        elif command == "status":
            get_pipeline_status()
        elif command == "snapshot" and len(sys.argv) > 2:
            snapshot_id = sys.argv[2]
            continue_from_snapshot(snapshot_id)
        else:
            print(f"❌ Unknown command: {command}")
            print("Available commands: facebook, process, summaries, status, snapshot <snapshot_id>")
    else:
        # Run full pipeline
        main()