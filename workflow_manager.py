#!/usr/bin/env python3
"""
Workflow Manager for Social Media Intelligence
Handles appending new scraped data to master files and automatic labeling
"""

import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import config
from gpt_labeler import label_posts_with_gpt
from storage import load_excel, save_excel, deduplicate_posts, merge_with_existing_data

def append_new_data_to_master(platform, new_data_file):
    """
    Append new scraped data to the master file and apply GPT labeling
    
    Args:
        platform (str): 'facebook'
        new_data_file (str): Path to the new scraped data file
    """
    print(f"🔄 Processing new {platform} data from {new_data_file}")
    
    # Load new data
    if not os.path.exists(new_data_file):
        print(f"❌ Error: File {new_data_file} not found")
        return False
    
    new_df = pd.read_excel(new_data_file)
    print(f"📊 Loaded {len(new_df)} new posts from {new_data_file}")
    
    # Determine master file path
    if platform.lower() == 'facebook':
        master_file = config.FACEBOOK_MASTER_XLSX
    else:
        print(f"❌ Error: Invalid platform '{platform}'. Must be 'facebook'")
        return False
    
    # Load existing master data
    if os.path.exists(master_file):
        existing_df = load_excel(master_file)
        print(f"📁 Loaded {len(existing_df)} existing posts from master file")
    else:
        existing_df = pd.DataFrame()
        print("📁 No existing master file found, creating new one")
    
    # Ensure new data has platform column
    new_df['platform'] = platform.lower()
    
    # Merge with existing data (deduplication happens in merge function)
    merged_df = merge_with_existing_data(existing_df, new_df)
    print(f"🔗 Merged data: {len(merged_df)} total posts")
    
    # Identify new posts that need labeling
    if len(existing_df) > 0:
        existing_post_ids = set(existing_df['post_id'].astype(str))
        new_posts_mask = ~merged_df['post_id'].astype(str).isin(existing_post_ids)
        new_posts_df = merged_df[new_posts_mask].copy()
    else:
        new_posts_df = merged_df.copy()
    
    print(f"🏷️  Found {len(new_posts_df)} new posts that need GPT labeling")
    
    # Apply GPT labeling to new posts
    if len(new_posts_df) > 0:
        print("🤖 Starting GPT labeling for new posts...")
        try:
            labeled_df = label_posts_with_gpt(new_posts_df)
            print(f"✅ Successfully labeled {len(labeled_df)} new posts")
            
            # Update the merged dataframe with labeled data
            merged_df.loc[new_posts_mask, ['post_summary', 'cluster_1', 'cluster_2', 'cluster_3']] = labeled_df[['post_summary', 'cluster_1', 'cluster_2', 'cluster_3']]
            
        except Exception as e:
            print(f"⚠️  Warning: GPT labeling failed: {e}")
            print("💾 Saving data without labels (can be labeled later)")
    else:
        print("ℹ️  No new posts to label")
    
    # Calculate engagement metrics
    merged_df["likes"] = pd.to_numeric(merged_df["likes"], errors="coerce").fillna(0)
    merged_df["comments"] = pd.to_numeric(merged_df["comments"], errors="coerce").fillna(0)
    merged_df["shares"] = pd.to_numeric(merged_df["shares"], errors="coerce").fillna(0)
    merged_df["total_engagement"] = (merged_df["likes"] * 1 + merged_df["comments"] * 3 + merged_df["shares"] * 5)
    
    # Save updated master file
    save_excel(merged_df, master_file)
    print(f"💾 Saved updated master file: {master_file}")
    print(f"📈 Total posts in master file: {len(merged_df)}")
    
    return True

def process_new_scraped_files(facebook_file=None):
    """
    Process new scraped files and append to master files
    
    Args:
        facebook_file (str, optional): Path to new Facebook data file
    """
    print("🚀 Starting workflow manager for new scraped data")
    print(f"📅 Analysis period: {config.ANALYSIS_START_DATE} to {config.ANALYSIS_END_DATE}")
    
    success_count = 0
    
    if facebook_file:
        if append_new_data_to_master('facebook', facebook_file):
            success_count += 1
    
    if success_count > 0:
        print(f"✅ Workflow completed successfully! Processed {success_count} platform(s)")
        print("🔄 You can now run the dashboard or regenerate summaries")
    else:
        print("❌ No files were processed successfully")

def regenerate_summaries_after_update():
    """Regenerate summaries after updating master files"""
    print("📝 Regenerating AI summaries with updated data...")
    try:
        import subprocess
        result = subprocess.run(['python', 'summary_generator.py'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Summaries regenerated successfully!")
        else:
            print(f"❌ Error regenerating summaries: {result.stderr}")
    except Exception as e:
        print(f"❌ Error running summary generator: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python workflow_manager.py --facebook path/to/facebook_data.xlsx")
        print("  python workflow_manager.py --summaries  # Regenerate summaries only")
        sys.exit(1)
    
    facebook_file = None
    regenerate_summaries = False
    
    i = 1
    while i < len(sys.argv):
        if sys.argv[i] == '--facebook' and i + 1 < len(sys.argv):
            facebook_file = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == '--summaries':
            regenerate_summaries = True
            i += 1
        else:
            i += 1
    
    if regenerate_summaries:
        regenerate_summaries_after_update()
    else:
        process_new_scraped_files(facebook_file)

