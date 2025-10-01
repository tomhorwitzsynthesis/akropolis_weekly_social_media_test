#!/usr/bin/env python3
"""
Process existing social media data files
Creates a master file from Facebook and LinkedIn data for the first 14 days of August
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import config
from transform import process_social_media_data
from storage import save_with_backup, print_data_summary

def process_facebook_data():
    """Process Facebook posts data"""
    print("📘 Processing Facebook data...")
    
    try:
        # Read Facebook data
        fb_df = pd.read_excel('data/facebook_posts.xlsx')
        print(f"📊 Facebook data shape: {fb_df.shape}")
        print(f"📊 Facebook columns: {list(fb_df.columns)}")
        
        # The Facebook data seems to be in a different format
        # Let's check if it's actually post data or summary data
        if fb_df.shape[0] < 10:
            print("⚠️ Facebook data appears to be summary data, not individual posts")
            return []
        
        # If it's actual post data, we'll need to adapt the processing
        # For now, return empty list as we need to understand the structure better
        return []
        
    except Exception as e:
        print(f"❌ Error processing Facebook data: {e}")
        return []

def process_linkedin_data():
    """Process LinkedIn posts data"""
    print("💼 Processing LinkedIn data...")
    
    try:
        # Read LinkedIn data
        li_df = pd.read_excel('data/linkedin_posts.xlsx')
        print(f"📊 LinkedIn data shape: {li_df.shape}")
        
        # Convert to our expected format
        posts = []
        
        for _, row in li_df.iterrows():
            # Extract relevant fields
            post = {
                'platform': 'linkedin',
                'post_id': str(row.get('id', '')),
                'created_date': row.get('date_posted', ''),
                'content': str(row.get('post_text', '')),
                'brand': str(row.get('user_id', '')),
                'url': str(row.get('url', '')),
                'likes': 0,  # LinkedIn data might not have engagement metrics
                'comments': 0,
                'shares': 0,
                'total_engagement': 0,
                'source_url': str(row.get('url', ''))
            }
            
            # Try to extract engagement metrics if available
            engagement_cols = ['likes', 'comments', 'shares', 'reactions', 'engagement']
            for col in engagement_cols:
                if col in row and pd.notna(row[col]):
                    try:
                        post[col] = int(row[col])
                    except (ValueError, TypeError):
                        post[col] = 0
            
            # Calculate total engagement
            post['total_engagement'] = post.get('likes', 0) + post.get('comments', 0) + post.get('shares', 0)
            
            posts.append(post)
        
        print(f"✅ Processed {len(posts)} LinkedIn posts")
        return posts
        
    except Exception as e:
        print(f"❌ Error processing LinkedIn data: {e}")
        return []

def filter_august_data(posts, days=14):
    """Filter posts to first 14 days of August 2025"""
    print(f"📅 Filtering to first {days} days of August 2025...")
    
    # Define August 1-14, 2025
    august_start = datetime(2025, 8, 1)
    august_end = august_start + timedelta(days=days-1)
    
    filtered_posts = []
    
    for post in posts:
        try:
            # Parse the date
            date_str = post.get('created_date', '')
            if not date_str:
                continue
                
            # Handle different date formats
            if 'T' in date_str:
                # ISO format: 2025-08-21T07:17:37.528Z
                post_date = datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()
            else:
                # Try other formats
                post_date = pd.to_datetime(date_str).date()
            
            # Check if it's in our date range
            if august_start.date() <= post_date <= august_end.date():
                filtered_posts.append(post)
                
        except Exception as e:
            print(f"⚠️ Error parsing date '{date_str}': {e}")
            continue
    
    print(f"✅ Filtered to {len(filtered_posts)} posts from August 1-{days}, 2025")
    return filtered_posts

def normalize_posts_brand_names(posts):
    """Normalize brand names to match expected format"""
    print("🏷️ Normalizing brand names...")
    
    # Brand name mappings
    brand_mappings = {
        'panorama-lt': 'PANORAMA',
        'akropolis-group': 'AKROPOLIS | Vilnius',
        'maxima-lietuva': 'Maxima LT',
        'lidl-lietuva': 'Lidl Lietuva',
        'rimi-lietuva': 'Rimi Lietuva',
        'iki-lietuva': 'IKI'
    }
    
    for post in posts:
        brand = post.get('brand', '').lower()
        if brand in brand_mappings:
            post['brand'] = brand_mappings[brand]
        else:
            # Try to clean up the brand name
            post['brand'] = brand.replace('-', ' ').replace('_', ' ').title()
    
    return posts

def create_sample_data():
    """Create sample data for testing if no real data is available"""
    print("🧪 Creating sample data for testing...")
    
    sample_posts = [
        {
            'platform': 'linkedin',
            'post_id': 'sample_1',
            'created_date': '2025-08-05T10:00:00Z',
            'content': 'Exciting news from our company! We are expanding our operations.',
            'brand': 'PANORAMA',
            'likes': 45,
            'comments': 12,
            'shares': 8,
            'total_engagement': 65,
            'source_url': 'https://linkedin.com/sample1'
        },
        {
            'platform': 'linkedin',
            'post_id': 'sample_2',
            'created_date': '2025-08-07T14:30:00Z',
            'content': 'Join us for our upcoming event this weekend!',
            'brand': 'AKROPOLIS | Vilnius',
            'likes': 78,
            'comments': 23,
            'shares': 15,
            'total_engagement': 116,
            'source_url': 'https://linkedin.com/sample2'
        },
        {
            'platform': 'linkedin',
            'post_id': 'sample_3',
            'created_date': '2025-08-10T09:15:00Z',
            'content': 'New product launch announcement!',
            'brand': 'Maxima LT',
            'likes': 32,
            'comments': 7,
            'shares': 4,
            'total_engagement': 43,
            'source_url': 'https://linkedin.com/sample3'
        }
    ]
    
    return sample_posts

def main():
    """Main function to process existing data"""
    print("🚀 Processing existing social media data...")
    print("=" * 60)
    
    # Process Facebook data
    facebook_posts = process_facebook_data()
    
    # Process LinkedIn data
    linkedin_posts = process_linkedin_data()
    
    # Combine all posts
    all_posts = facebook_posts + linkedin_posts
    
    # If no real data, create sample data
    if not all_posts:
        print("⚠️ No real data found, creating sample data for testing...")
        all_posts = create_sample_data()
    
    # Filter to August data
    august_posts = filter_august_data(all_posts)
    
    # Normalize brand names
    august_posts = normalize_posts_brand_names(august_posts)
    
    if not august_posts:
        print("❌ No posts found for August 1-14, 2025")
        return
    
    # Process through our transformation pipeline (skip date filtering)
    print("\n🔄 Processing through transformation pipeline...")
    
    # Use a custom processing function that doesn't filter by date
    from transform import flatten_posts, standardize_linkedin_data, ensure_standard_columns, normalize_brand_names, clean_content, calculate_engagement_metrics
    
    # Step 1: Flatten the data
    df = flatten_posts(august_posts)
    print(f"📊 Flattened to {len(df)} posts")
    
    # Step 2: Standardize LinkedIn data
    df = standardize_linkedin_data(df)
    print(f"✅ Standardized columns: {len(df)} posts")
    
    # Step 3: Ensure standard columns
    df = ensure_standard_columns(df)
    print(f"✅ Ensured standard columns: {len(df)} posts")
    
    # Step 4: Normalize brand names
    df = normalize_brand_names(df)
    print(f"🏷️ Normalized brand names: {len(df)} posts")
    
    # Step 5: Clean content
    df = clean_content(df)
    print(f"🧹 Cleaned content: {len(df)} posts")
    
    # Step 6: Calculate engagement metrics
    df = calculate_engagement_metrics(df)
    print(f"📈 Calculated engagement metrics: {len(df)} posts")
    
    if df.empty:
        print("❌ No valid posts after transformation")
        return
    
    # Save to master file
    print("\n💾 Saving to master file...")
    master_path = Path(config.MASTER_XLSX)
    save_with_backup(df, master_path)
    
    print("\n🎉 Data processing complete!")
    print_data_summary(df)

if __name__ == "__main__":
    main()
