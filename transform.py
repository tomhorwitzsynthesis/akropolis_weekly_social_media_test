#!/usr/bin/env python3
"""
Data transformation functions for social media posts
Handles Facebook post data from Bright Data
"""

import pandas as pd
from dateutil import tz
import json
import ast
import re
from typing import List, Dict, Optional
import config

# Required columns for social media posts
REQUIRED_COLUMNS = ["platform", "post_id", "created_date", "brand", "content", "source_url"]

def flatten_posts(posts: List[Dict]) -> pd.DataFrame:
    """
    Flatten nested social media post data into a DataFrame
    
    Args:
        posts: List of post dictionaries from Bright Data
    
    Returns:
        Flattened DataFrame
    """
    if not posts:
        return pd.DataFrame()
    
    df = pd.DataFrame(posts)
    return pd.json_normalize(df.to_dict(orient="records"), sep="/") if not df.empty else df

def standardize_facebook_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize Facebook post data structure
    
    Args:
        df: DataFrame with Facebook post data
    
    Returns:
        Standardized DataFrame
    """
    df = df.copy()
    
    # Map Facebook-specific fields to standard columns
    field_mappings = {
        'post_id': ['id', 'post_id', 'snapshot/id'],
        'created_date': ['created_time', 'snapshot/created_time', 'timestamp'],
        'content': ['message', 'snapshot/message', 'text', 'snapshot/text'],
        'brand': ['page_name', 'snapshot/page_name', 'from/name'],
        'likes': ['likes/count', 'snapshot/likes/count', 'reactions/like'],
        'comments': ['comments/count', 'snapshot/comments/count'],
        'shares': ['shares/count', 'snapshot/shares/count'],
        'engagement': ['engagement', 'snapshot/engagement'],
        'reach': ['reach', 'snapshot/reach'],
        'source_url': ['source_url', 'url', 'snapshot/url']
    }
    
    # Apply field mappings
    for standard_col, candidates in field_mappings.items():
        if standard_col not in df.columns:
            for candidate in candidates:
                if candidate in df.columns:
                    df[standard_col] = df[candidate]
                    break
            else:
                df[standard_col] = pd.NA
    
    return df


def ensure_standard_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all required columns exist with proper data types
    Only keeps essential columns to avoid column structure issues
    
    Args:
        df: DataFrame with social media post data
    
    Returns:
        DataFrame with standardized columns
    """
    df = df.copy()
    
    # Define essential columns to keep
    essential_columns = [
        'platform', 'post_id', 'created_date', 'brand', 'content', 'source_url',
        'likes', 'comments', 'shares', 'total_engagement', 'engagement_rate',
        'page_name', 'user_username_raw', 'date_posted', 'num_comments', 'num_shares',
        'post_summary', 'cluster_1', 'cluster_2', 'cluster_3', 'scraped_at'
    ]
    
    # Keep only essential columns that exist in the dataframe
    existing_essential = [col for col in essential_columns if col in df.columns]
    df = df[existing_essential].copy()
    
    # Ensure platform column
    if 'platform' not in df.columns:
        df['platform'] = 'facebook'
    
    # Ensure post_id column
    if 'post_id' not in df.columns:
        df['post_id'] = pd.NA
    
    # Ensure created_date column with proper formatting - always use date_posted as source
    if 'date_posted' in df.columns:
        # Always use date_posted as the source for created_date
        df['created_date'] = pd.to_datetime(df['date_posted'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    elif 'created_date' in df.columns:
        # If date_posted doesn't exist, use existing created_date but convert properly
        df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        df['created_date'] = pd.NA
    
    # Ensure brand column (use page_name if available)
    if 'brand' not in df.columns:
        if 'page_name' in df.columns:
            df['brand'] = df['page_name']
        else:
            df['brand'] = pd.NA
    
    # Ensure content column
    if 'content' not in df.columns:
        df['content'] = pd.NA
    
    # Ensure source_url column
    if 'source_url' not in df.columns:
        df['source_url'] = pd.NA
    
    # Ensure engagement metrics columns with proper mapping
    # Map scraped data columns to standard column names
    if 'num_comments' in df.columns and 'comments' not in df.columns:
        df['comments'] = df['num_comments']
    if 'num_shares' in df.columns and 'shares' not in df.columns:
        df['shares'] = df['num_shares']
    # likes column should already exist in scraped data
    
    # Ensure all engagement metrics exist and are numeric
    for metric in ['likes', 'comments', 'shares']:
        if metric not in df.columns:
            df[metric] = 0
        else:
            # Convert to numeric, filling NaN with 0
            df[metric] = pd.to_numeric(df[metric], errors='coerce').fillna(0)
    
    # Calculate total_engagement if not present
    if 'total_engagement' not in df.columns:
        df['total_engagement'] = (df['likes'] * 1 + df['comments'] * 3 + df['shares'] * 5)
    
    # Convert all required columns to string type
    for col in ['platform', 'post_id', 'created_date', 'brand', 'content', 'source_url']:
        if col in df.columns:
            df[col] = df[col].astype('string')
    
    return df

def extract_brand_from_url(url: str) -> str:
    """
    Extract brand name from social media URL
    
    Args:
        url: Social media page URL
    
    Returns:
        Extracted brand name
    """
    if pd.isna(url) or not url:
        return "Unknown"
    
    # Facebook URL patterns
    if "facebook.com" in url:
        # Extract from facebook.com/username
        match = re.search(r'facebook\.com/([^/?]+)', url)
        if match:
            username = match.group(1)
            # Clean up the username
            username = username.replace('.', ' ').replace('_', ' ').replace('-', ' ')
            return username.title()
    
    
    return "Unknown"

def normalize_brand_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize brand names to match the expected format
    Uses page_name as the source of truth for brand names
    
    Args:
        df: DataFrame with social media post data
    
    Returns:
        DataFrame with normalized brand names
    """
    df = df.copy()
    
    # Use page_name as the exact brand name (no case changes or mappings)
    if 'page_name' in df.columns:
        df['brand'] = df['page_name']
    elif 'brand' in df.columns:
        # If page_name doesn't exist, keep existing brand but don't modify case
        pass
    else:
        # If neither exists, try to extract from source_url
        if 'source_url' in df.columns:
            df['brand'] = df['source_url'].apply(extract_brand_from_url)
        else:
            df['brand'] = 'Unknown'
    
    return df

def clean_content(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize post content
    
    Args:
        df: DataFrame with social media post data
    
    Returns:
        DataFrame with cleaned content
    """
    df = df.copy()
    
    if 'content' not in df.columns:
        return df
    
    def clean_text(text):
        if pd.isna(text) or not isinstance(text, str):
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Remove URLs (optional - might want to keep them for analysis)
        # text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
        
        # Remove excessive line breaks
        text = re.sub(r'\n+', '\n', text)
        
        return text.strip()
    
    df['content'] = df['content'].apply(clean_text)
    
    return df

def calculate_engagement_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate additional engagement metrics
    
    Args:
        df: DataFrame with social media post data
    
    Returns:
        DataFrame with calculated engagement metrics
    """
    df = df.copy()
    
    # Calculate total engagement (likes + comments + shares)
    if all(col in df.columns for col in ['likes', 'comments', 'shares']):
        df['total_engagement'] = df['likes'] + df['comments'] + df['shares']
    
    # Calculate engagement rate (if reach is available)
    if 'reach' in df.columns and 'total_engagement' in df.columns:
        df['engagement_rate'] = (df['total_engagement'] / df['reach'] * 100).fillna(0)
        df['engagement_rate'] = df['engagement_rate'].round(2)
    
    return df

def filter_recent_posts(df: pd.DataFrame, tz_name: str = None, days_back: int = None) -> pd.DataFrame:
    """
    Filter posts to only include recent ones
    
    Args:
        df: DataFrame with social media post data
        tz_name: Timezone name (uses config.TIMEZONE if None)
        days_back: Number of days back to include (uses config.DAYS_BACK if None)
    
    Returns:
        Filtered DataFrame with recent posts only
    """
    if tz_name is None:
        tz_name = config.TIMEZONE
    if days_back is None:
        days_back = config.DAYS_BACK
    
    if 'created_date' not in df.columns:
        print("⚠️ No created_date column found, skipping date filtering")
        return df
    
    df = df.copy()
    
    # Convert created_date to datetime
    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
    
    # Filter for recent posts - use timezone-naive comparison
    now = pd.Timestamp.now().normalize()
    cutoff_date = now - pd.Timedelta(days=days_back)
    
    # Convert both to timezone-naive for comparison
    df['created_date'] = df['created_date'].dt.tz_localize(None) if df['created_date'].dt.tz is not None else df['created_date']
    
    recent_posts = df[df['created_date'] >= cutoff_date].copy()
    
    print(f"📅 Filtered to {len(recent_posts)} posts from last {days_back} days (from {len(df)} total)")
    
    return recent_posts

def process_social_media_data(posts: List[Dict]) -> pd.DataFrame:
    """
    Main function to process social media posts through the complete transformation pipeline
    
    Args:
        posts: List of raw post dictionaries from Bright Data
    
    Returns:
        Processed and standardized DataFrame
    """
    if not posts:
        print("⚠️ No posts to process")
        return pd.DataFrame()
    
    print(f"🔄 Processing {len(posts)} social media posts...")
    
    # Step 1: Flatten the data
    df = flatten_posts(posts)
    print(f"📊 Flattened to {len(df)} posts")
    
    # Step 2: Standardize platform-specific data
    if 'platform' in df.columns:
        facebook_posts = df[df['platform'] == 'facebook'].copy()
        
        if not facebook_posts.empty:
            facebook_posts = standardize_facebook_data(facebook_posts)
            df = facebook_posts
    
    # Step 3: Ensure standard columns
    df = ensure_standard_columns(df)
    print(f"✅ Standardized columns: {len(df)} posts")
    
    # Step 4: Normalize brand names
    df = normalize_brand_names(df)
    print(f"🏷️ Normalized brand names: {len(df)} posts")
    
    # Step 5: Clean content
    df = clean_content(df)
    print(f"🧹 Cleaned content: {len(df)} posts")
    
    # Step 6: Calculate engagement metrics
    df = calculate_engagement_metrics(df)
    print(f"📈 Calculated engagement metrics: {len(df)} posts")
    
    # Step 7: Filter for recent posts
    df = filter_recent_posts(df)
    print(f"📅 Filtered to recent posts: {len(df)} posts")
    
    print(f"🎉 Processing complete: {len(df)} posts ready for analysis")
    
    return df

if __name__ == "__main__":
    # Test the transformation functions
    print("🧪 Testing social media data transformation...")
    
    # Create sample data
    sample_posts = [
        {
            'platform': 'facebook',
            'id': '123456789',
            'created_time': '2024-01-15T10:30:00Z',
            'message': 'Check out our new products!',
            'page_name': 'Akropolis Vilnius',
            'likes': {'count': 150},
            'comments': {'count': 25},
            'shares': {'count': 10},
            'source_url': 'https://www.facebook.com/akropolis.vilnius/'
        },
    ]
    
    # Process the sample data
    processed_df = process_social_media_data(sample_posts)
    
    if not processed_df.empty:
        print("✅ Test successful!")
        print("\nProcessed data:")
        print(processed_df[['platform', 'brand', 'content', 'likes', 'comments', 'shares']].head())
    else:
        print("❌ Test failed: No data processed")