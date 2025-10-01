#!/usr/bin/env python3
"""
Weekly Summary Generator for Social Media Intelligence Dashboard
Generates LLM-powered summaries for Akropolis and competitor social media performance
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed
import config

# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY", config.OPENAI_API_KEY))

# Brand groupings (from config)
AKROPOLIS_LOCATIONS = config.AKROPOLIS_LOCATIONS
BIG_PLAYERS = config.BIG_PLAYERS
SMALLER_PLAYERS = config.SMALLER_PLAYERS
OTHER_CITIES = config.OTHER_CITIES
RETAIL = config.RETAIL

ALL_COMPETITORS = BIG_PLAYERS + SMALLER_PLAYERS + OTHER_CITIES + RETAIL

def load_and_filter_data():
    """Load data from Facebook master file and filter for last 14 days and previous 7 days"""
    # Load Facebook data
    facebook_df = pd.read_excel(config.FACEBOOK_MASTER_XLSX)
    facebook_df['platform'] = 'facebook'
    
    # Use Facebook data only
    df = facebook_df
    
    # Parse dates
    df["date"] = pd.to_datetime(df["created_date"], errors="coerce")
    df["likes"] = pd.to_numeric(df["likes"], errors="coerce").fillna(0)
    df["comments"] = pd.to_numeric(df["comments"], errors="coerce").fillna(0)
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0)
    
    # Calculate weighted engagement: like=1, comment=3, share=5
    df["total_engagement"] = (df["likes"] * 1 + df["comments"] * 3 + df["shares"] * 5)
    
    # Get date ranges - last 14 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=14)
    
    # Split into two 7-day periods for comparison
    # Current week: most recent 7 days (days 1-7 from today)
    # Previous week: older 7 days (days 8-14 from today)
    current_7_days_end = end_date
    current_7_days_start = end_date - timedelta(days=6)  # 7 days total including end_date
    prev_7_days_end = current_7_days_start - timedelta(days=1)
    prev_7_days_start = prev_7_days_end - timedelta(days=6)  # 7 days total
    
    # Filter data
    df_14_days = df[
        (df["date"].dt.date >= start_date) & 
        (df["date"].dt.date <= end_date)
    ].copy()
    
    df_current_week = df[
        (df["date"].dt.date >= current_7_days_start) & 
        (df["date"].dt.date <= current_7_days_end)
    ].copy()
    
    df_previous_week = df[
        (df["date"].dt.date >= prev_7_days_start) & 
        (df["date"].dt.date <= prev_7_days_end)
    ].copy()
    
    return df_14_days, df_current_week, df_previous_week, start_date, end_date

def get_brand_stats(df_current_week, df_previous_week, brand_name):
    """Get statistics for a specific brand"""
    current_data = df_current_week[df_current_week["brand"] == brand_name]
    previous_data = df_previous_week[df_previous_week["brand"] == brand_name]
    
    current_posts = current_data["post_id"].nunique()
    current_engagement = current_data["total_engagement"].sum()
    current_likes = current_data["likes"].sum()
    current_comments = current_data["comments"].sum()
    current_shares = current_data["shares"].sum()
    
    previous_posts = previous_data["post_id"].nunique()
    previous_engagement = previous_data["total_engagement"].sum()
    previous_likes = previous_data["likes"].sum()
    previous_comments = previous_data["comments"].sum()
    previous_shares = previous_data["shares"].sum()
    
    # Calculate percentage changes
    posts_change = ((current_posts - previous_posts) / previous_posts * 100) if previous_posts > 0 else (100 if current_posts > 0 else 0)
    engagement_change = ((current_engagement - previous_engagement) / previous_engagement * 100) if previous_engagement > 0 else (100 if current_engagement > 0 else 0)
    likes_change = ((current_likes - previous_likes) / previous_likes * 100) if previous_likes > 0 else (100 if current_likes > 0 else 0)
    comments_change = ((current_comments - previous_comments) / previous_comments * 100) if previous_comments > 0 else (100 if current_comments > 0 else 0)
    shares_change = ((current_shares - previous_shares) / previous_shares * 100) if previous_shares > 0 else (100 if current_shares > 0 else 0)
    
    # Get platform breakdown
    current_platforms = current_data["platform"].value_counts().to_dict()
    previous_platforms = previous_data["platform"].value_counts().to_dict()
    
    return {
        "current_posts": current_posts,
        "current_engagement": current_engagement,
        "current_likes": current_likes,
        "current_comments": current_comments,
        "current_shares": current_shares,
        "previous_posts": previous_posts,
        "previous_engagement": previous_engagement,
        "previous_likes": previous_likes,
        "previous_comments": previous_comments,
        "previous_shares": previous_shares,
        "posts_change": posts_change,
        "engagement_change": engagement_change,
        "likes_change": likes_change,
        "comments_change": comments_change,
        "shares_change": shares_change,
        "current_content": current_data["content"].dropna().tolist(),
        "previous_content": previous_data["content"].dropna().tolist(),
        "current_clusters": current_data["cluster_1"].dropna().tolist(),
        "previous_clusters": previous_data["cluster_1"].dropna().tolist(),
        "current_platforms": current_platforms,
        "previous_platforms": previous_platforms,
    }

def generate_competitor_summary(brand_name, stats):
    """Generate summary for a specific competitor brand"""
    if stats["current_posts"] == 0 and stats["previous_posts"] == 0:
        return f"{brand_name} had no social media posts in both this week and the previous week."
    
    prompt = f"""
You are analyzing social media performance for {brand_name} in Lithuania. Please provide a factual summary of their social media performance this week (most recent 7 days) compared to the previous week (7 days before that).

PERFORMANCE METRICS:
- This week (most recent 7 days): {stats['current_posts']} posts, {stats['current_engagement']:,.0f} total engagement ({stats['current_likes']:,.0f} likes, {stats['current_comments']:,.0f} comments, {stats['current_shares']:,.0f} shares)
- Previous week (7 days before): {stats['previous_posts']} posts, {stats['previous_engagement']:,.0f} total engagement ({stats['previous_likes']:,.0f} likes, {stats['previous_comments']:,.0f} comments, {stats['previous_shares']:,.0f} shares)
- Posts change: {stats['posts_change']:+.1f}%
- Engagement change: {stats['engagement_change']:+.1f}%
- Likes change: {stats['likes_change']:+.1f}%
- Comments change: {stats['comments_change']:+.1f}%
- Shares change: {stats['shares_change']:+.1f}%

PLATFORM BREAKDOWN:
- This week platforms: {stats['current_platforms']}
- Previous week platforms: {stats['previous_platforms']}

THIS WEEK POST CONTENT (first 15 posts):
{chr(10).join(stats['current_content'][:15])}

PREVIOUS WEEK POST CONTENT (first 15 posts):
{chr(10).join(stats['previous_content'][:15])}

THIS WEEK CLUSTERS:
{', '.join(stats['current_clusters'])}

PREVIOUS WEEK CLUSTERS:
{', '.join(stats['previous_clusters'])}

Please provide a concise 2-3 paragraph summary covering:
1. Performance metrics (posts and engagement changes)
2. Content focus areas and changes
3. Specific examples of posts posted this week vs the previous week

Focus only on facts and actual data. Do not make assumptions about strategy, intentions, or potential outcomes. Include specific examples of actual posts posted.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=500
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"Error generating summary: {str(e)}"

def generate_single_summary(brand_name, df_current_week, df_previous_week):
    """Generate summary for a single brand (for parallel processing)"""
    # Now treat Akropolis locations as individual brands
    stats = get_brand_stats(df_current_week, df_previous_week, brand_name)
    return generate_competitor_summary(brand_name, stats)

def generate_all_summaries():
    """Generate summaries for all brands and append to Excel file using parallel processing"""
    print("Loading social media data...")
    df_14_days, df_current_week, df_previous_week, start_date, end_date = load_and_filter_data()
    
    # Prepare all brands for parallel processing (including individual Akropolis locations)
    all_brands = AKROPOLIS_LOCATIONS + ALL_COMPETITORS
    
    print(f"Generating summaries for {len(all_brands)} brands using parallel processing...")
    
    summaries = {
        "start_date": start_date,
        "end_date": end_date
    }
    
    # Use ThreadPoolExecutor for parallel processing
    max_workers = min(config.GPT_MAX_WORKERS, len(all_brands))  # Use config setting or number of brands
    print(f"Using {max_workers} parallel workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_brand = {
            executor.submit(generate_single_summary, brand, df_current_week, df_previous_week): brand 
            for brand in all_brands
        }
        
        # Collect results as they complete
        completed = 0
        for future in as_completed(future_to_brand):
            brand = future_to_brand[future]
            try:
                summary = future.result()
                summaries[brand] = summary
                completed += 1
                safe_brand = brand.encode('ascii', errors='ignore').decode('ascii')
                print(f"Completed {completed}/{len(all_brands)}: {safe_brand}")
            except Exception as e:
                safe_brand = brand.encode('ascii', errors='ignore').decode('ascii')
                print(f"Error generating summary for {safe_brand}: {e}")
                summaries[brand] = f"Error generating summary: {str(e)}"
                completed += 1
    
    # Create DataFrame for new summaries
    df_new_summaries = pd.DataFrame([summaries])
    
    # Ensure data directory exists
    Path("data").mkdir(exist_ok=True)
    
    # Load existing summaries or create new file
    output_path = "data/summaries.xlsx"
    if Path(output_path).exists():
        try:
            df_existing = pd.read_excel(output_path)
            # Append new summaries to existing data
            df_combined = pd.concat([df_existing, df_new_summaries], ignore_index=True)
            print(f"Appending new summaries to existing file with {len(df_existing)} existing rows")
        except Exception as e:
            print(f"Error reading existing summaries file: {e}. Creating new file.")
            df_combined = df_new_summaries
    else:
        df_combined = df_new_summaries
        print("Creating new summaries file")
    
    # Save to Excel
    df_combined.to_excel(output_path, index=False)
    
    print(f"All summaries completed and saved to {output_path}")
    return output_path

def get_engagement_insights(df_current_week, df_previous_week):
    """Generate insights about engagement patterns"""
    if df_current_week.empty and df_previous_week.empty:
        return "No engagement data available for analysis."
    
    # Calculate overall engagement trends
    current_avg_engagement = df_current_week["total_engagement"].mean() if not df_current_week.empty else 0
    previous_avg_engagement = df_previous_week["total_engagement"].mean() if not df_previous_week.empty else 0
    
    # Top performing posts
    top_current = df_current_week.nlargest(3, "total_engagement") if not df_current_week.empty else pd.DataFrame()
    top_previous = df_previous_week.nlargest(3, "total_engagement") if not df_previous_week.empty else pd.DataFrame()
    
    # Platform performance
    platform_current = df_current_week.groupby("platform")["total_engagement"].sum() if not df_current_week.empty else pd.Series()
    platform_previous = df_previous_week.groupby("platform")["total_engagement"].sum() if not df_previous_week.empty else pd.Series()
    
    insights = f"""
ENGAGEMENT INSIGHTS:

Average Engagement:
- Current week: {current_avg_engagement:.1f} per post
- Previous week: {previous_avg_engagement:.1f} per post
- Change: {((current_avg_engagement - previous_avg_engagement) / previous_avg_engagement * 100) if previous_avg_engagement > 0 else 0:+.1f}%

Top Performing Posts This Week:
"""
    
    if not top_current.empty:
        for i, (_, post) in enumerate(top_current.iterrows(), 1):
            insights += f"{i}. {post['brand']}: {post['total_engagement']} engagement - {post['content'][:100]}...\n"
    else:
        insights += "No posts this week.\n"
    
    insights += "\nPlatform Performance This Week:\n"
    if not platform_current.empty:
        for platform, engagement in platform_current.items():
            insights += f"- {platform}: {engagement:,.0f} total engagement\n"
    else:
        insights += "No platform data available.\n"
    
    return insights

if __name__ == "__main__":
    if config.ENABLE_WEEKLY_SUMMARIES:
        generate_all_summaries()
    else:
        print("Weekly summaries are disabled in config.")