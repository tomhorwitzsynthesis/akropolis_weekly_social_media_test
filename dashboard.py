#!/usr/bin/env python3
"""
Facebook Social Media Intelligence Dashboard
Streamlit dashboard for analyzing Facebook social media performance across brands
"""

import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta
import numpy as np
import config

st.set_page_config(page_title=f"Facebook Social Media Intelligence – {config.ANALYSIS_START_DATE.strftime('%B %d')}-{config.ANALYSIS_END_DATE.strftime('%d, %Y')}", layout="wide")

# ---- Brand Groups (from config) ----
AKROPOLIS_LOCATIONS = config.AKROPOLIS_LOCATIONS
BIG_PLAYERS = config.BIG_PLAYERS
SMALLER_PLAYERS = config.SMALLER_PLAYERS
OTHER_CITIES = config.OTHER_CITIES
RETAIL = config.RETAIL

SUBSETS_CORE = {
    "Big players": BIG_PLAYERS,
    "Smaller players": SMALLER_PLAYERS,
    "Other cities": OTHER_CITIES,
}
SUBSETS_WITH_RETAIL = {
    **SUBSETS_CORE,
    "Retail": RETAIL,
}

# ---- Load data from master files ----
FACEBOOK_FILE_PATH = config.FACEBOOK_MASTER_XLSX

@st.cache_data(show_spinner=False)
def get_available_periods():
    """Get available 14-day periods from summaries file"""
    try:
        summaries_df = pd.read_excel(config.SUMMARIES_XLSX)
        if summaries_df.empty:
            return []
        
        periods = []
        for i, row in summaries_df.iterrows():
            start_date = pd.to_datetime(row['start_date']).date()
            end_date = pd.to_datetime(row['end_date']).date()
            period_label = f"{start_date.strftime('%B %d')} - {end_date.strftime('%B %d, %Y')}"
            periods.append({
                'index': i,
                'label': period_label,
                'start_date': start_date,
                'end_date': end_date
            })
        
        return periods
    except Exception as e:
        st.error(f"Error loading available periods: {e}")
        return []

@st.cache_data(show_spinner=False)
def load_data(custom_start_date=None, custom_end_date=None):
    """Load and process data from Facebook master file"""
    # Load Facebook data
    facebook_df = pd.read_excel(FACEBOOK_FILE_PATH)
    facebook_df['platform'] = 'facebook'  # Ensure platform is set
    
    # Use Facebook data as the main dataset
    df = facebook_df
    
    # Parse dates
    df["date"] = pd.to_datetime(df["created_date"], errors="coerce")
    
    # Convert engagement metrics to numeric
    df["likes"] = pd.to_numeric(df["likes"], errors="coerce").fillna(0)
    df["comments"] = pd.to_numeric(df["comments"], errors="coerce").fillna(0)
    df["shares"] = pd.to_numeric(df["shares"], errors="coerce").fillna(0)
    
    # Calculate weighted engagement: like=1, comment=3, share=5
    df["total_engagement"] = (df["likes"] * 1 + df["comments"] * 3 + df["shares"] * 5)
    
    # Use custom date ranges if provided, otherwise use config defaults
    if custom_start_date and custom_end_date:
        last_14_days_start = custom_start_date
        last_14_days_end = custom_end_date
    else:
        last_14_days_start = config.ANALYSIS_START_DATE
        last_14_days_end = config.ANALYSIS_END_DATE
    
    # Calculate current and previous week dynamically
    # Split the analysis period into two periods (most recent 7 days vs 7 days before that)
    from datetime import timedelta
    
    # Current week: most recent 7 days (days 1-7 from end date)
    # Previous week: older 7 days (days 8-14 from end date)
    current_7_days_end = last_14_days_end
    current_7_days_start = last_14_days_end - timedelta(days=6)  # 7 days total including end_date
    prev_7_days_end = current_7_days_start - timedelta(days=1)
    prev_7_days_start = prev_7_days_end - timedelta(days=6)  # 7 days total
    
    # Filter for last 14 days (for charts)
    df_14_days = df[
        (df["date"].dt.date >= last_14_days_start) & 
        (df["date"].dt.date <= last_14_days_end)
    ].copy()
    
    # Filter for current 7 days (for comparison stats)
    df_current = df[
        (df["date"].dt.date >= current_7_days_start) & 
        (df["date"].dt.date <= current_7_days_end)
    ].copy()
    
    # Filter for previous 7 days (for comparison stats)
    df_previous = df[
        (df["date"].dt.date >= prev_7_days_start) & 
        (df["date"].dt.date <= prev_7_days_end)
    ].copy()
    
    return df_14_days, df_current, df_previous, last_14_days_start, last_14_days_end

def calculate_comparison_stats(current_df, previous_df, akropolis_brands):
    """Calculate comparison statistics between current and previous week"""
    # Current week stats for Akropolis brands
    current_akropolis = current_df[current_df["brand"].isin(akropolis_brands)]
    current_posts = current_akropolis["post_id"].nunique()
    current_engagement = current_akropolis["total_engagement"].sum()
    current_likes = current_akropolis["likes"].sum()
    current_comments = current_akropolis["comments"].sum()
    current_shares = current_akropolis["shares"].sum()
    
    # Previous week stats for Akropolis brands
    previous_akropolis = previous_df[previous_df["brand"].isin(akropolis_brands)]
    previous_posts = previous_akropolis["post_id"].nunique()
    previous_engagement = previous_akropolis["total_engagement"].sum()
    previous_likes = previous_akropolis["likes"].sum()
    previous_comments = previous_akropolis["comments"].sum()
    previous_shares = previous_akropolis["shares"].sum()
    
    # Calculate percentage changes
    posts_change = ((current_posts - previous_posts) / previous_posts * 100) if previous_posts > 0 else 0
    engagement_change = ((current_engagement - previous_engagement) / previous_engagement * 100) if previous_engagement > 0 else 0
    likes_change = ((current_likes - previous_likes) / previous_likes * 100) if previous_likes > 0 else 0
    comments_change = ((current_comments - previous_comments) / previous_comments * 100) if previous_comments > 0 else 0
    shares_change = ((current_shares - previous_shares) / previous_shares * 100) if previous_shares > 0 else 0
    
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
        "shares_change": shares_change
    }

def get_color_for_change(change):
    """Get color based on percentage change"""
    if change > 0:
        return "green"
    elif change < 0:
        return "red"
    else:
        return "black"

def create_post_card(brand, engagement, content, post_id, platform, source_url=None):
    """Create a card-style display for a social media post with clickable link"""
    truncated_content = content[:100] + "..." if len(content) > 100 else content
    
    # Create clickable link if source_url is available
    link_html = ""
    if source_url and pd.notna(source_url) and source_url != "":
        link_html = f'<p style="margin: 5px 0;"><a href="{source_url}" target="_blank" style="color: #007bff; text-decoration: none; font-size: 12px;">🔗 View Original Post</a></p>'
    
    return f"""
    <div style="border: 1px solid #ddd; border-radius: 8px; padding: 15px; margin: 10px 0; background-color: #f9f9f9;">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div>
                <h4 style="margin: 0; color: #333;">{brand} ({platform.upper()})</h4>
                <p style="margin: 5px 0; color: #666; font-size: 14px;">{truncated_content}</p>
                {link_html}
            </div>
            <div style="text-align: right;">
                <h3 style="margin: 0; color: #2E8B57;">{int(engagement):,}</h3>
                <p style="margin: 0; color: #666; font-size: 12px;">engagement</p>
            </div>
        </div>
    </div>
    """

def create_cluster_card_with_examples(cluster_name, posts_count, total_engagement, examples):
    """Create a card-style display for a cluster with examples"""
    examples_html = ""
    if not examples.empty:
        examples_html = "<div style='margin-top: 10px; padding-top: 10px; border-top: 1px solid #eee;'>"
        examples_html += "<p style='margin: 0 0 5px 0; color: #888; font-size: 12px; font-weight: bold;'>Examples:</p>"
        
        for idx, row in examples.iterrows():
            summary = str(row["post_summary"])
            source_url = row["source_url"] if pd.notna(row["source_url"]) else None
            
            # Truncate long summaries
            truncated_summary = summary[:150] + "..." if len(summary) > 150 else summary
            
            # Create clickable link if source_url is available
            if source_url and source_url != "":
                examples_html += f'<p style="margin: 2px 0; color: #666; font-size: 12px; font-style: italic;">• {truncated_summary} <a href="{source_url}" target="_blank" style="color: #007bff; text-decoration: none; font-size: 11px; margin-left: 5px;">🔗 View Post</a></p>'
            else:
                examples_html += f'<p style="margin: 2px 0; color: #666; font-size: 12px; font-style: italic;">• {truncated_summary}</p>'
        
        examples_html += "</div>"
    
    return f"""
    <div style="border: 1px solid #ddd; border-radius: 8px; padding: 15px; margin: 10px 0; background-color: #f9f9f9;">
        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
            <div style="flex: 1;">
                <h4 style="margin: 0; color: #333;">{cluster_name}</h4>
                <p style="margin: 5px 0; color: #666; font-size: 14px;">{posts_count} posts</p>
                {examples_html}
            </div>
            <div style="text-align: right; margin-left: 15px;">
                <h3 style="margin: 0; color: #2E8B57;">{int(total_engagement):,}</h3>
                <p style="margin: 0; color: #666; font-size: 12px;">engagement</p>
            </div>
        </div>
    </div>
    """

# ---- Sidebar Period Selection ----
st.sidebar.header("📅 Analysis Period")

# Get available periods
available_periods = get_available_periods()

if available_periods:
    # Create period options for the selector
    period_options = [period['label'] for period in available_periods]
    
    # Add "Latest" option at the beginning
    period_options.insert(0, "Latest (Most Recent)")
    
    # Default to latest period
    default_index = 0
    
    selected_period_label = st.sidebar.selectbox(
        "Select 14-day analysis period:",
        options=period_options,
        index=default_index,
        help="Choose from available periods with generated summaries"
    )
    
    # Determine selected dates
    if selected_period_label == "Latest (Most Recent)":
        selected_period = available_periods[-1]  # Latest period
        selected_start_date = selected_period['start_date']
        selected_end_date = selected_period['end_date']
    else:
        # Find the selected period
        selected_period = next((p for p in available_periods if p['label'] == selected_period_label), None)
        if selected_period:
            selected_start_date = selected_period['start_date']
            selected_end_date = selected_period['end_date']
        else:
            # Fallback to latest
            selected_period = available_periods[-1]
            selected_start_date = selected_period['start_date']
            selected_end_date = selected_period['end_date']
    
    st.sidebar.caption(f"Selected: {selected_start_date.strftime('%B %d')} - {selected_end_date.strftime('%B %d, %Y')}")
else:
    st.sidebar.warning("No analysis periods available. Run the summary generator first.")
    selected_start_date = config.ANALYSIS_START_DATE
    selected_end_date = config.ANALYSIS_END_DATE

# Load data for selected period
df_14_days, df_current, df_previous, start_date, end_date = load_data(selected_start_date, selected_end_date)

# Load summaries if available
@st.cache_data(show_spinner=False)
def load_summaries(selected_start_date=None, selected_end_date=None):
    """Load weekly summaries from Excel file for the selected period"""
    try:
        summaries_df = pd.read_excel(config.SUMMARIES_XLSX)
        if summaries_df.empty:
            return None
        
        # If no specific dates provided, return the latest summary
        if not selected_start_date or not selected_end_date:
            return summaries_df.iloc[-1].to_dict()
        
        # Find the summary row that matches the selected period
        for i, row in summaries_df.iterrows():
            row_start = pd.to_datetime(row['start_date']).date()
            row_end = pd.to_datetime(row['end_date']).date()
            
            # Check if the dates match (allowing for small differences due to timezone/formatting)
            if (row_start == selected_start_date and row_end == selected_end_date):
                return row.to_dict()
        
        # If no exact match found, return None
        st.warning(f"No summary found for period {selected_start_date} to {selected_end_date}")
        return None
        
    except FileNotFoundError:
        return None
    except Exception as e:
        st.error(f"Error loading summaries: {e}")
        return None

summaries = load_summaries(selected_start_date, selected_end_date)

# ---- UI controls ----
st.title(f"Facebook Social Media Intelligence – {start_date.strftime('%B %d')}-{end_date.strftime('%d, %Y')} Analysis")

# Date range display
st.caption(f"Analysis period: {start_date.strftime('%B %d')} - {end_date.strftime('%B %d, %Y')}")

# Facebook-only data (no platform filtering needed)
df_14_days_filtered = df_14_days.copy()
df_current_filtered = df_current.copy()
df_previous_filtered = df_previous.copy()

st.markdown("**Select Akropolis locations (always included):**")
ak_cols = st.columns(4)
ak_selected = []
for i, loc in enumerate(AKROPOLIS_LOCATIONS):
    with ak_cols[i]:
        if st.checkbox(loc, value=True, key=f"ak_{i}"):
            ak_selected.append(loc)

# Company cluster selector - full width at the top
st.markdown("**Select company cluster to analyze:**")
subset_name = st.selectbox(
    "Subset of companies",
    options=list(SUBSETS_WITH_RETAIL.keys()),
    index=0,
    help="Charts include the selected Akropolis locations **plus** this subset.",
)

st.markdown("---")

# ---- Comparison Statistics with Tabs ----
st.subheader("📊 Performance vs Previous Week")

# Get all brands in the selected cluster
brands_universe = set(ak_selected) | set(SUBSETS_WITH_RETAIL.get(subset_name, []))
all_brands = sorted(list(brands_universe))

# Create tabs for each brand
if all_brands:
    performance_tabs = st.tabs(all_brands)
    
    for i, brand in enumerate(all_brands):
        with performance_tabs[i]:
            # Calculate stats for this specific brand using filtered data
            brand_current = df_current_filtered[df_current_filtered["brand"] == brand]
            brand_previous = df_previous_filtered[df_previous_filtered["brand"] == brand]
            
            current_posts = brand_current["post_id"].nunique()
            current_engagement = brand_current["total_engagement"].sum()
            current_likes = brand_current["likes"].sum()
            current_comments = brand_current["comments"].sum()
            current_shares = brand_current["shares"].sum()
            
            previous_posts = brand_previous["post_id"].nunique()
            previous_engagement = brand_previous["total_engagement"].sum()
            previous_likes = brand_previous["likes"].sum()
            previous_comments = brand_previous["comments"].sum()
            previous_shares = brand_previous["shares"].sum()
            
            # Calculate percentage changes - handle 0 previous week case
            if previous_posts > 0:
                posts_change = ((current_posts - previous_posts) / previous_posts * 100)
            elif current_posts > 0:
                posts_change = 100  # 100% increase from 0
            else:
                posts_change = 0  # Both are 0
            
            if previous_engagement > 0:
                engagement_change = ((current_engagement - previous_engagement) / previous_engagement * 100)
            elif current_engagement > 0:
                engagement_change = 100  # 100% increase from 0
            else:
                engagement_change = 0  # Both are 0
            
            if previous_likes > 0:
                likes_change = ((current_likes - previous_likes) / previous_likes * 100)
            elif current_likes > 0:
                likes_change = 100
            else:
                likes_change = 0
            
            if previous_comments > 0:
                comments_change = ((current_comments - previous_comments) / previous_comments * 100)
            elif current_comments > 0:
                comments_change = 100
            else:
                comments_change = 0
            
            if previous_shares > 0:
                shares_change = ((current_shares - previous_shares) / previous_shares * 100)
            elif current_shares > 0:
                shares_change = 100
            else:
                shares_change = 0
            
            # Create comparison cards for this brand (simplified)
            col1, col2 = st.columns(2)
            
            with col1:
                posts_color = get_color_for_change(posts_change)
                st.markdown(f"""
                <div style="border: 2px solid black; padding: 20px; border-radius: 10px; margin: 10px 0;">
                    <h3 style="color: {posts_color}; margin: 0;">📝 Total Posts: {current_posts}</h3>
                    <p style="color: {posts_color}; font-size: 18px; margin: 5px 0;">
                        {posts_change:+.1f}% vs previous week
                    </p>
                    <p style="color: gray; font-size: 14px; margin: 0;">Previous week: {previous_posts} posts</p>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                engagement_color = get_color_for_change(engagement_change)
                st.markdown(f"""
                <div style="border: 2px solid black; padding: 20px; border-radius: 10px; margin: 10px 0;">
                    <h3 style="color: {engagement_color}; margin: 0;">💬 Total Engagement: {int(current_engagement):,}</h3>
                    <p style="color: {engagement_color}; font-size: 18px; margin: 5px 0;">
                        {engagement_change:+.1f}% vs previous week
                    </p>
                    <p style="color: gray; font-size: 14px; margin: 0;">Previous week: {int(previous_engagement):,} engagement</p>
                </div>
                """, unsafe_allow_html=True)
            
            # Show info if no data available
            if current_posts == 0 and previous_posts == 0:
                st.info(f"No data available for {brand} in the selected time periods.")

st.markdown("---")

# ---- Weekly Summaries ----
if summaries:
    st.subheader(f"📝 Weekly AI Summaries ({config.ANALYSIS_START_DATE.strftime('%B %d')}-{config.ANALYSIS_END_DATE.strftime('%d, %Y')})")
    
    # Get all available summaries (including Akropolis)
    all_summaries = {k: v for k, v in summaries.items() if k not in ["start_date", "end_date"] and v}
    
    if all_summaries:
        # Filter summaries based on selected cluster, but always include Akropolis
        relevant_summaries = {}
        
        # Always include Akropolis if available
        if "Akropolis" in all_summaries:
            relevant_summaries["Akropolis"] = all_summaries["Akropolis"]
        
        # Add competitors from selected cluster
        for brand, summary in all_summaries.items():
            if brand != "Akropolis" and brand in brands_universe:
                relevant_summaries[brand] = summary
        
        if relevant_summaries:
            # Create tabs with Akropolis at the end
            tab_names = [brand for brand in relevant_summaries.keys() if brand != "Akropolis"]
            if "Akropolis" in relevant_summaries:
                tab_names.append("Akropolis")
            
            # Add summary count info
            st.caption(f"AI-generated summaries for {len(relevant_summaries)} brands")
            
            summary_tabs = st.tabs(tab_names)
            
            for i, brand in enumerate(tab_names):
                with summary_tabs[i]:
                    # Add brand-specific styling for Akropolis
                    if brand == "Akropolis":
                        border_color = "#2E8B57"  # Green for Akropolis
                        bg_color = "#f0fff0"      # Light green background
                    else:
                        border_color = "#333"     # Black for competitors
                        bg_color = "#f8f9fa"      # Light gray background
                    
                    # Convert line breaks to HTML paragraphs for proper formatting
                    # Handle case where summary might be NaN or not a string
                    summary_content = relevant_summaries[brand]
                    if pd.isna(summary_content) or not isinstance(summary_content, str):
                        summary_text = "Summary not available for this period."
                    else:
                        summary_text = summary_content.replace('\n\n', '</p><p style="margin: 10px 0; line-height: 1.6; color: #333; font-size: 16px;">')
                    
                    st.markdown(f"""
                    <div style="border: 2px solid {border_color}; padding: 20px; border-radius: 10px; margin: 10px 0; background-color: {bg_color};">
                        <h4 style="margin: 0 0 15px 0; color: {border_color}; font-size: 18px;">{brand} - Weekly Summary</h4>
                        <p style="margin: 10px 0; line-height: 1.6; color: #333; font-size: 16px;">{summary_text}</p>
                    </div>
                    """, unsafe_allow_html=True)
        else:
            st.info("No summaries available for the selected cluster.")
    else:
        st.info("No summaries available.")
else:
    st.info("Weekly summaries not available. Run the pipeline to generate summaries.")

st.markdown("---")

# ---- Main Analysis Section ----
# Filter to chosen brands using filtered data
brands_universe = set(ak_selected) | set(SUBSETS_WITH_RETAIL.get(subset_name, []))
df_f = df_14_days_filtered[df_14_days_filtered["brand"].isin(brands_universe)].copy()
df_f_current = df_current_filtered[df_current_filtered["brand"].isin(brands_universe)].copy()

st.subheader(f"Facebook Social Media Intelligence ({config.ANALYSIS_START_DATE.strftime('%B %d')}-{config.ANALYSIS_END_DATE.strftime('%d, %Y')})")

# Show platform information
st.caption(
    f"{df_f['brand'].nunique()} brands · {df_f['post_id'].nunique()} posts · {int(df_f['total_engagement'].sum()):,} total engagement · Facebook"
)

# ---- 1) Daily chart with tabs ----
st.markdown("#### Daily Performance")

# Chart type selector
chart_type = st.radio(
    "Chart type:",
    ["Bar Chart", "Line Chart"],
    horizontal=True,
    key="chart_type_selector"
)

# Create tabs for posts count vs engagement
tab1, tab2 = st.tabs(["📝 Posts Posted per Day", "💬 Engagement per Day"])

with tab1:
    # Group by date (not datetime) to get daily totals
    df_f_copy = df_f.copy()
    df_f_copy["date_only"] = df_f_copy["date"].dt.date
    daily_posts = (
        df_f_copy.groupby(["date_only", "brand", "platform"], as_index=False)
        .agg(posts_count=("post_id", "nunique"))
        .sort_values("date_only")
    )
    daily_posts["date"] = pd.to_datetime(daily_posts["date_only"])
    
    if daily_posts.empty:
        st.info("No posts found for these brands in the last 14 days.")
    else:
        # Choose chart type based on selection
        if chart_type == "Bar Chart":
            chart = (
                alt.Chart(daily_posts)
                .mark_bar()
                .encode(
                    x=alt.X("date:T", title="Day", axis=alt.Axis(format="%m/%d")),
                    y=alt.Y("posts_count:Q", title="Posts posted"),
                    color=alt.Color("brand:N", title="Brand"),
                    tooltip=["date", "brand", "platform", "posts_count"],
                )
                .properties(height=360)
            )
        else:  # Line Chart
            chart = (
                alt.Chart(daily_posts)
                .mark_line(point=True)
                .encode(
                    x=alt.X("date:T", title="Day", axis=alt.Axis(format="%m/%d")),
                    y=alt.Y("posts_count:Q", title="Posts posted"),
                    color=alt.Color("brand:N", title="Brand"),
                    tooltip=["date", "brand", "platform", "posts_count"],
                )
                .properties(height=360)
            )
        st.altair_chart(chart, use_container_width=True)

with tab2:
    # Group by date (not datetime) to get daily totals
    df_f_copy2 = df_f.copy()
    df_f_copy2["date_only"] = df_f_copy2["date"].dt.date
    daily_engagement = (
        df_f_copy2.groupby(["date_only", "brand", "platform"], as_index=False)
        .agg(total_engagement=("total_engagement", "sum"))
        .sort_values("date_only")
    )
    daily_engagement["date"] = pd.to_datetime(daily_engagement["date_only"])
    
    if daily_engagement.empty:
        st.info("No engagement data found for these brands in the last 14 days.")
    else:
        # Choose chart type based on selection
        if chart_type == "Bar Chart":
            chart = (
                alt.Chart(daily_engagement)
                .mark_bar()
                .encode(
                    x=alt.X("date:T", title="Day", axis=alt.Axis(format="%m/%d")),
                    y=alt.Y("total_engagement:Q", title="Total engagement"),
                    color=alt.Color("brand:N", title="Brand"),
                    tooltip=["date", "brand", "platform", "total_engagement"],
                )
                .properties(height=360)
            )
        else:  # Line Chart
            chart = (
                alt.Chart(daily_engagement)
                .mark_line(point=True)
                .encode(
                    x=alt.X("date:T", title="Day", axis=alt.Axis(format="%m/%d")),
                    y=alt.Y("total_engagement:Q", title="Total engagement"),
                    color=alt.Color("brand:N", title="Brand"),
                    tooltip=["date", "brand", "platform", "total_engagement"],
                )
                .properties(height=360)
            )
        st.altair_chart(chart, use_container_width=True)

# ---- 2) Top 3 posts by engagement ----
st.markdown("#### Top 3 Posts by Engagement")
post_rollup = (
    df_f.groupby(["post_id", "brand", "platform"], as_index=False)
    .agg(engagement=("total_engagement", "max"), content=("content", "first"), source_url=("source_url", "first"))
)

if post_rollup.empty:
    st.info("No posts to show.")
else:
    brands_in_view = sorted(post_rollup["brand"].unique())
    tabs = st.tabs(["Overall"] + brands_in_view)
    
    def top3(d):
        return d.sort_values("engagement", ascending=False).head(3).reset_index(drop=True)
    
    with tabs[0]:
        top3_overall = top3(post_rollup)
        for idx, row in top3_overall.iterrows():
            st.markdown(create_post_card(row["brand"], row["engagement"], row["content"], row["post_id"], row["platform"], row["source_url"]), unsafe_allow_html=True)
    
    for i, b in enumerate(brands_in_view, start=1):
        with tabs[i]:
            top3_brand = top3(post_rollup[post_rollup["brand"] == b])
            for idx, row in top3_brand.iterrows():
                st.markdown(create_post_card(row["brand"], row["engagement"], row["content"], row["post_id"], row["platform"], row["source_url"]), unsafe_allow_html=True)

# ---- 3) Top 3 clusters by engagement ----
st.markdown("#### Top 3 Clusters by Engagement")

# Filter for posts with cluster_1 data
df_with_clusters = df_f[df_f["cluster_1"].notna() & (df_f["cluster_1"] != "")]

if df_with_clusters.empty:
    st.info("No cluster data available.")
else:
    # Group by cluster and brand for brand-specific tabs
    cluster_rollup = (
        df_with_clusters.groupby(["cluster_1", "brand"], as_index=False)
        .agg(
            posts_count=("post_id", "nunique"),
            total_engagement=("total_engagement", "sum")
        )
    )
    
    # Group by cluster only for overall tab to avoid duplicate clusters
    cluster_rollup_overall = (
        df_with_clusters.groupby("cluster_1", as_index=False)
        .agg(
            posts_count=("post_id", "nunique"),
            total_engagement=("total_engagement", "sum")
        )
    )
    
    brands_with_clusters = sorted(cluster_rollup["brand"].unique())
    cluster_tabs = st.tabs(["Overall"] + brands_with_clusters)
    
    def top3_clusters(d):
        return d.sort_values("total_engagement", ascending=False).head(3).reset_index(drop=True)
    
    with cluster_tabs[0]:
        top3_clusters_overall = top3_clusters(cluster_rollup_overall)
        for idx, row in top3_clusters_overall.iterrows():
            # Get examples for this cluster (using post_summary and source_url)
            examples = (
                df_with_clusters[df_with_clusters["cluster_1"] == row["cluster_1"]]
                [["post_summary", "source_url"]]
                .dropna(subset=["post_summary"])
                .head(2)
            )
            st.markdown(create_cluster_card_with_examples(row["cluster_1"], row["posts_count"], row["total_engagement"], examples), unsafe_allow_html=True)
    
    for i, b in enumerate(brands_with_clusters, start=1):
        with cluster_tabs[i]:
            top3_clusters_brand = top3_clusters(cluster_rollup[cluster_rollup["brand"] == b])
            for idx, row in top3_clusters_brand.iterrows():
                # Get examples for this cluster and brand (using post_summary and source_url)
                examples = (
                    df_with_clusters[(df_with_clusters["cluster_1"] == row["cluster_1"]) & (df_with_clusters["brand"] == b)]
                    [["post_summary", "source_url"]]
                    .dropna(subset=["post_summary"])
                    .head(2)
                )
                st.markdown(create_cluster_card_with_examples(row["cluster_1"], row["posts_count"], row["total_engagement"], examples), unsafe_allow_html=True)

# ---- Optional totals by brand ----
with st.expander("Totals by brand"):
    totals = (
        df_f.groupby("brand", as_index=False)
        .agg(
            posts=("post_id", "nunique"), 
            total_engagement=("total_engagement", "sum"),
            avg_engagement=("total_engagement", "mean"),
            likes=("likes", "sum"),
            comments=("comments", "sum"),
            shares=("shares", "sum")
        )
        .sort_values("total_engagement", ascending=False)
    )
    st.dataframe(totals, use_container_width=True)