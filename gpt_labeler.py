#!/usr/bin/env python3
"""
GPT Labeler for Social Media Posts
Generates summaries and categorizes social media content using OpenAI GPT
"""

import os
import re
import json
import hashlib
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Tuple
import pandas as pd
from tqdm import tqdm
from openai import OpenAI
import config

# ----------------------------
# CONFIG
# ----------------------------
MODEL = "gpt-4o-mini"
TEMPERATURE = 0
MAX_WORKERS = 10  # Conservative for rate limits
MAX_CHARS_PER_POST = 1400

# Column names for social media data
COL_CONTENT = "content"
COL_BRAND = "brand"
COL_SUMMARY = "post_summary"
COL_CLUSTER_1 = "cluster_1"  # Most appropriate
COL_CLUSTER_2 = "cluster_2"  # Second most appropriate
COL_CLUSTER_3 = "cluster_3"  # Third most appropriate

# ----------------------------
# Helpers
# ----------------------------
def normalize_text(s):
    """Normalize text by cleaning whitespace and line breaks."""
    if not isinstance(s, str):
        return ""
    s = s.replace("\r", "\n")
    s = re.sub(r"\n+", "\n", s)
    s = re.sub(r"[ \t]+", " ", s)
    return s.strip()

def compact_text(s, limit=MAX_CHARS_PER_POST):
    """Truncate text to limit while preserving readability."""
    s = normalize_text(s)
    return s if len(s) <= limit else (s[:limit] + "…")

def hash_text(s: str) -> str:
    """Create hash for text deduplication."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def parse_label_line(text):
    """Parse the label line format: 'Labels: <Theme A>; <Theme B>; <Theme C>'"""
    if not isinstance(text, str):
        return [None, None, None]
    m = re.search(r"Labels\s*:\s*(.+)$", text.strip(), flags=re.I)
    if not m:
        return [None, None, None]
    parts = [p.strip() for p in m.group(1).split(";") if p.strip()]
    parts = parts[:3]
    while len(parts) < 3:
        parts.append(None)
    return parts

# ----------------------------
# GPT Prompts for Social Media
# ----------------------------
SUMMARY_SYSTEM_PROMPT = (
    "You are a precise annotator of social media posts.\n"
    "Given a social media post's content, return a ONE-SENTENCE description of the main message, promotion, or announcement.\n"
    "Rules:\n"
    "- If a clear single product/service/promotion/event/announcement is identifiable, describe it succinctly in one sentence.\n"
    "- If the post is only brand building, company news, or general content with no concrete offer, still summarize the post in one sentence.\n"
    "- Keep it factual (no hype), <= 140 characters where feasible, no emojis, no hashtags, no URLs.\n"
    "- Treat promotions/discounts/events/contests as valid 'products' (e.g., '50% off weekend sale at Maxima').\n"
    "- ALWAYS return everything in English, even if the post is in another language!\n"
    'Return STRICT JSON ONLY as: {"summary":"<ONE_SENTENCE_OR_NONE>"}'
)


CLUSTER_SYSTEM_PROMPT = (
    "You are labeling a social media post against a FIXED taxonomy.\n"
    "Rules:\n"
    "- Choose 1 to 3 labels from ALLOWED THEMES (listed below with examples).\n"
    "- The FIRST label must be the single MOST APPROPRIATE cluster.\n"
    "- If no cluster fits, output OTHER.\n"
    "- VERY IMPORTANT: do NOT force-fit; keep OTHER if uncertain.\n"
    "- Output ENGLISH only in EXACTLY this format:\n"
    "Labels: <Theme A>; <Theme B>; <Theme C>\n"
    "(Use 1–3 labels; separate with semicolons; do not number them.)\n"
    "- Prefer the most specific matching themes.\n\n"
    "Output requirement:\n"
    "- Each cluster name is followed by a dash and examples. RETURN ONLY the text before the dash (the cluster name itself), not the examples.\n"
    "  Example: If you pick 'Seasonal Promotions and Discounts — Christmas sale, Black Friday offers', output just 'Seasonal Promotions and Discounts'.\n\n"
    "Key distinctions:\n"
    "- Seasonal Promotions and Discounts = time-bound events tied to a season, holiday, or calendar moment (e.g. Christmas sale, Black Friday, back-to-school, summer clearance, flower promos for a holiday).\n"
    "- General Discounts and Promotions = price cuts or deals not tied to a season or holiday (e.g. permanent weekly sale, everyday low prices).\n"
    "Available clusters (with illustrative examples—DO NOT RETURN the examples, just the theme name before the dash!):\n"
    "1. Store Openings and Tenant Updates — new store opening, major renovation, new tenant announcement.\n"
    "2. Seasonal Promotions and Discounts — Christmas sale, Black Friday offers, Easter weekend deals, summer clearance, back-to-school campaigns.\n"
    "3. Competitions and Giveaways — social media raffle, prize draw, scholarship contest.\n"
    "4. Events and Experiences — live concerts, family festivals, community fairs, interactive installations.\n"
    "5. Fashion and Style Highlights — clothing trends, styling tips, seasonal wardrobe ideas.\n"
    "6. Food and Dining Specials — restaurant or café openings, tasting events, featured recipes, bakery showcases.\n"
    "7. Beauty and Personal Care — hair salon promotions, skincare demos, cosmetic discounts.\n"
    "8. Digital or App-Exclusive Offers — mobile-app coupons, e-shop exclusives, online order perks.\n"
    "9. Holiday and Celebration Greetings — holiday wishes, themed decorations, festive atmosphere posts.\n"
    "10. Shopping Experience and Atmosphere — free parking, stroller rental, pet-friendly policy, upgraded family rooms, mall gift cards.\n"
    "11. Travel and Leisure Essentials — luggage sales, vacation prep, travel accessories.\n"
    "12. Sustainability and Eco-Actions — recycling initiatives, zero-waste fairs, green programs.\n"
    "13. Services and Repairs — tailoring, electronics service desks, key-cutting, watch repair.\n"
    "14. Health and Social Responsibility — free health checks, blood drives, charitable or community aid, inclusive social projects.\n"
    "15. Books, Learning and Educational Products — book fairs, stationery launches, coding kits.\n"
    "16. Home & Living / Fabric Care Tips — furniture and décor inspiration, fabric-care guidance, interior refresh ideas.\n"
    "17. Gifting and Accessories — gift ideas, jewelry highlights, special flower or accessory offers.\n"
    "18. Financial and Business Performance — earnings results, credit ratings, investment plans, large-scale capital projects.\n"
    "19. Leadership Appointments and HR News — executive hires, leadership promotions, organizational restructuring.\n"
    "20. Employee Development and Workplace Culture — career growth, internal mobility, employee stories, well-being programs.\n"
    "21. Awards and Industry Recognition — industry prizes, rankings, certifications, external recognition.\n"
    "22. Supply Chain and Product Quality — supplier policies, quality-control measures, sourcing standards, logistics achievements.\n"
    "23. Corporate Partnerships and Sponsorships — strategic alliances, co-branded campaigns, sports or cultural sponsorships.\n"
    "24. Thought Leadership and Expert Commentary — market insights, opinion pieces, expert interviews, future-of-industry perspectives.\n"
)



def build_summary_prompt(post_content: str) -> str:
    """Build user prompt for summary generation."""
    return f"Social media post content:\n{post_content}"

def build_cluster_prompt(post_content: str) -> str:
    """Build user prompt for cluster categorization."""
    return f"Social media post:\n{post_content}\n\nChoose 1–3 from ALLOWED THEMES."

# ----------------------------
# Model calls
# ----------------------------
def get_openai_client() -> OpenAI:
    """Get OpenAI client with API key from environment or config."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        # Try to import from config if available
        try:
            api_key = getattr(config, 'OPENAI_API_KEY', None)
        except ImportError:
            pass
    
    if not api_key:
        raise ValueError("Set OPENAI_API_KEY environment variable or add it to config.py")
    
    return OpenAI(api_key=api_key)

def generate_summary(post_content: str) -> str:
    """Generate a one-sentence summary for a social media post."""
    try:
        client = get_openai_client()
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": SUMMARY_SYSTEM_PROMPT},
                {"role": "user", "content": build_summary_prompt(post_content)},
            ],
            temperature=TEMPERATURE,
            response_format={"type": "json_object"},
            max_tokens=200,
        )
        raw = (resp.choices[0].message.content or "").strip()
        # Parse JSON strictly or by bracket slice
        data = json.loads(raw) if raw.startswith("{") else json.loads(raw[raw.find("{"):raw.rfind("}")+1])
        summary = (data.get("summary") or "").strip()
        if not summary or summary.upper() == "NULL":
            return "NONE"
        summary = re.sub(r'https?://\S+', '', summary)
        summary = normalize_text(summary)
        if len(summary) > 160:
            summary = summary[:160].rstrip(" ,.;:") + "."
        return summary
    except Exception as e:
        print(f"[ERROR] Summary generation failed: {e}")
        return "NONE"

def generate_clusters(post_content: str) -> Tuple[str, str, str]:
    """Generate cluster categories for a social media post."""
    try:
        client = get_openai_client()
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": CLUSTER_SYSTEM_PROMPT},
                {"role": "user", "content": build_cluster_prompt(post_content)},
            ],
            temperature=TEMPERATURE,
            max_tokens=80,
        )
        raw = (resp.choices[0].message.content or "").strip()
        # Parse the label line format
        clusters = parse_label_line(raw)
        return clusters[0], clusters[1], clusters[2]
    except Exception as e:
        print(f"[ERROR] Cluster generation failed: {e}")
        return "NONE", None, None

def process_post_with_gpt(post_content: str) -> Tuple[str, str, str, str]:
    """Process a single social media post to generate summary and three ranked clusters."""
    summary = generate_summary(post_content)
    cluster1, cluster2, cluster3 = generate_clusters(post_content)
    return summary, cluster1, cluster2, cluster3

# ----------------------------
# Main processing functions
# ----------------------------
def label_posts_with_gpt(df: pd.DataFrame, max_workers: int = MAX_WORKERS) -> pd.DataFrame:
    """
    Add GPT-generated summaries and clusters to a DataFrame of social media posts.
    
    Args:
        df: DataFrame with social media post data
        max_workers: Number of parallel workers for GPT API calls
    
    Returns:
        DataFrame with added summary and cluster columns
    """
    if COL_CONTENT not in df.columns:
        print(f"[WARNING] Column {COL_CONTENT} not found, skipping GPT labeling")
        return df
    
    # Clean and prepare data
    df = df.copy()
    df[COL_CONTENT] = df[COL_CONTENT].map(lambda x: compact_text(x if isinstance(x, str) else ""))
    
    # Remove empty texts
    df = df[df[COL_CONTENT].str.len() > 0].reset_index(drop=True)
    
    if df.empty:
        print("[WARNING] No valid post content found for GPT labeling")
        return df
    
    # Deduplicate based on normalized text
    df["__norm__"] = df[COL_CONTENT].map(lambda s: normalize_text(s).lower())
    df = df.drop_duplicates(subset=["__norm__"], keep="first").reset_index(drop=True)
    
    contents = df[COL_CONTENT].tolist()
    print(f"[INFO] Processing {len(contents)} unique posts with GPT...")
    
    # Initialize result columns
    summaries = [None] * len(contents)
    cluster1_list = [None] * len(contents)
    cluster2_list = [None] * len(contents)
    cluster3_list = [None] * len(contents)
    
    # Parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_post_with_gpt, content): i for i, content in enumerate(contents)}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="GPT Labeling"):
            i = futures[future]
            try:
                summary, cluster1, cluster2, cluster3 = future.result()
                summaries[i] = summary
                cluster1_list[i] = cluster1
                cluster2_list[i] = cluster2
                cluster3_list[i] = cluster3
            except Exception as e:
                print(f"[ERROR] Failed to process post {i}: {e}")
                summaries[i] = "NONE"
                cluster1_list[i] = "NONE"
                cluster2_list[i] = None
                cluster3_list[i] = None
    
    # Add results to DataFrame
    df[COL_SUMMARY] = summaries
    df[COL_CLUSTER_1] = cluster1_list
    df[COL_CLUSTER_2] = cluster2_list
    df[COL_CLUSTER_3] = cluster3_list
    
    # Clean up temporary column
    df = df.drop(columns=["__norm__"])
    
    print(f"[DONE] GPT labeling completed for {len(df)} posts")
    return df

def get_cluster_stats(df: pd.DataFrame) -> Dict[str, int]:
    """Get statistics on cluster distribution."""
    if COL_CLUSTER_1 not in df.columns:
        return {}
    
    cluster_counts = {}
    
    # Count all clusters from all three columns
    for col in [COL_CLUSTER_1, COL_CLUSTER_2, COL_CLUSTER_3]:
        if col in df.columns:
            for cluster in df[col]:
                if cluster and cluster != "NONE" and pd.notna(cluster):
                    cluster_counts[cluster] = cluster_counts.get(cluster, 0) + 1
    
    return dict(sorted(cluster_counts.items(), key=lambda x: x[1], reverse=True))

def print_cluster_stats(df: pd.DataFrame):
    """Print cluster statistics."""
    stats = get_cluster_stats(df)
    if not stats:
        print("[INFO] No cluster statistics available")
        return
    
    print("\n[CLUSTER STATISTICS]")
    print("-" * 50)
    for cluster, count in stats.items():
        print(f"{cluster}: {count}")
    print("-" * 50)

def get_brand_performance_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Get performance statistics by brand."""
    if df.empty:
        return pd.DataFrame()
    
    # Group by brand and calculate metrics
    brand_stats = df.groupby('brand').agg({
        'post_id': 'count',
        'likes': 'sum',
        'comments': 'sum', 
        'shares': 'sum',
        'total_engagement': 'sum',
        'engagement_rate': 'mean'
    }).round(2)
    
    brand_stats.columns = ['total_posts', 'total_likes', 'total_comments', 'total_shares', 'total_engagement', 'avg_engagement_rate']
    brand_stats = brand_stats.sort_values('total_engagement', ascending=False)
    
    return brand_stats

def get_top_posts_by_engagement(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Get top posts by total engagement."""
    if df.empty or 'total_engagement' not in df.columns:
        return pd.DataFrame()
    
    return df.nlargest(n, 'total_engagement')[['brand', 'content', 'total_engagement', 'likes', 'comments', 'shares', 'cluster_1']].copy()

if __name__ == "__main__":
    # Test the GPT labeling functions
    print("🧪 Testing social media GPT labeling...")
    
    # Create sample data
    sample_posts = [
        {
            'brand': 'AKROPOLIS | Vilnius',
            'content': '🎉 Black Friday is here! Get up to 70% off on selected items. Visit us this weekend for amazing deals!',
            'likes': 150,
            'comments': 25,
            'shares': 10
        },
        {
            'brand': 'PANORAMA',
            'content': 'We are proud to announce our new sustainability initiative. Together we can make a difference for our planet.',
            'likes': 75,
            'comments': 15,
            'shares': 5
        }
    ]
    
    df = pd.DataFrame(sample_posts)
    df['total_engagement'] = df['likes'] + df['comments'] + df['shares']
    
    # Test GPT labeling
    if config.ENABLE_GPT_LABELING:
        try:
            labeled_df = label_posts_with_gpt(df)
            print("✅ Test successful!")
            print("\nLabeled data:")
            print(labeled_df[['brand', 'content', 'post_summary', 'cluster_1']].head())
            print_cluster_stats(labeled_df)
        except Exception as e:
            print(f"❌ Test failed: {e}")
    else:
        print("ℹ️ GPT labeling is disabled in config")