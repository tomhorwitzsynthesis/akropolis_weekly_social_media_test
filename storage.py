#!/usr/bin/env python3
"""
Storage functions for social media data
Handles loading, saving, and deduplication of social media posts
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import config

def load_excel(path: Path) -> pd.DataFrame:
    """
    Load data from Excel file, returning empty DataFrame if file doesn't exist.
    
    Args:
        path: Path to Excel file
    
    Returns:
        DataFrame with loaded data or empty DataFrame if file doesn't exist
    """
    if path.exists():
        try:
            df = pd.read_excel(path, dtype=str)
            print(f"📂 Loaded {len(df)} posts from {path}")
            return df
        except Exception as e:
            print(f"❌ Error loading {path}: {e}")
            return pd.DataFrame()
    else:
        print(f"📂 File {path} does not exist, starting with empty dataset")
        return pd.DataFrame()

def save_excel(df: pd.DataFrame, path: Path) -> None:
    """
    Save DataFrame to Excel file with proper formatting.
    
    Args:
        df: DataFrame to save
        path: Path where to save the Excel file
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(path, index=False, engine='openpyxl')
        print(f"💾 Saved {len(df)} posts to {path}")
    except Exception as e:
        print(f"❌ Error saving to {path}: {e}")
        raise

def deduplicate_posts(df: pd.DataFrame, keys: list[str] = None) -> pd.DataFrame:
    """
    Remove duplicate social media posts based on specified keys.
    
    Args:
        df: DataFrame with social media posts
        keys: List of column names to use for deduplication (uses config.DEDUP_KEYS if None)
    
    Returns:
        DataFrame with duplicates removed
    """
    if keys is None:
        keys = config.DEDUP_KEYS
    
    if df.empty:
        return df
    
    df = df.copy()
    original_count = len(df)
    
    # Ensure all deduplication keys exist and are string type
    for key in keys:
        if key not in df.columns:
            print(f"⚠️ Warning: Deduplication key '{key}' not found in data, adding as empty column")
            df[key] = pd.NA
        df[key] = df[key].astype("string")
    
    # Remove duplicates
    df_deduped = df.drop_duplicates(subset=keys, keep="first")
    
    removed_count = original_count - len(df_deduped)
    if removed_count > 0:
        print(f"🔄 Removed {removed_count} duplicate posts (kept {len(df_deduped)} unique posts)")
    else:
        print(f"✅ No duplicates found ({len(df_deduped)} unique posts)")
    
    return df_deduped

def merge_with_existing_data(new_posts: pd.DataFrame, existing_path: Path) -> pd.DataFrame:
    """
    Merge new posts with existing data and deduplicate.
    
    Args:
        new_posts: DataFrame with new posts
        existing_path: Path to existing Excel file
    
    Returns:
        Combined DataFrame with duplicates removed
    """
    if new_posts.empty:
        print("⚠️ No new posts to merge")
        return load_excel(existing_path)
    
    # Load existing data
    existing_posts = load_excel(existing_path)
    
    if existing_posts.empty:
        print("📝 No existing data found, using new posts only")
        return new_posts
    
    # Combine data
    print(f"🔄 Merging {len(new_posts)} new posts with {len(existing_posts)} existing posts")
    combined = pd.concat([existing_posts, new_posts], ignore_index=True, sort=False)
    
    # Deduplicate
    combined_deduped = deduplicate_posts(combined)
    
    return combined_deduped

def backup_existing_data(path: Path) -> None:
    """
    Create a backup of existing data before overwriting.
    
    Args:
        path: Path to the data file to backup
    """
    if not path.exists():
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = path.parent / "backups" /f"{path.stem}_backup_{timestamp}{path.suffix}"
    
    try:
        import shutil
        shutil.copy2(path, backup_path)
        print(f"💾 Created backup: {backup_path}")
    except Exception as e:
        print(f"⚠️ Failed to create backup: {e}")

def get_data_summary(df: pd.DataFrame) -> dict:
    """
    Get summary statistics of the social media data.
    
    Args:
        df: DataFrame with social media posts
    
    Returns:
        Dictionary with summary statistics
    """
    if df.empty:
        return {
            "total_posts": 0,
            "platforms": {},
            "brands": {},
            "date_range": None,
            "engagement_metrics": {}
        }
    
    summary = {
        "total_posts": len(df),
        "platforms": df['platform'].value_counts().to_dict() if 'platform' in df.columns else {},
        "brands": df['brand'].value_counts().to_dict() if 'brand' in df.columns else {},
    }
    
    # Date range
    if 'created_date' in df.columns:
        try:
            dates = pd.to_datetime(df['created_date'], errors='coerce')
            valid_dates = dates.dropna()
            if not valid_dates.empty:
                summary["date_range"] = {
                    "earliest": valid_dates.min().date().isoformat(),
                    "latest": valid_dates.max().date().isoformat()
                }
        except Exception:
            summary["date_range"] = None
    
    # Engagement metrics
    engagement_cols = ['likes', 'comments', 'shares', 'total_engagement']
    summary["engagement_metrics"] = {}
    for col in engagement_cols:
        if col in df.columns:
            try:
                numeric_values = pd.to_numeric(df[col], errors='coerce').fillna(0)
                summary["engagement_metrics"][col] = {
                    "total": int(numeric_values.sum()),
                    "average": float(numeric_values.mean()),
                    "median": float(numeric_values.median())
                }
            except Exception:
                summary["engagement_metrics"][col] = {"total": 0, "average": 0, "median": 0}
    
    return summary

def print_data_summary(df: pd.DataFrame) -> None:
    """
    Print a summary of the social media data.
    
    Args:
        df: DataFrame with social media posts
    """
    summary = get_data_summary(df)
    
    print("\n📊 DATA SUMMARY")
    print("=" * 50)
    print(f"Total posts: {summary['total_posts']}")
    
    if summary['platforms']:
        print("\nPlatforms:")
        for platform, count in summary['platforms'].items():
            print(f"  {platform}: {count}")
    
    if summary['brands']:
        print(f"\nTop 10 brands:")
        for brand, count in list(summary['brands'].items())[:10]:
            print(f"  {brand}: {count}")
    
    if summary['date_range']:
        print(f"\nDate range: {summary['date_range']['earliest']} to {summary['date_range']['latest']}")
    
    if summary['engagement_metrics']:
        print("\nEngagement metrics:")
        for metric, stats in summary['engagement_metrics'].items():
            print(f"  {metric}: {stats['total']:,} total, {stats['average']:.1f} avg")
    
    print("=" * 50)

def save_with_backup(df: pd.DataFrame, path: Path) -> None:
    """
    Save DataFrame with automatic backup of existing data.
    
    Args:
        df: DataFrame to save
        path: Path where to save the data
    """
    # Create backup if file exists
    backup_existing_data(path)
    
    # Save new data
    save_excel(df, path)
    
    # Print summary
    print_data_summary(df)

# Legacy function names for backward compatibility
def load_csv(path: Path) -> pd.DataFrame:
    """Legacy function - now loads Excel files."""
    return load_excel(path)

def save_csv(df: pd.DataFrame, path: Path) -> None:
    """Legacy function - now saves Excel files."""
    save_excel(df, path)

def deduplicate(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Legacy function - now uses deduplicate_posts."""
    return deduplicate_posts(df, keys)

if __name__ == "__main__":
    # Test the storage functions
    print("🧪 Testing social media storage functions...")
    
    # Create sample data
    sample_data = pd.DataFrame([
        {
            'platform': 'facebook',
            'post_id': '123456789',
            'created_date': '2024-01-15',
            'brand': 'AKROPOLIS | Vilnius',
            'content': 'Test post 1',
            'likes': 100,
            'comments': 20,
            'shares': 10
        },
        {
            'platform': 'linkedin',
            'post_id': '987654321',
            'created_date': '2024-01-16',
            'brand': 'PANORAMA',
            'content': 'Test post 2',
            'likes': 50,
            'comments': 15,
            'shares': 5
        }
    ])
    
    # Test saving and loading
    test_path = Path("test_social_media_data.xlsx")
    
    try:
        # Test save
        save_excel(sample_data, test_path)
        
        # Test load
        loaded_data = load_excel(test_path)
        
        # Test deduplication
        duplicated_data = pd.concat([sample_data, sample_data], ignore_index=True)
        deduped_data = deduplicate_posts(duplicated_data)
        
        # Test summary
        print_data_summary(loaded_data)
        
        print("✅ All storage tests passed!")
        
    except Exception as e:
        print(f"❌ Storage test failed: {e}")
    
    finally:
        # Clean up test file
        if test_path.exists():
            test_path.unlink()
            print("🧹 Cleaned up test file")