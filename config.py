# === Configuration ===
import os
from datetime import datetime

# === DATE CONFIGURATION ===
# Define the analysis period for the dashboard and summaries
ANALYSIS_START_DATE = datetime(2025, 9, 23).date()  # September 5, 2025
ANALYSIS_END_DATE = datetime(2025, 10, 7).date()   # September 18, 2025

# === DEPLOYMENT CONFIGURATION ===
# Set to True when deploying to Streamlit Cloud, False for local development
STREAMLIT_HOSTING = False  # Change to True for Streamlit Cloud deployment

# Load environment variables based on deployment type
if STREAMLIT_HOSTING:
    # Streamlit Cloud - uses secrets management
    BRIGHTDATA_API_TOKEN = os.getenv("BRIGHTDATA_API_TOKEN")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
else:
    # Local development - uses .env file
    from dotenv import load_dotenv
    load_dotenv()
    BRIGHTDATA_API_TOKEN = os.getenv("BRIGHTDATA_API_TOKEN")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

TIMEZONE = "Europe/Vilnius"
DAYS_BACK = 14            # today + yesterday
MAX_POSTS = 30           # maximum posts per company
MAX_WORKERS = 1           # number of parallel scraping threads
MAX_WAIT_MINUTES = 30     # maximum wait time for Bright Data snapshots

# === BRIGHT DATA CONFIGURATION ===
BRIGHTDATA_DATASET_IDS = {
    "facebook": "gd_lkaxegm826bjpoo9m5"
}

# === PATH CONFIGURATION ===
if STREAMLIT_HOSTING:
    # Streamlit Cloud - direct repository root deployment
    FACEBOOK_MASTER_XLSX = "./data/facebook_master_file.xlsx"
    SUMMARIES_XLSX = "./data/summaries.xlsx"
else:
    # Local development - direct folder structure
    FACEBOOK_MASTER_XLSX = "./data/facebook_master_file.xlsx"
    SUMMARIES_XLSX = "./data/summaries.xlsx"

# Primary master file - now points to Facebook
MASTER_XLSX = FACEBOOK_MASTER_XLSX

# === DATA PROCESSING CONFIGURATION ===
# Note: July 1-14 labeled data has been integrated into the main master files

# === FILE STRUCTURE NOTES ===
# The system now uses Facebook master file:
# - facebook_master_file.xlsx: All Facebook posts with complete column structure + GPT labels
# Note: GPT-generated summaries and clusters are integrated into the main master files
# Note: MASTER_XLSX now points to Facebook master file

# === GPT Labeling Configuration ===
GPT_MAX_WORKERS = 20    # number of parallel GPT API calls
ENABLE_GPT_LABELING = True  # Set to False to skip GPT labeling
ENABLE_WEEKLY_SUMMARIES = True  # Set to False to skip weekly summary generation

# === SOCIAL MEDIA URLS ===
FACEBOOK_URLS = [
    "https://www.facebook.com/ozas.lt/",
    "https://www.facebook.com/panorama.lt/",
    "https://www.facebook.com/akropolis.vilnius/",
    "https://www.facebook.com/kaunoakropolis/?locale=lt_LT",
    "https://www.facebook.com/akropolis.klaipeda/",
    "https://www.facebook.com/akropolis.siauliai/",
    "https://www.facebook.com/vilniusoutlet/?locale=lt_LT",
    "https://www.facebook.com/outletparklietuva/",
    "https://www.facebook.com/CUPprekyboscentras/",
    "https://www.facebook.com/nordika.lt/",
    "https://www.facebook.com/bigvilnius/",
    "https://www.facebook.com/pceuropa.lt/",
    "https://www.facebook.com/G9shoppingcenter/",
    "https://www.facebook.com/saulesmiestas/?locale=lt_LT",
    "https://www.facebook.com/MOLAS.Klaipeda/",
    "https://www.facebook.com/Mega.lt/",
    "https://www.facebook.com/MAXIMALT/",
    "https://www.facebook.com/lidllietuva/?locale=lt_LT",
    "https://www.facebook.com/RimiLietuva/",
    "https://www.facebook.com/PrekybosTinklasIKI/"
]

# Combine all URLs for processing (Facebook only)
ALL_URLS = FACEBOOK_URLS

# === BRAND GROUPINGS FOR ANALYSIS ===
AKROPOLIS_LOCATIONS = [
    "AKROPOLIS | Vilnius",
    "AKROPOLIS | Klaipėda", 
    "AKROPOLIS | Šiauliai",
]

BIG_PLAYERS = ["PANORAMA", "OZAS", "Kauno Akropolis"]
SMALLER_PLAYERS = [
    "Vilnius Outlet",
    "BIG Vilnius",
    "Outlet Park",
    "CUP prekybos centras",
    "PC Europa",
    "G9",
]
OTHER_CITIES = [
    "SAULĖS MIESTAS",
    "PLC Mega",     # covers Kaunas Mega
]
RETAIL = ["Maxima LT", "Lidl Lietuva", "Rimi Lietuva", "IKI"]

SUBSETS_CORE = {
    "Big players": BIG_PLAYERS,
    "Smaller players": SMALLER_PLAYERS,
    "Other cities": OTHER_CITIES,
}
SUBSETS_WITH_RETAIL = {
    **SUBSETS_CORE,
    "Retail": RETAIL,
}

# Columns to uniquely identify a social media post
DEDUP_KEYS = [
    "post_id"
]
