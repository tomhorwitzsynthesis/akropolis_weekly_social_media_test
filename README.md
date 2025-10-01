# Social Media Intelligence Pipeline

A comprehensive system for tracking, analyzing, and visualizing social media performance across Facebook and LinkedIn for retail and shopping center brands in Lithuania.

## 🚀 Features

- **Multi-Platform Scraping**: Facebook and LinkedIn posts via Bright Data API
- **AI-Powered Analysis**: GPT-4 powered content categorization and summarization
- **Interactive Dashboard**: Streamlit-based visualization with real-time metrics
- **Automated Pipeline**: End-to-end workflow from scraping to insights
- **Brand Comparison**: Performance analysis across competitor groups
- **Engagement Tracking**: Likes, comments, shares, and total engagement metrics

## 📋 Prerequisites

1. **Python 3.8+**
2. **Bright Data API Token** - For social media scraping
3. **OpenAI API Key** - For GPT labeling and summarization
4. **Required Python packages** (see requirements.txt)

## 🛠️ Installation

1. **Clone or download** the Social Media Tracking folder
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**:
   Create a `.env` file in the Social Media Tracking folder:
   ```env
   BRIGHTDATA_API_TOKEN=your_bright_data_token_here
   OPENAI_API_KEY=your_openai_api_key_here
   ```

## ⚙️ Configuration

Edit `config.py` to customize:

- **URLs to scrape**: Facebook and LinkedIn company pages
- **Date ranges**: How many days back to scrape
- **Brand groupings**: How to categorize competitors
- **API settings**: Rate limits, timeouts, etc.

### Key Configuration Options

```python
# Scraping settings
DAYS_BACK = 14                    # How many days to scrape
MAX_POSTS = 500                   # Max posts per company
MAX_WORKERS = 3                   # Parallel scraping threads

# GPT settings
ENABLE_GPT_LABELING = True        # Enable AI categorization
ENABLE_WEEKLY_SUMMARIES = True    # Enable AI summaries
GPT_MAX_WORKERS = 20              # Parallel GPT API calls
```

## 🚀 Usage

### Full Pipeline (Recommended)

Run the complete pipeline:
```bash
python pipeline.py
```

This will:
1. Scrape social media posts from all configured URLs
2. Transform and standardize the data
3. Apply GPT labeling and categorization
4. Merge with existing data and deduplicate
5. Generate weekly AI summaries
6. Save all data to Excel files

### Platform-Specific Scraping

Scrape only Facebook posts:
```bash
python pipeline.py facebook
```

Scrape only LinkedIn posts:
```bash
python pipeline.py linkedin
```

### Data Processing Only

Process existing data (useful for re-running GPT labeling):
```bash
python pipeline.py process
```

### Generate Summaries Only

Update AI summaries without re-scraping:
```bash
python pipeline.py summaries
```

### Check Pipeline Status

View current configuration and data status:
```bash
python pipeline.py status
```

## 📊 Dashboard

Launch the interactive dashboard:
```bash
streamlit run dashboard.py
```

The dashboard provides:
- **Performance comparisons** between current and previous weeks
- **Daily posting and engagement trends**
- **Top performing posts** by engagement
- **Content categorization** analysis
- **Platform performance** breakdowns
- **AI-generated weekly summaries**

## 📁 Data Structure

### Master Data File
`data/social_media_master_file.xlsx` contains:
- `platform`: Facebook or LinkedIn
- `post_id`: Unique post identifier
- `created_date`: When the post was published
- `brand`: Company/brand name
- `content`: Post text content
- `likes`, `comments`, `shares`: Engagement metrics
- `total_engagement`: Sum of all engagement
- `post_summary`: AI-generated one-sentence summary
- `cluster_1`, `cluster_2`, `cluster_3`: AI-generated content categories

### Summaries File
`data/summaries.xlsx` contains:
- Weekly AI-generated summaries for each brand
- Performance insights and trends
- Content focus area analysis

## 🏷️ Content Categories

The system automatically categorizes posts into 30+ categories including:
- Seasonal Promotions and Discounts
- Community Engagement and Events
- Health and Wellness Initiatives
- Family-Friendly Activities
- Fashion and Style Trends
- Food and Culinary Experiences
- Company News and Updates
- Social Responsibility and CSR
- And many more...

## 🔧 Troubleshooting

### Common Issues

1. **API Token Errors**:
   - Ensure `BRIGHTDATA_API_TOKEN` and `OPENAI_API_KEY` are set in `.env`
   - Check token validity and permissions

2. **No Data Scraped**:
   - Verify URLs in `config.py` are correct and accessible
   - Check Bright Data API status and quotas
   - Ensure date ranges are reasonable

3. **GPT Labeling Fails**:
   - Verify OpenAI API key and credits
   - Check rate limits in `config.py`
   - Ensure content is not empty or too long

4. **Dashboard Issues**:
   - Ensure data files exist in `data/` folder
   - Check that columns match expected format
   - Verify Streamlit installation

### Debug Mode

Run individual components for debugging:
```python
# Test scraper
python scraper.py

# Test transformer
python transform.py

# Test GPT labeler
python gpt_labeler.py
```

## 📈 Performance Tips

1. **Rate Limiting**: Adjust `MAX_WORKERS` and `GPT_MAX_WORKERS` based on API limits
2. **Data Volume**: Use `DAYS_BACK` to control scraping scope
3. **Caching**: Streamlit dashboard caches data for faster loading
4. **Parallel Processing**: GPT labeling uses parallel API calls for speed

## 🔄 Automation

Set up automated runs using cron (Linux/Mac) or Task Scheduler (Windows):

```bash
# Run daily at 9 AM
0 9 * * * cd /path/to/Social\ Media\ Tracking && python pipeline.py
```

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review configuration in `config.py`
3. Test individual components
4. Check API token validity and quotas

## 🎯 Brand Groups

The system automatically groups brands for analysis:

- **Akropolis Locations**: Vilnius, Klaipėda, Šiauliai
- **Big Players**: PANORAMA, OZAS, Kauno Akropolis
- **Smaller Players**: Vilnius Outlet, BIG Vilnius, etc.
- **Retail**: Maxima LT, Lidl Lietuva, Rimi Lietuva, IKI

Customize these groups in `config.py` as needed.

---

**Happy analyzing! 🎉**
