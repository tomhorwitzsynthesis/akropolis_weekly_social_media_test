# Social Media Intelligence Dashboard

A system for tracking and analyzing Facebook posts from Lithuanian shopping centers and retail brands.

## Quick Start for Colleagues

### 1. View the Dashboard
```bash
streamlit run dashboard.py
```
This opens an interactive dashboard in your browser showing:
- Performance comparisons between weeks
- Daily posting trends
- Top performing posts
- AI-generated content categories
- Weekly summaries

### 2. Understanding the Data
The system tracks Facebook posts from 21 brands including:
- **Akropolis locations**: Vilnius, Klaipėda, Šiauliai
- **Major competitors**: PANORAMA, OZAS, Kauno Akropolis
- **Smaller shopping centers**: BIG Vilnius, Vilnius Outlet, etc.
- **Retail chains**: Maxima LT, Lidl Lietuva, Rimi Lietuva, IKI

Interestingly enough, Kauno Akropolis is not part of the Akropolis franchise, so they are tracked separately.

### 3. Key Data Files
- `data/facebook_master_file.xlsx` - All Facebook posts with engagement metrics and AI categories
- `data/summaries.xlsx` - Weekly AI-generated summaries for each brand

## Weekly Data Updates

### Option 1: Manual Update (Recommended)
Fill in the correct dates (ANALYSIS_START_DATE and ANALYSIS_END_DATE) in the config.py file, which should be a 14-day period. 
(For me it's always a bit counter-intuivite, but so 1/10 - 15/10 is not a 14 day period, it's actually 15 days, so it should be 1/10 - 14/10, just to be sure.)

It's a good idea to make copies of the output files in the Social Media Tracking\data folder (facebook_master_file and summaries) before you run the analysis, just to have a backup if something seems off.

If that's all done, run this command or simply go to the file in VS Code/Cursor and run it.
```bash
python pipeline.py
```

This will:
1. Scrape new Facebook posts from the last 14 days
2. Apply AI categorization to new posts
3. Merge with existing data (removing duplicates)
4. Generate updated weekly summaries

It will run from Bright Data, so you'll see 20-second updates to see if the scraping is ready. This could take 8-15 minutes and it's always very unpredictable how long it will take. If it takes too long, the scraping will fail, but don't start it again immediately!

You can go to Bright Data > Web Scrapers > Web Scrapers Library > facebook.com > "Facebook - Pages Posts by Profiles URL - collect by URL" > press "Next" (for some reason it does not get added to your Web Scraper menu so you have to do this every time) > Go to the "Logs" tab > You should see the Snapshot running, and you can see what the Status is. 

If the scraping failed but the Status is Ready, you can run the pipelin.py file again, but this time in your command line with the command python pipelin.py snapshot s_mget3mpl1lt2lqmhxu (you can find your snapshot on Bright Data or in the output of the code: Snapshot s_mget3mpl1lt2lqmhxu status: running (checked at 7 min 0 sec)")

If the Snapshot is not Ready, or can't be downloaded for some reason, give it a few hours and try again... Bright Data can be very annoying.

If you just want to refresh the AI summaries without scraping new data:
```bash
python pipeline.py summaries
```

This can be useful if for some reason the summaries are not generated correctly or if you want to change the summary prompts but not have to rerun the whole scraping again. Can also be used to run summaries on data from days or weeks ago, by just changing the dates in config to any you want and then running just the summaries.

## What You Need to Run Updates

### Required API Keys
You need these in a `.env` file in the project folder:
```
BRIGHTDATA_API_TOKEN=your_token_here
OPENAI_API_KEY=your_key_here
```

After running all the analyses, it's good to check the facebook_master_file to see if everything is okay. The format of the file might be looking a bit weird, as the way of scraping and analysis has changed throughout the development, so not all columns are filled in, but this is okay.

Check:
- If the date_posted column has rows with the newly scraped dates
- If post_summary, cluster_1, cluster_2 and cluster_3 are filled in for those
- If the total_engagement column has values, it's normal that the comments and shares in those columns are 0
- If the summaries.xlsx file has a new row with the dates from the config file, and check quickly if the brands actually have summaries or that it says for a majority "This week there were no new posts" or something similar
- Run the dashboard and check if you can select the new dates and if the numbers in the summaries (number of posts) is exactly the same as the weekly metrics at the top.