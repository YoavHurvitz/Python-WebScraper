from datetime import datetime

BOT_NAME = "madlan"

SPIDER_MODULES = ["madlan.spiders"]
NEWSPIDER_MODULE = "madlan.spiders"

ROBOTSTXT_OBEY = False
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8-sig"
ITEM_PIPELINES = {
    'madlan.pipelines.DBPipeline': 300,
}

DB_FILE = '../madlan_db.db'

SCRAPINGBEE_API_KEY = ''

DOWNLOADER_MIDDLEWARES = {
    'scrapy_scrapingbee.ScrapingBeeMiddleware': 725,
}

CONCURRENT_REQUESTS = 5

def get_output_folder():
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%B").lower()  # Convert month name to lowercase
    day = now.strftime("%d")
    return f"{year}/{month}/{day}"


output_folder = get_output_folder()

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

FEED_URI = f'csv_files/{output_folder}/madlan_{timestamp}.csv'
FEED_FORMAT = 'csv'
