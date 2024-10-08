BOT_NAME = "yad2_status_update"

SPIDER_MODULES = ["yad2_status_update.spiders"]
NEWSPIDER_MODULE = "yad2_status_update.spiders"
ROBOTSTXT_OBEY = False
SCRAPINGBEE_API_KEY = ''

DOWNLOADER_MIDDLEWARES = {
    'scrapy_scrapingbee.ScrapingBeeMiddleware': 725,
}
FEED_EXPORT_ENCODING = "utf-8-sig"
ITEM_PIPELINES = {
    'yad2_status_update.pipelines.SQLitePipeline': 300,
}

HTTPERROR_ALLOW_ALL = True
DB_FILE = "../properties.db"
RETRY_TIMES = 5

from datetime import datetime
def get_output_folder():
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%B").lower()  # Convert month name to lowercase
    day = now.strftime("%d")
    return f"{year}/{month}/{day}"


output_folder = get_output_folder()

timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
FEED_URI = f'csv_files/{output_folder}/yad2_update_{timestamp}.csv'
FEED_FORMAT = 'csv'
