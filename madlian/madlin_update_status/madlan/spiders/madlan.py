import json
import sqlite3
from datetime import datetime
from ..settings import DB_FILE
from scrapy_scrapingbee import ScrapingBeeSpider, ScrapingBeeRequest

api_url = 'https://www.madlan.co.il/api2'

headers = {
    "Accept-Language": "en-US,en;q=0.9",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleGFjdC10aW1lIjoxNzEzNzA0NjU1NjMxLCJwYXlsb2FkIjoie1widWlkXCI6XCJiYzBhYTJlOC1mZTdkLTQ2NDctYmI1MC1hOWYxOTI0YTViYjBcIixcInNlc3Npb24taWRcIjpcImVmZWI3NGEyLWNkNzUtNGNkMC05MTg1LWNhZTU1NWJiMzc3NFwiLFwidHRsXCI6NjMxMTUyMDB9IiwiaWF0IjoxNzEzNzA0NjU1LCJpc3MiOiJsb2NhbGl6ZSIsInVzZXJJZCI6ImJjMGFhMmU4LWZlN2QtNDY0Ny1iYjUwLWE5ZjE5MjRhNWJiMCIsInJlZ2lzdHJhdGlvblR5cGUiOiJWSVNJVE9SIiwicm9sZXMiOlsiVklTSVRPUiJdLCJpc0ltcGVyc29uYXRpb25Mb2dJbiI6ZmFsc2UsInNhbHQiOiJlZmViNzRhMi1jZDc1LTRjZDAtOTE4NS1jYWU1NTViYjM3NzQiLCJ2IjoyLCJleHAiOjE3NzY4MTk4NTV9.KPecYhX0ZcW1gpMCk7uVciecgnnjWpt4CnSV6xZcJdY",
    "Connection": "keep-alive",
    "Origin": "https://www.madlan.co.il",
    "Referer": "https://www.madlan.co.il/for-rent/%D7%99%D7%A9%D7%A8%D7%90%D7%9C?page=3&tracking_search_source=type_change&marketplace=residential",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "X-Original-Hostname": "www.madlan.co.il",
    "X-Original-Url": "/for-rent/%D7%99%D7%A9%D7%A8%D7%90%D7%9C?page=3&tracking_search_source=type_change&marketplace=residential",
    "X-Requested-With": "XMLHttpRequest",
    "X-Source": "web",
    "accept": "*/*",
    "content-type": "application/json",
    "sec-ch-ua": "\"Chromium\";v=\"124\", \"Google Chrome\";v=\"124\", \"Not-A.Brand\";v=\"99\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\""
}

cookies = {
    "_pxvid": "a47a9704-ffdf-11ee-9472-00856d94a034",
    "_gcl_au": "1.1.164556883.1713704650",
    "APP_CTX_USER_ID": "4db3d5dd-f50e-4b0a-8d40-33d8c899d327",
    "Infinite_user_id_key": "4db3d5dd-f50e-4b0a-8d40-33d8c899d327",
    "Infinite_ab_tests_context_v2_key": "{%22context%22:{%22whatsappSticky%22:%22modeB%22%2C%22_be_sortMarketplaceByDate%22:%22modeA%22%2C%22_be_sortMarketplaceAgeWeight%22:%22modeA%22%2C%22_be_sortMarketplaceByHasAtLeastOneImage%22:%22modeA%22%2C%22removeWizard%22:%22modeB%22%2C%22whatsAppPoc%22:%22modeB%22}}",
    "USER_TOKEN_V2": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleGFjdC10aW1lIjoxNzEzNzA0NjU1NjMxLCJwYXlsb2FkIjoie1widWlkXCI6XCJiYzBhYTJlOC1mZTdkLTQ2NDctYmI1MC1hOWYxOTI0YTViYjBcIixcInNlc3Npb24taWRcIjpcImVmZWI3NGEyLWNkNzUtNGNkMC05MTg1LWNhZTU1NWJiMzc3NFwiLFwidHRsXCI6NjMxMTUyMDB9IiwiaWF0IjoxNzEzNzA0NjU1LCJpc3MiOiJsb2NhbGl6ZSIsInVzZXJJZCI6ImJjMGFhMmU4LWZlN2QtNDY0Ny1iYjUwLWE5ZjE5MjRhNWJiMCIsInJlZ2lzdHJhdGlvblR5cGUiOiJWSVNJVE9SIiwicm9sZXMiOlsiVklTSVRPUiJdLCJpc0ltcGVyc29uYXRpb25Mb2dJbiI6ZmFsc2UsInNhbHQiOiJlZmViNzRhMi1jZDc1LTRjZDAtOTE4NS1jYWU1NTViYjM3NzQiLCJ2IjoyLCJleHAiOjE3NzY4MTk4NTV9.KPecYhX0ZcW1gpMCk7uVciecgnnjWpt4CnSV6xZcJdY",
    "AWSALB": "RqshwWm769TEfw+i0xoD0/RrLEM7ZSb6UfPDhOAxfp7mnl9IGRNtnneeffgdtUad9HG6EuPyZkFvwnbNVyiraJZ88CpWfkZ8kvEdIudaUBjluxwym91TvW1zgdkK",
    "_hjSessionUser_1261107": "eyJpZCI6IjM2ODZkYTlhLTg3NTgtNTNlMi04Y2Y1LWU3ZjQ1ZmM2MmU2NCIsImNyZWF0ZWQiOjE3MTM3MDQ2NTA1NjgsImV4aXN0aW5nIjp0cnVlfQ==",
    "pxcts": "7b6a9806-00d0-11ef-9c05-612482361809",
    "APP_CTX_SESSION_ID": "268fc655-e6f3-4bd0-9192-bcd24eea1d92",
    "_hjSession_1261107": "eyJpZCI6Ijc1ODYwZmIwLWFkM2EtNGVjYi05OTAxLTFiMTk0YWY2OTIzNCIsImMiOjE3MTM4MDgwOTU1NTIsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjowLCJzcCI6MH0=",
    "_gid": "GA1.3.152912757.1713808100",
    "_clck": "cdw1xc%7C2%7Cfl5%7C0%7C1572",
    "ten_seconds_sending_flag_name": "1",
    "__za_cds_19763687": "%7B%22data_for_campaign%22%3A%7B%22country%22%3A%22-%22%2C%22language%22%3A%22EN%22%2C%22ip%22%3A%22103.154.65.246%22%2C%22start_time%22%3A1713808100000%7D%7D",
    "__za_19763687": "%7B%22sId%22%3A6217265%2C%22dbwId%22%3A%221%22%2C%22sCode%22%3A%22fc104b178ed9a0da9d2132ef8360618b%22%2C%22sInt%22%3A5000%2C%22na%22%3A1%2C%22td%22%3A1%2C%22ca%22%3A%221%22%7D",
    "_px3": "440a2663c678acb5440b9a03b7e370eedf9f00f8355f141bc2514503107de598:iGm9F7ddLBa8vGtIpwT4QwnQsttiUkSQ/3dt/wKJpevVaEOYS/3sh93+VIEL7HQwKYgYzgkWsPWHMAzv6XNb4g==:1000:AXbc+0sinQ8rKhmSv4vFqTuQqBxa1yvBljBj7bmnvV68cjQCe3ct5JAYWEUfJaWdXgcuWvTmybhwPLjCpgV/cd8kVMKrKEfJApJyrItQOsMtTdzi4dC4HJN3mcIeR4fA1HA9m/B6Q4It/uzP/3Qr15p01euH6aXAwGpwci4/42G1gC9JX4qOfGAxLZbT8JHxPFGSwkP2zBm57g/ztIHZuysR+zzXeA/MRIdnj2i35/Q=",
    "_ga": "GA1.3.197777778.1713704651",
    "__za_cd_19763687": "%7B%22visits%22%3A%22%5B1713808102%2C1713704656%5D%22%2C%22campaigns_status%22%3A%7B%2288660%22%3A1713808121%2C%2289525%22%3A1713704656%7D%7D",
    "WINDOW_WIDTH": "446",
    "_clsk": "1moa3e9%7C1713808161914%7C6%7C1%7Cf.clarity.ms%2Fcollect",
    "page_view_count_name": "7",
    "_ga_F3RMNQ4VM9": "GS1.1.1713808096.3.1.1713808162.60.0.0"
}


class MadlanScraper(ScrapingBeeSpider):
    name = "madlan_updater"
    listing_ids = None

    def __init__(self, *args, **kwargs):
        super(MadlanScraper, self).__init__(*args, **kwargs)
        try:
            self.conn = sqlite3.connect(DB_FILE)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            # self.cursor.execute('SELECT MadlanID, TotalPrice, DateUpdated FROM Amazon_Listing WHERE ACTIVE=1')
            self.cursor.execute('SELECT * FROM Amazon_Listing WHERE ACTIVE=1')
            # idx = [(row[0], row[1], row[2]) for row in self.cursor.fetchall()]
            self.listing_ids = [dict(row) for row in self.cursor.fetchall()]
            # self.listing_ids = set(idx)
        except Exception as e:
            print(e)
            # self.listing_ids = set()

    def start_requests(self):
        for row in self.listing_ids:
            madlin_id = row['MadlanID']
            url = f'https://www.madlan.co.il/listings/{madlin_id}'
            yield ScrapingBeeRequest(
                url=url,
                dont_filter=True,
                callback=self.parse_details,
                meta={'dont_redirect': True, 'id': madlin_id, 'row': row}
            )

    def parse_details(self, response, **kwargs):
        data_id = response.meta.get('id')
        row = response.meta.get('row')
        old_price = row['TotalPrice']
        row_copy = row.copy()
        # if response.url == 'https://www.madlan.co.il/?hiddenListingModal=1':
        if data_id not in response.url:
            item = {
                "seo_title": "Listing has been removed",
                "MadlanID": data_id,
                "Active": False
            }

            yield item

        else:
            try:
                price = response.css('div[data-auto="price"] > div > div::text').get('').lstrip('₪').replace(',',
                                                                                                             '') or response.css(
                    'div[data-auto="current-price"] ::text').get('').strip(' ₪‏').replace(',', '')
                price = int(price)

            except:
                return

            if price == old_price:
                return
            else:
                try:
                    updated_date = json.loads(row['DateUpdated'])
                    if isinstance(updated_date, dict):
                        updated_date = [updated_date]
                except:
                    updated_date = []

                updated_date.append(
                    {"updatedAt": datetime.now().strftime("%m/%d/%y %H:%M %p"),
                     "price": price}
                )
                row_copy['DateUpdated'] = updated_date

                yield row_copy
