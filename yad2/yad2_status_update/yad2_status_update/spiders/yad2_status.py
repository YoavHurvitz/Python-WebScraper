import json
import sqlite3
from datetime import datetime
from ..settings import DB_FILE
from scrapy_scrapingbee import ScrapingBeeSpider, ScrapingBeeRequest

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "sec-ch-ua": "\"Google Chrome\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\""
}

cookies = {
    "__uzma": "5cd44322-e51a-47cf-b3ac-c2ec447a075a",
    "__uzmb": "1717413616",
    "__uzme": "9886",
    "canary": "never",
    "__ssds": "3",
    "__uzmaj3": "3b8cb948-f608-44bd-bc1d-360eeda7f48e",
    "__uzmbj3": "1717413623",
    "__uzmlj3": "kJdPJLyfmGtODr5Xfbf/fj3dqYV6hLe8X03VeStz0CA=",
    "guest_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InV1aWQiOiI4ODVhYzcyZS0xMDRhLTQxNTEtOWViZS1hNzdjNGE4NzAwZDgifSwiaWF0IjoxNzE3NDEzNjI5LCJleHAiOjE3NTA2NjcwNDMwODN9.pQLUPTMcw-MDDPAB-KYC2EwI0K70DfXqlcElX34gaQg",
    "_gcl_au": "1.1.1267156436.1717413630",
    "_gid": "GA1.3.436465143.1717413632",
    "bc.visitor_token": "ee7c20e1-7db1-b417-a457-4e697fb1b5e2",
    "_hjSessionUser_266550": "eyJpZCI6IjhjMGY4ODgxLTA1ZWUtNThlNC04NDI1LWI3ODdhMzZiZTY5ZiIsImNyZWF0ZWQiOjE3MTc0MTM2NDE5NjAsImV4aXN0aW5nIjp0cnVlfQ==",
    "__rtbh.lid": "%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22ezMHxLLh99MWKstof0a6%22%7D",
    "__rtbh.uid": "%7B%22eventType%22%3A%22uid%22%7D",
    "__gads": "ID=94bb8a185ea38c16:T=1717414940:RT=1717500977:S=ALNI_MaKg1D9tbeEonbMkd1oOxtAImL9oQ",
    "__gpi": "UID=00000d7e5cb9a5a4:T=1717414940:RT=1717500977:S=ALNI_MZhvKsNHXj5qp-3m8HAKhRJDKGbOw",
    "__eoi": "ID=a7250081757be7b5:T=1717414940:RT=1717500977:S=AA-AfjZXyEUcobnT7TVuTEvaYmMi",
    "abTestKey": "3",
    "_vwo_uuid_v2": "DB423A9CFBBF95FD8B5B2E0CC087CEE57|874235cacf300c56711002706200e44b",
    "server_env": "production",
    "leadSaleRentFree": "39",
    "y2_cohort_2020": "25",
    "use_elastic_search": "1",
    "y2018-2-cohort": "44",
    "__ssuzjsr3": "a9be0cd8e",
    "dicbo_id": "%7B%22dicbo_fetch%22%3A1717505662431%7D",
    "_ga_GQ385NHRG1": "GS1.1.1717505043.7.1.1717505759.60.0.0",
    "_ga": "GA1.3.1624592776.1717413630",
    "__uzmcj3": "9160410354604",
    "__uzmdj3": "1717505788",
    "__uzmfj3": "7f6000270d5a54-666c-4f39-ba59-8352683a967d171741362324492165108-ffb417a1abc818f3103",
    "cohortGroup": "C",
    "favorites_userid": "haa1355803175",
    "__uzmc": "6412434633175",
    "__uzmd": "1717505791",
    "__uzmf": "7f6000270d5a54-666c-4f39-ba59-8352683a967d171741361647892175098-160cb561f2731b8d346",
    "uzmx": "7f90007bab0e23-6433-4ffa-875c-43642c792a1c2-171741361647892175098-4a85d5df7f1bf7e3508"
}

yad2_ids = []

class YadScraper(ScrapingBeeSpider):
    name = "yad2_updater"
    listing_ids = []

    def __init__(self, *args, **kwargs):
        super(YadScraper, self).__init__(*args, **kwargs)
        try:
            self.conn = sqlite3.connect(DB_FILE)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            self.cursor.execute('SELECT * FROM properties WHERE ACTIVE=1')

            # self.cursor.execute('SELECT Yad2ID, TotalPrice, DateUpdated FROM properties WHERE ACTIVE=1')
            self.listing_ids = [dict(row) for row in self.cursor.fetchall()]
            # self.listing_ids = set(idx)
        except:
            # self.listing_ids = set()
            pass

    def start_requests(self):
        for row in self.listing_ids:    
            yad_id = row['Yad2ID']
            url = f'https://www.yad2.co.il/realestate/item/{yad_id}'
            yield ScrapingBeeRequest(
                url=url,
                dont_filter=True,
                callback=self.parse_details,
                meta={'dont_redirect': True, 'id': yad_id, 'row': row}
            )

    def parse_details(self, response, **kwargs):
        data_id = response.meta.get('id')
        row = response.meta.get('row')

        old_price = row['TotalPrice']
        updated_date = row['DateUpdated']
        # new_row = row.copy()
        if response.status != 200:
            item = {
                "seo_title": "Listing has been removed",
                "Yad2ID": data_id,
                "Active": False
            }
            yield item
        else:
            try:
                json_data = json.loads(response.css('script#__NEXT_DATA__::text').get(''))
            except Exception as e:
                return

            details_data = json_data['props']['pageProps']['dehydratedState']['queries'][-1]['state']['data']
            total_price = details_data.get('price', '')

            if old_price == total_price:
                return

            else:
                if total_price:
                    # try:
                    #     updated_date = json.loads(updated_date)
                    # except:
                    #     updated_date = []
                    #     # pass

                    # updated_date.append(
                    #     {"updatedAt": details_data.get('dates', {}).get('updatedAt', ''),
                    #       "price": details_data.get('price')})

                    amenities_data = {}
                    for div in response.css('ul[data-testid="in-property-grid"] > li'):
                        value = True
                        if "disabled" in div.css('::attr(class)').get(''):
                            value = False
                        text = div.css('span.in-property-item_text__lKNTv::text').get('')
                        if "מעלית" in text:
                            amenities_data['Elevator'] = value
                        elif "מזגן" in text:
                            amenities_data['Air conditioning'] = value
                        elif "חניה" in text:
                            amenities_data['parking'] = value
                        elif "מרפסת" in text:
                            amenities_data['balcony'] = value
                        elif "סורגים" in text:
                            amenities_data['bars'] = value
                        elif 'ממ"ד' in text:
                            amenities_data['dimension'] = value
                        elif "מחסן" in text:
                            amenities_data['warehouse'] = value
                        elif "גישה לנכים" in text:
                            amenities_data['handicap access'] = value
                    street = details_data.get('address', {}).get('street', {}).get('text', '').strip()
                    street_number = str(details_data.get('address', {}).get('house', {}).get('number', '')).strip()
                    if street_number and len(str(street_number)):
                        _street_number = str(street_number) + " "
                    else:
                        _street_number = ""

                    from_broker = row.get('FromBroker', '')

                    if (not street) or (not _street_number):
                        from_broker = True

                    city = details_data.get('address', {}).get('city', {}).get('text', '')
                    seo_title = f'{street} {_street_number}{city}'.strip()
                    balcony = details_data.get('additionalDetails', {}).get('balconiesCount', 0)
                    doe = 'כניסה גמישה' if details_data.get('additionalDetails', {}).get('isEnterDateFlexible',
                                                                                         '') else 'כניסה מידית'
                    item = {
                        "seo_title": seo_title,
                        "Yad2ID": details_data.get('token'),
                        "Status": row.get('Status'),
                        "City": city,
                        "AdType": row.get('AdType'),
                        "Neighborhood": details_data.get('address', {}).get('neighborhood', {}).get('text'),
                        "Street": street,
                        "StreetNumber": street_number,
                        "Latitude": details_data.get('address', {}).get('coords', {}).get('lat'),
                        "Longitude": details_data.get('address', {}).get('coords', {}).get('lon'),
                        "PropertyType": details_data.get('additionalDetails', {}).get('property', {}).get('textEng'),
                        "RoomsNumber": details_data.get('additionalDetails', {}).get('roomsCount', ''),
                        "FloorNumber": details_data.get('address', {}).get('house', {}).get('floor'),
                        "FloorsInBuilding": details_data.get('additionalDetails', {}).get('buildingTopFloor', ''),
                        "SizeInMeters": details_data.get('additionalDetails', {}).get('squareMeter', ''),
                        "TotalPrice": details_data.get('price'),
                        "Url": response.url,
                        "Active": True,
                        "PriceperSquereMeter": None,
                        "PropertyDescription": details_data.get('metaData', {}).get('description', '').strip(),
                        "AdvertiserName": details_data.get('customer', {}).get('name', ''),
                        "AdvertiserPhone": details_data.get('customer', {}).get('phone', ''),
                        "FromBroker": from_broker,
                        "Elevator": amenities_data.get('Elevator', False),
                        "AirConditioning": amenities_data.get('Air conditioning', False),
                        "parking": amenities_data.get('parking', False),
                        "balcony": True if balcony else False,
                        "bars": amenities_data.get('bars', False),
                        "dimension": amenities_data.get('dimension', False),
                        "warehouse": amenities_data.get('warehouse', False),
                        "HandicapAccessibility": amenities_data.get('handicap access', False),
                        "PropertyMode": details_data.get('additionalDetails', {}).get('propertyCondition', {}).get('text',
                                                                                                                   ''),
                        "DateOfEntry": doe,
                        "DatePublished": details_data.get('dates', {}).get('createdAt', ''),
                        "DateUpdated": None,
                        "DateScraped": datetime.now().strftime("%m/%d/%y %H:%M %p")
                    }

                    gallery_images = {}
                    for _, image in enumerate(details_data.get('metaData', {}).get('images', [])):
                        gallery_images[str(_)] = image

                    item['gallery_images'] = gallery_images
                    try:
                        item['PriceperSquereMeter'] = item.get('TotalPrice') / item.get('SizeInMeters')
                    except:
                        pass
                    # self.listing_ids.add(details_data.get('token'))
                    yield item
