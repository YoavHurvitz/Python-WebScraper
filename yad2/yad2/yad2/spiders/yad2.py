import re
import json
import logging
import sqlite3
from datetime import datetime
from ..settings import DB_FILE
from scrapingbee import ScrapingBeeClient
from ..settings import SCRAPINGBEE_API_KEY
from scrapy_scrapingbee import ScrapingBeeSpider, ScrapingBeeRequest

client = ScrapingBeeClient(api_key=SCRAPINGBEE_API_KEY)

for_sale_api_url = 'https://www.yad2.co.il/realestate/_next/data/{}/forsale.json?page={}'
for_rent_api_url = 'https://www.yad2.co.il/realestate/_next/data/{}/rent.json?page={}'
for_commercial_api_url = 'https://gw.yad2.co.il/feed-search-legacy/realestate/commercial?dealType={}&page={}'

headers = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.yad2.co.il/realestate/forsale?page=2",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "baggage": "sentry-environment=prod,sentry-release=realestate-eca7bf4f1cd90076dc4b13dc2cafe86cf970bdfb,sentry-public_key=fd7d23a8d868485399868caf5fd39b0d,sentry-trace_id=e754e16bd2214f098a2958a485f161a8,sentry-sample_rate=1,sentry-sampled=true",
    "sec-ch-ua": "\"Google Chrome\";v=\"125\", \"Chromium\";v=\"125\", \"Not.A/Brand\";v=\"24\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sentry-trace": "e754e16bd2214f098a2958a485f161a8-89263c70485455f8-1",
    "uzlc": "true",
    "x-nextjs-data": "1"
}

cookies = {
    "__uzma": "cb4f2559-9eec-4a5b-be07-0a9e0cd96fde",
    "__uzmb": "1716879482",
    "__uzme": "1183",
    "y2018-2-cohort": "45",
    "__ssds": "3",
    "__uzmaj3": "b9018fbd-10b9-46ac-a521-20a297b88f7f",
    "__uzmbj3": "1716879487",
    "__uzmlj3": "r5PxcRlEuLWmcrb4zEVDrTOABN8k+eRIqT7yLcMHdKE=",
    "_gcl_au": "1.1.55793898.1716879491",
    "canary": "never",
    "_gid": "GA1.3.1470775463.1716879560",
    "guest_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwYXlsb2FkIjp7InV1aWQiOiIwNmRlZGJhMy03NGU2LTQ5NjktYTIxOC1iZTZiYjc0YWJhODUifSwiaWF0IjoxNzE2ODc5NTYwLCJleHAiOjE3NTAxMzI0NDAwMTl9.nRAR0KeJF7pv4ucuiBj8-Xe8XIkO86m6MBE4APkX374",
    "_hjSession_266550": "eyJpZCI6ImY0NDc5MzUwLWU0Y2QtNDc2Yi04YjAwLTFkY2FlZTNlYWY3MCIsImMiOjE3MTY4Nzk1OTg0MzcsInMiOjAsInIiOjAsInNiIjowLCJzciI6MCwic2UiOjAsImZzIjoxLCJzcCI6MH0=",
    "abTestKey": "69",
    "_vwo_uuid_v2": "D30E59DFA1C706C1BADF6FA9D70807E8B|d63e68bdf1b9bd4de5b7f9b3954bac89",
    "_hjSessionUser_266550": "eyJpZCI6IjhkOWU4YzIxLTRhYzUtNWUzZi1hZTA1LWFmMDk0MmI3MzJlMCIsImNyZWF0ZWQiOjE3MTY4Nzk1OTg0MzYsImV4aXN0aW5nIjp0cnVlfQ==",
    "recommendations-home-category": "{\"categoryId\":2,\"subCategoryId\":1}",
    "server_env": "production",
    "_ga": "GA1.1.509994043.1716879503",
    "leadSaleRentFree": "88",
    "y2_cohort_2020": "62",
    "use_elastic_search": "1",
    "cohortGroup": "C",
    "favorites_userid": "fbb96645077",
    "bc.visitor_token": "8f2dd734-8bda-6e68-44a9-6057272542c7",
    "dicbo_id": "%7B%22dicbo_fetch%22%3A1716881269454%7D",
    "__ssuzjsr3": "a9be0cd8e",
    "__uzmcj3": "222874373801",
    "__uzmdj3": "1716881433",
    "__uzmfj3": "7f6000e49e3477-186f-4d03-80f4-08515d71e8f517168794870531946532-71af64eb8667295743",
    "_ga_GQ385NHRG1": "GS1.1.1716879502.1.1.1716881442.47.0.0",
    "__uzmc": "7988013697919",
    "__uzmd": "1716881444",
    "__uzmf": "7f6000e49e3477-186f-4d03-80f4-08515d71e8f517168794829951961931-910e87a0a38157a9136",
    "uzmx": "7f90008da028b1-48ec-4759-8ce2-1472062168351-17168794829951961931-61d1dbcfd0bd5a5d181"
}


def get_session_id():
    for i in range(5):
        try:
            response = client.get('https://www.yad2.co.il/')
            return re.search(r'"buildId":"([^"]+)', response.text).group(1)
        except Exception as e:
            logging.warning(e)


class Yad2Scraper(ScrapingBeeSpider):
    name = "yad2"
    listing_ids = set()
    key = get_session_id()

    def __init__(self, *args, **kwargs):
        super(Yad2Scraper, self).__init__(*args, **kwargs)
        try:
            self.conn = sqlite3.connect(DB_FILE)
            self.cursor = self.conn.cursor()
            self.cursor.execute('SELECT Yad2ID FROM properties WHERE ACTIVE=1')
            idx = [row[0] for row in self.cursor.fetchall()]
            self.listing_ids = set(idx)
        except:
            self.listing_ids = set()

    def start_requests(self):
        self.key = 'RwouRVXgwDyAP2pNRcnUd'
        yield ScrapingBeeRequest(
            url=for_sale_api_url.format(self.key, 1),
            method='GET',
            dont_filter=True,
            headers=headers,
            meta={'type': 'buy', 'first': True}
        )

        yield ScrapingBeeRequest(
            url=for_rent_api_url.format(self.key, 1),
            method='GET',
            dont_filter=True,
            headers=headers,
            meta={'type': 'rent', 'first': True}
        )

        yield ScrapingBeeRequest(
            url=for_commercial_api_url.format(0, 1),
            method='GET',
            dont_filter=True,
            headers=headers,
            callback=self.parse_commercial,
            meta={'type': 'commercial_buy', 'AdType': 'buy', 'first': True}
        )
        yield ScrapingBeeRequest(
            url=for_commercial_api_url.format(1, 1),
            method='GET',
            dont_filter=True,
            headers=headers,
            callback=self.parse_commercial,
            meta={'type': 'commercial_rent', 'AdType': 'rent', 'first': True}
        )

    def parse(self, response, **kwargs):
        try:
            data = response.json()
        except Exception as e:
            logging.warning(e)
            return
        _type = response.meta.get('type')
        if response.meta.get('first'):
            total_pages = data.get('pageProps', {}).get('feed', {}).get('pagination', {}).get('totalPages')
            if _type == "buy":
                # for i in range(2, 50):
                for i in range(2, total_pages + 1):
                    yield ScrapingBeeRequest(
                        url=for_sale_api_url.format(self.key, i),
                        method='GET',
                        dont_filter=True,
                        headers=headers,
                        meta={'type': _type}
                    )
            elif _type == "rent":
                for i in range(2, total_pages + 1):
                # for i in range(2, 50):
                    yield ScrapingBeeRequest(
                        url=for_rent_api_url.format(self.key, i),
                        method='GET',
                        dont_filter=True,
                        headers=headers,
                        meta={'type': _type}
                    )
        items = data.get('pageProps', {}).get('feed', {}).get('private', []) + data.get('pageProps', {}).get('feed',
                                                                                                             {}).get(
            'agency', [])
        for _ in items:
            idx = _.get("token") or _.get("link_token")
            if idx in self.listing_ids:
                continue
            url = f'https://www.yad2.co.il/realestate/item/{idx}'
            _ad_type = _.get('adType', '')
            from_broker = False
            if _ad_type == 'agency':
                from_broker = True
            yield ScrapingBeeRequest(
                url=url,
                dont_filter=True,
                meta={'type': _type, 'FromBroker': from_broker},
                callback=self.parse_details
            )

    def parse_commercial(self, response, **kwargs):
        data = response.json()
        _type = response.meta.get('type')
        ad_type = response.meta.get('AdType')
        if response.meta.get('first'):
            total_pages = data.get('data', {}).get('pagination', {}).get('last_page')
            if "rent" in ad_type:
                ad_type_value = 1
            else:
                ad_type_value = 0
            # for i in range(2, 50):
            for i in range(2, total_pages + 1):
                yield ScrapingBeeRequest(
                    url=for_commercial_api_url.format(ad_type_value, i),
                    method='GET',
                    dont_filter=True,
                    headers=headers,
                    callback=self.parse_commercial,
                    meta={'type': _type, 'AdType': ad_type}
                )
        items = data.get('data', {}).get('feed', {}).get('feed_items', [])
        for _ in items:
            if _.get("type", '') == 'ad':
                idx = _.get("link_token") or _.get("token")
                if idx in self.listing_ids:
                    continue
                url = f'https://www.yad2.co.il/realestate/item/{idx}'
                ad_type = _.get('feed_source', '')
                from_broker = False
                if ad_type == 'commercial':
                    from_broker = True
                yield ScrapingBeeRequest(
                    url=url,
                    dont_filter=True,
                    meta={'type': _type, 'AdType': ad_type, 'FromBroker': from_broker},
                    callback=self.parse_details
                )

    def parse_details(self, response, **kwargs):
        try:
            json_data = json.loads(response.css('script#__NEXT_DATA__::text').get(''))
        except Exception as e:
            return
        details_data = json_data.get('props', {}).get('pageProps', {}).get('dehydratedState', {}).get('queries', [{}])[
            -1].get('state', {}).get('data', {})

        if not details_data:
            return
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
        street = str(details_data.get('address', {}).get('street', {}).get('text', '')).strip()
        street_number = str(details_data.get('address', {}).get('house', {}).get('number', '')).strip()
        if street_number and len(str(street_number)):
            _street_number = str(street_number) + " "
            form_broker_crm = True
        else:
            form_broker_crm = False
            _street_number = ""

        from_broker = response.meta.get('FromBroker', '')

        city = details_data.get('address', {}).get('city', {}).get('text', '')
        if (not street) or (not _street_number):
            from_broker = True
        seo_title = f'{street} {_street_number}{city}'.strip()
        balcony = details_data.get('additionalDetails', {}).get('balconiesCount', 0)

        doe = 'כניסה גמישה' if details_data.get('additionalDetails', {}).get('entranceDate',
                                                                             '') else 'כניסה מידית'
        item = {
            "seo_title": seo_title,
            "Yad2ID": details_data.get('token'),
            "Status": response.meta.get('type'),
            "City": city,
            "AdType": response.meta.get('AdType'),
            "Neighborhood": details_data.get('address', {}).get('neighborhood', {}).get('text'),
            "Street": street,
            "StreetNumber": street_number,
            "Latitude": details_data.get('address', {}).get('coords', {}).get('lat'),
            "Longitude": details_data.get('address', {}).get('coords', {}).get('lon'),
            "PropertyType": details_data.get('additionalDetails', {}).get('property', {}).get('text'),
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
            # "FromBrokerCRM": form_broker_crm,
            "Elevator": amenities_data.get('Elevator', False),
            "AirConditioning": amenities_data.get('Air conditioning', False),
            "parking": amenities_data.get('parking', False),
            "balcony": True if balcony else False,
            "bars": amenities_data.get('bars', False),
            "dimension": amenities_data.get('dimension', False),
            "warehouse": amenities_data.get('warehouse', False),
            "HandicapAccessibility": amenities_data.get('handicap access', False),
            "PropertyMode": details_data.get('additionalDetails', {}).get('propertyCondition', {}).get('text', ''),
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
        self.listing_ids.add(details_data.get('token'))
        yield item
