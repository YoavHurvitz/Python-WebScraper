import json
import sqlite3
import logging
import requests
import dateparser
from .settings import DB_FILE
from datetime import datetime


class SQLitePipeline:

    def open_spider(self, spider):
        self.connection = sqlite3.connect(DB_FILE)
        self.cursor = self.connection.cursor()
        # self.create_table()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):

        if not item.get('Yad2ID', ''):
            return

        current_datetime = datetime.now().strftime("%m/%d/%y %H:%M %p")

        active = item.get('Active', False)
        if not active:
            self.cursor.execute('''
                        UPDATE properties
                        SET SEO_Title=?, 
                        Active=?,
                        DateInactive=?
                        WHERE Yad2ID=?
                    ''', (
                item.get('SEO_Title', ''),
                False,  # Placeholder value for 'Active',
                current_datetime,
                item.get('Yad2ID')
            ))

            self.connection.commit()

            logging.info("Data updated in Database.")

            payload = {
                "Yad2Id": item.get('Yad2ID'),
                "Active": False,
                "DateInactive": current_datetime,
                "SEO_Title": "Listing has been removed"
            }



        else:

            update_query = """
                UPDATE properties
                SET seo_title = ?, Status = ?, City = ?, AdType = ?, Neighborhood = ?, Street = ?, StreetNumber = ?, Latitude = ?, Longitude = ?,
                    PropertyType = ?, RoomsNumber = ?, FloorNumber = ?, FloorsInBuilding = ?, SizeInMeters = ?, TotalPrice = ?, Url = ?, Active = ?,
                    PriceperSquereMeter = ?, PropertyDescription = ?, AdvertiserName = ?, AdvertiserPhone = ?, FromBroker = ?,
                    Elevator = ?, AirConditioning = ?, parking = ?, balcony = ?, bars = ?, dimension = ?, warehouse = ?, HandicapAccessibility = ?,
                    PropertyMode = ?, DateOfEntry = ?, DatePublished = ?, DateUpdated = ?, DateScraped = ?, gallery_images = ?
                WHERE Yad2ID = ?;
            """
            self.cursor.execute(update_query, (
                item.get('seo_title'),
                item.get('Status'),
                item.get('City'),
                item.get('AdType'),
                item.get('Neighborhood'),
                item.get('Street'),
                item.get('StreetNumber'),
                item.get('Latitude'),
                item.get('Longitude'),
                item.get('PropertyType'),
                item.get('RoomsNumber'),
                item.get('FloorNumber'),
                item.get('FloorsInBuilding'),
                item.get('SizeInMeters'),
                item.get('TotalPrice'),
                item.get('Url'),
                item.get('Active'),
                item.get('PriceperSquereMeter'),
                item.get('PropertyDescription'),
                item.get('AdvertiserName'),
                item.get('AdvertiserPhone'),
                item.get('FromBroker'),
                item.get('Elevator'),
                item.get('AirConditioning'),
                item.get('parking'),
                item.get('balcony'),
                item.get('bars'),
                item.get('dimension'),
                item.get('warehouse'),
                item.get('HandicapAccessibility'),
                item.get('PropertyMode'),
                item.get('DateOfEntry'),
                item.get('DatePublished'),
                current_datetime,
                item.get('DateScraped'),
                json.dumps(item.get('gallery_images')),
                item.get('Yad2ID')
            ))
            self.connection.commit()

            date_scraped_str = item.get('DateScraped', '')

            if date_scraped_str:
                date_scraped = dateparser.parse(date_scraped_str)

                scraped_date = date_scraped.date().strftime('%Y-%m-%d')
                scraped_time = date_scraped.time().strftime('%H:%M:%S')
            else:
                scraped_date = scraped_time = ''
            payload = {
                "Yad2Id": item.get('Yad2ID', ''),
                "Street": item.get('Street', ''),
                "StreetNumber": item.get('StreetNumber', ''),
                "Latitude": item.get('Latitude', ''),
                "Longitude": item.get('Longitude', ''),
                "PropertyType": item.get('PropertyType', ''),
                "Rooms": item.get('RoomsNumber', ''),
                "Size": item.get('SizeInMeters', ''),
                "Floor": item.get('FloorNumber', ''),
                "FloorsInBuilding": item.get('FloorsInBuilding', ''),
                "TotalPrice": item.get('TotalPrice', ''),
                "PricePerSquareMeter": item.get('PriceperSquereMeter', ''),
                "Url": item.get('Url', ''),
                "Description": item.get('PropertyDescription', ''),
                "PublisherName": item.get('AdvertiserName', ''),
                "PublisherPhone": item.get('AdvertiserPhone', ''),
                "Status": item.get('Status', ''),
                "SEO_Index": item.get('SEO_Index', ''),
                "SEO_Slug": item.get('SEO_Slug', ''),
                "SEO_Title": item.get('seo_title', ''),
                "SEO_Description": item.get('SEO_Description', ''),
                "Social_title": item.get('Social_title', ''),
                "Social_Description": item.get('Social_Description', ''),
                "City": item.get('City', ''),
                "Neighborhood": item.get('Neighborhood', ''),
                "FromBroker": item.get('FromBroker', ''),
                "Elevator": item.get('Elevator', ''),
                "AirConditioning": item.get('AirConditioning', ''),
                "Parking": item.get('parking', ''),
                "Balcony": item.get('balcony', ''),
                "Bars": item.get('bars', ''),
                "Dimension": item.get('dimension', ''),
                "Warehouse": item.get('warehouse', ''),
                "Active": item.get('Active', ''),
                "HandicapAccessibility": item.get('HandicapAccessibility', ''),
                "Furniture": item.get('Furniture', ''),
                "Hood": item.get('Hood', ''),
                "Taxes": item.get('Taxes', ''),
                "FeaturedImage": item.get('featured_img_url', ''),
                "GalleryImages": item.get('gallery_images', {}),
                "DateOfEntry": item.get('DateOfEntry', ''),
                "DatePublished": item.get('DatePublished', ''),
                "DateScraped": item.get('DateScraped', ''),
                "DateUpdated": current_datetime,
                # "Date": item.get('Date', ''),
                "Date": scraped_date,
                # "Time": item.get('Time', ''),
                "Time": scraped_time,
                "AdType": item.get('AdType', '')
            }

        url = "https://brokerland.co.il/wp-json/bl-api/v2/listings"

        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Basic YWRtaW46eFFFdiAzQXFzIDhqQmQgRGpocCBTZDlyIGpta3U='
        }

        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            logging.info(f"Data sent to external server. {response.text}")
        else:
            logging.warning(
                f"Host db status code is {response.status_code}. Data couldn't be sent to external server.")

        return item
