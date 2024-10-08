import json
import logging
import sqlite3
import requests
import dateparser
from datetime import datetime


class DBPipeline:
    def __init__(self, db_file):
        self.db_file = db_file

    @classmethod
    def from_crawler(cls, crawler):
        db_file = crawler.settings.get('DB_FILE')
        return cls(db_file)

    def open_spider(self, spider):
        self.conn = sqlite3.connect(self.db_file)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):

        if not item.get('MadlanID'):
            return
        current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        active_status = item.get('Active')
        if not active_status:
            madlan_id = item.get("MadlanID", '')
            self.cursor.execute('''
                UPDATE Amazon_Listing
                SET SEO_Title=?, 
                Active=?,
                DateInactive=?
                WHERE MadlanID=?
            ''', (
                item.get('SEO_Title', ''),
                False,  # Placeholder value for 'Active',
                current_datetime,
                madlan_id  # MadlanID for WHERE clause
            ))

            self.conn.commit()

            logging.info("Data updated in Database.")

            payload = json.dumps({
                "MadlanID": item.get('MadlanID'),
                "Active": False,
                "DateInactive": current_datetime,
                "SEO_Title": "Listing has been removed"
            })


        else:
            madlan_id = item.get('MadlanID')
            try:
                self.cursor.execute('''
                    INSERT or replace INTO Amazon_Listing (Status, MadlanID, Street, StreetNumber, Latitude, Longitude, PropertyType, RoomsNumber, FloorNumber, FloorsInBuilding, SizeInMeters, TotalPrice, PricePerSquareMeter, Url, PropertyDescription, AdvertiserName, AdvertiserPhone, SEO_Title, City, Neighborhood, Elevator, FromBroker, AirConditioning, Parking, Balcony, Bars, Dimension, Warehouse, Active, HandicapAccessibility, Taxes, Furniture, DateOfEntry, Date, DateUpdated, featured_img_url, gallery_images)
                    VALUES (?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    item.get("Status", ''),
                    madlan_id,
                    item.get("Street", ''),
                    item.get("StreetNumber", ''),
                    item.get('Latitude', ''),
                    item.get('Longitude', ''),
                    item.get("PropertyType", ''),
                    item.get("RoomsNumber", ''),
                    item.get("FloorNumber", ''),
                    item.get("FloorsInBuilding", ''),
                    item.get("SizeInMeters", ''),
                    item.get("TotalPrice", ''),
                    item.get("PricePerSquareMeter", ''),
                    item.get("Url", ''),
                    item.get("PropertyDescription", ''),
                    item.get("AdvertiserName", ''),
                    item.get("AdvertiserPhone", ''),
                    item.get('SEO_Title', ''),
                    item.get("City", ''),
                    item.get("Neighborhood", ''),
                    item.get("Elevator", ''),
                    item.get("FromBroker", False),
                    item.get("AirConditioning", ''),
                    item.get("Parking", ''),
                    item.get("Balcony", ''),
                    item.get("Bars", ''),
                    item.get("Dimension", ''),
                    item.get("Warehouse", ''),
                    True,  # Placeholder value for 'Active'
                    item.get("HandicapAccessibility", ''),
                    item.get("Taxes", ''),
                    item.get("Furniture", ''),
                    item.get("DateOfEntry", ''),
                    current_datetime,  # Placeholder value for 'Date'
                    current_datetime,
                    "https://domain.com/image.png",  # Placeholder value for 'featured_img_url'
                    json.dumps(item.get("gallery_images", {}))  # Serialize gallery_images dictionary to JSON
                ))

                # Commit changes to the database
                self.conn.commit()
            except Exception as e:
                logging.info(e)
            # Log insertion status
            logging.info("Data inserted or updated in Database.")

            date_scraped_str = item.get('DateScraped', '')

            date_scraped_str = item.get('DateScraped', '')

            if date_scraped_str:
                date_scraped = dateparser.parse(date_scraped_str)

                scraped_date = date_scraped.date().strftime('%Y-%m-%d')
                scraped_time = date_scraped.time().strftime('%H:%M:%S')
            else:
                scraped_date = scraped_time = ''

            payload = json.dumps({
                "MadlanID": item.get('MadlanID', ''),
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
                "FromBroker": item.get('FromBroker', False),
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

            })

        url = "https://brokerland.co.il/wp-json/bl-api/v2/listings"

        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Basic YWRtaW46eFFFdiAzQXFzIDhqQmQgRGpocCBTZDlyIGpta3U='
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            logging.info("Data sent to external server.")
        else:
            logging.warning(
                f"Host db status code is {response.status_code}. Data couldn't be sent to external server.")

        return item

    def close_spider(self, spider):

        self.conn.close()
