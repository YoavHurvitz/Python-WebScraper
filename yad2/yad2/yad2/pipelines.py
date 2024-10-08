import datetime
import json
import sqlite3
import logging
import requests
import dateparser
from .settings import DB_FILE


class SQLitePipeline:

    def open_spider(self, spider):
        self.connection = sqlite3.connect(DB_FILE)
        self.cursor = self.connection.cursor()
        self.create_table()

    def close_spider(self, spider):
        self.connection.close()

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            seo_title TEXT,
            Yad2ID TEXT,
            Status TEXT,
            City TEXT,
            AdType TEXT,
            Neighborhood TEXT,
            Street TEXT,
            StreetNumber TEXT,
            Latitude REAL,
            Longitude REAL,
            PropertyType TEXT,
            RoomsNumber INTEGER,
            FloorNumber INTEGER,
            FloorsInBuilding INTEGER,
            SizeInMeters REAL,
            TotalPrice REAL,
            Url TEXT,
            Active BOOLEAN,
            PriceperSquereMeter REAL,
            PropertyDescription TEXT,
            AdvertiserName TEXT,
            AdvertiserPhone TEXT,
            FromBroker TEXT,
            Elevator BOOLEAN,
            AirConditioning BOOLEAN,
            parking BOOLEAN,
            balcony BOOLEAN,
            bars BOOLEAN,
            dimension BOOLEAN,
            warehouse BOOLEAN,
            HandicapAccessibility BOOLEAN,
            PropertyMode TEXT,
            DateOfEntry TEXT,
            DatePublished TEXT,
            DateUpdated TEXT,
            DateScraped TEXT,
            gallery_images TEXT
        );
        """
        self.cursor.execute(create_table_query)
        self.connection.commit()

    def process_item(self, item, spider):
        if not item.get('Yad2ID', ''):
            return
        current_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        insert_query = """
        INSERT INTO properties (
            seo_title, Yad2ID, Status, City, AdType, Neighborhood, Street, StreetNumber, Latitude, Longitude,
            PropertyType, RoomsNumber, FloorNumber, FloorsInBuilding, SizeInMeters, TotalPrice, Url, Active,
            PriceperSquereMeter, PropertyDescription, AdvertiserName, AdvertiserPhone, FromBroker,
            Elevator, AirConditioning, parking, balcony, bars, dimension, warehouse, HandicapAccessibility, 
            PropertyMode, DateOfEntry,DatePublished,DateUpdated, DateScraped, gallery_images
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        self.cursor.execute(insert_query, (
            item.get('seo_title'),
            item.get('Yad2ID'),
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
            # item.get('FromBrokerCRM'),
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
            json.dumps(item.get('gallery_images'))
        ))
        self.connection.commit()

        date_scraped_str = item.get('DateScraped', '')

        if date_scraped_str:
            if date_scraped_str:
                date_scraped = dateparser.parse(date_scraped_str)

                scraped_date = date_scraped.date().strftime('%Y-%m-%d')
                scraped_time = date_scraped.time().strftime('%H:%M:%S')
            else:
                scraped_date = scraped_time = ''
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
            logging.info("Data sent to external server.")
        else:
            logging.warning(f"Host db status code is {response.status_code}. Data couldn't be sent to external server.")

        return item
