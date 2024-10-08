# Python-WebScraper

This project is a comprehensive Python-based web scraping tool designed to extract real estate listings data from Yad2.co.il and Madlan.co.il, two of Israel's largest real estate websites. It uses the Scrapy framework with ScrapingBee's proxy and IP rotation service to bypass anti-scraping measures and ensure successful data collection.

# Features
-Scrapes both rental and sale listings from Madlan.co.il and Yad2.co.il
-Uses ScrapingBee for IP rotation and proxy services
-Implements logic to determine if a listing is from a broker
-Stores scraped data in a SQLite database
-Sends scraped data to an external API (Brokerland)
-Extracts detailed information including price, location, amenities, and more
-Handles pagination to scrape large volumes of listings
-Updates existing listings and adds new ones

# Requirements
-Python 3.x
-Scrapy
-ScrapingBee account and API key
-Some database
-Additional Python packages: dateparser, requests

# Data Storage
-The scraped data is stored in a SQLite database.
-The data is also sent to an external API (Brokerland) for further processing or storage.
