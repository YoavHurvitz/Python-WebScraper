# Python-WebScraper

This project is a comprehensive Python-based web scraping tool designed to extract real estate listings data from Yad2.co.il and Madlan.co.il, two of Israel's largest real estate websites. It uses the Scrapy framework with ScrapingBee's proxy and IP rotation service to bypass anti-scraping measures and ensure successful data collection.
# Flow 
The scraper operates continuously, running 24/7 to capture new listings as they appear. It first checks for new listings by comparing scraped IDs with those already in the database, ensuring only fresh data is added. For each new listing, the scraper extracts detailed information including price, location, property features, and amenities. It then applies custom logic to determine if the listing is from a broker, based on factors such as the presence of a street number and specific listing characteristics. The extracted data is processed, sanitized, and then stored in a SQLite database. Simultaneously, the data is sent to an external API (Brokerland) for further analysis and integration. The system also generates timestamped CSV files for each scraping session, organizing them in a year/month/day folder structure for easy tracking and analysis. This workflow ensures a constant stream of up-to-date, de-duplicated real estate data for market analysis and business operations.

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
