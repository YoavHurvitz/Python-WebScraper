import scrapy
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
    name = "madlan"
    listing_ids = set()

    def __init__(self, *args, **kwargs):
        super(MadlanScraper, self).__init__(*args, **kwargs)
        try:
            self.conn = sqlite3.connect(DB_FILE)
            self.cursor = self.conn.cursor()
            # self.cursor.execute('SELECT MadlanID FROM Amazon_Listing WHERE ACTIVE=1')
            self.cursor.execute('SELECT MadlanID FROM Amazon_Listing')
            idx = [row[0] for row in self.cursor.fetchall()]
            self.listing_ids = set(idx)
        except:
            self.listing_ids = set()

    def start_requests(self):

        body = '{"operationName":"searchPoi","variables":{"noFee":false,"dealType":"unitRent","numberOfEmployeesRange":[null,null],"commercialAmenities":{},"qualityClass":[],"roomsRange":[null,null],"bathsRange":[null,null],"floorRange":[null,null],"areaRange":[null,null],"buildingClass":[],"sellerType":[],"generalCondition":[],"ppmRange":[null,null],"priceRange":[null,null],"monthlyTaxRange":[null,null],"amenities":{},"sort":[{"field":"geometry","order":"asc","reference":null,"docIds":["ישראל"]}],"priceDrop":false,"underPriceEstimation":false,"isCommercialRealEstate":false,"userContext":null,"tileRanges":[{"x1":156024,"y1":105299,"x2":157211,"y2":108584}],"poiTypes":["bulletin","project"],"searchContext":"marketplace","cursor":{"seenProjects":["עזריאלי_מודיעין","מתחם קופת חולים - עזריאלי"],"bulletinsOffset":48},"offset":0,"limit":50,"abtests":{"_be_sortMarketplaceByDate":"modeA","_be_sortMarketplaceAgeWeight":"modeA","_be_sortMarketplaceByHasAtLeastOneImage":"modeA"}},"query":"query searchPoi($dealType: String, $userContext: JSONObject, $abtests: JSONObject, $noFee: Boolean, $priceRange: [Int], $ppmRange: [Int], $monthlyTaxRange: [Int], $roomsRange: [Int], $bathsRange: [Float], $buildingClass: [String], $amenities: inputAmenitiesFilter, $generalCondition: [GeneralCondition], $sellerType: [SellerType], $floorRange: [Int], $areaRange: [Int], $tileRanges: [TileRange], $tileRangesExcl: [TileRange], $sort: [SortField], $limit: Int, $offset: Int, $cursor: inputCursor, $poiTypes: [PoiType], $locationDocId: String, $abtests: JSONObject, $searchContext: SearchContext, $underPriceEstimation: Boolean, $priceDrop: Boolean, $isCommercialRealEstate: Boolean, $commercialAmenities: inputCommercialAmenitiesFilter, $qualityClass: [String], $numberOfEmployeesRange: [Float], $creationDaysRange: Int) {\n  searchPoiV2(noFee: $noFee, dealType: $dealType, userContext: $userContext, abtests: $abtests, priceRange: $priceRange, ppmRange: $ppmRange, monthlyTaxRange: $monthlyTaxRange, roomsRange: $roomsRange, bathsRange: $bathsRange, buildingClass: $buildingClass, sellerType: $sellerType, floorRange: $floorRange, areaRange: $areaRange, generalCondition: $generalCondition, amenities: $amenities, tileRanges: $tileRanges, tileRangesExcl: $tileRangesExcl, sort: $sort, limit: $limit, offset: $offset, cursor: $cursor, poiTypes: $poiTypes, locationDocId: $locationDocId, abtests: $abtests, searchContext: $searchContext, underPriceEstimation: $underPriceEstimation, priceDrop: $priceDrop, isCommercialRealEstate: $isCommercialRealEstate, commercialAmenities: $commercialAmenities, qualityClass: $qualityClass, numberOfEmployeesRange: $numberOfEmployeesRange, creationDaysRange: $creationDaysRange) {\n    total\n    cursor {\n      bulletinsOffset\n      projectsOffset\n      seenProjects\n      __typename\n    }\n    totalNearby\n    lastInGeometryId\n    cursor {\n      bulletinsOffset\n      projectsOffset\n      __typename\n    }\n    ...PoiFragment\n    __typename\n  }\n}\n\nfragment PoiFragment on PoiSearchResult {\n  poi {\n    ...PoiInner\n    ... on Bulletin {\n      rentalBrokerFee\n      eventsHistory {\n        eventType\n        price\n        date\n        __typename\n      }\n      insights {\n        insights {\n          category\n          tradeoff {\n            insightPlace\n            value\n            tagLine\n            impactful\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment PoiInner on Poi {\n  id\n  locationPoint {\n    lat\n    lng\n    __typename\n  }\n  type\n  firstTimeSeen\n  addressDetails {\n    docId\n    city\n    borough\n    zipcode\n    streetName\n    neighbourhood\n    neighbourhoodDocId\n    cityDocId\n    resolutionPreferences\n    streetNumber\n    unitNumber\n    district\n    __typename\n  }\n  ... on Project {\n    discount {\n      showDiscount\n      description\n      bannerUrl\n      __typename\n    }\n    dealType\n    phoneNumber\n    apartmentType {\n      size\n      beds\n      apartmentSpecification\n      type\n      price\n      __typename\n    }\n    bedsRange {\n      min\n      max\n      __typename\n    }\n    priceRange {\n      min\n      max\n      __typename\n    }\n    images {\n      path\n      __typename\n    }\n    promotionStatus {\n      status\n      __typename\n    }\n    projectName\n    projectLogo\n    isCommercial\n    projectMessages {\n      listingDescription\n      __typename\n    }\n    previewImage {\n      path\n      __typename\n    }\n    developers {\n      id\n      logoPath\n      __typename\n    }\n    tags {\n      bestSchool\n      bestSecular\n      bestReligious\n      safety\n      parkAccess\n      quietStreet\n      dogPark\n      familyFriendly\n      lightRail\n      commute\n      __typename\n    }\n    buildingStage\n    blockDetails {\n      buildingsNum\n      floorRange {\n        min\n        max\n        __typename\n      }\n      units\n      mishtakenPrice\n      urbanRenewal\n      __typename\n    }\n    __typename\n  }\n  ... on CommercialBulletin {\n    address\n    agentId\n    qualityClass\n    amenities {\n      accessible\n      airConditioner\n      alarm\n      conferenceRoom\n      doorman\n      elevator\n      fullTimeAccess\n      kitchenette\n      outerSpace\n      parkingBikes\n      parkingEmployee\n      parkingVisitors\n      reception\n      secureRoom\n      storage\n      subDivisible\n      __typename\n    }\n    area\n    availabilityType\n    availableDate\n    balconyArea\n    buildingClass\n    buildingType\n    buildingYear\n    currency\n    dealType\n    description\n    estimatedPrice\n    lastUpdated\n    eventsHistory {\n      eventType\n      price\n      date\n      __typename\n    }\n    feeType\n    floor\n    floors\n    fromDateTime\n    furnitureDetails\n    generalCondition\n    images {\n      ...ImageItem\n      __typename\n    }\n    lastActiveMarkDate\n    leaseTerm\n    leaseType\n    matchScore\n    monthlyTaxes\n    newListing\n    numberOfEmployees\n    originalId\n    poc {\n      type\n      ... on BulletinAgent {\n        madadSearchResult\n        officeId\n        officeContact {\n          imageUrl\n          __typename\n        }\n        exclusivity {\n          exclusive\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    ppm\n    price\n    qualityBin\n    rentalBrokerFee\n    rooms\n    source\n    status {\n      promoted\n      __typename\n    }\n    url\n    virtualTours\n    __typename\n  }\n  ... on Bulletin {\n    dealType\n    address\n    matchScore\n    beds\n    floor\n    baths\n    buildingYear\n    area\n    price\n    virtualTours\n    rentalBrokerFee\n    generalCondition\n    lastUpdated\n    eventsHistory {\n      eventType\n      price\n      date\n      __typename\n    }\n    status {\n      promoted\n      __typename\n    }\n    poc {\n      type\n      ... on BulletinAgent {\n        madadSearchResult\n        officeId\n        officeContact {\n          imageUrl\n          __typename\n        }\n        exclusivity {\n          exclusive\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    tags {\n      bestSchool\n      bestSecular\n      bestReligious\n      safety\n      parkAccess\n      quietStreet\n      dogPark\n      familyFriendly\n      lightRail\n      commute\n      __typename\n    }\n    commuteTime\n    dogsParkWalkTime\n    parkWalkTime\n    buildingClass\n    images {\n      ...ImageItem\n      __typename\n    }\n    __typename\n  }\n  ... on Ad {\n    addressDetails {\n      docId\n      city\n      borough\n      zipcode\n      streetName\n      neighbourhood\n      neighbourhoodDocId\n      resolutionPreferences\n      streetNumber\n      unitNumber\n      __typename\n    }\n    city\n    district\n    firstTimeSeen\n    id\n    locationPoint {\n      lat\n      lng\n      __typename\n    }\n    neighbourhood\n    type\n    __typename\n  }\n  __typename\n}\n\nfragment ImageItem on ImageItem {\n  description\n  imageUrl\n  isFloorplan\n  rotation\n  __typename\n}\n"}'

        yield scrapy.Request(
            url=api_url,
            method='POST',
            dont_filter=True,
            headers=headers,
            body=body,
            meta={'type': 'rent', 'first': True}
        )
        body = '{"operationName":"searchPoi","variables":{"noFee":false,"dealType":"unitBuy","numberOfEmployeesRange":[null,null],"commercialAmenities":{},"qualityClass":[],"roomsRange":[null,null],"bathsRange":[null,null],"floorRange":[null,null],"areaRange":[null,null],"buildingClass":[],"sellerType":[],"generalCondition":[],"ppmRange":[null,null],"priceRange":[null,null],"monthlyTaxRange":[null,null],"amenities":{},"sort":[{"field":"geometry","order":"asc","reference":null,"docIds":["ישראל"]}],"priceDrop":false,"underPriceEstimation":false,"isCommercialRealEstate":false,"userContext":null,"tileRanges":[{"x1":156024,"y1":105299,"x2":157211,"y2":108584}],"poiTypes":["bulletin","project"],"searchContext":"marketplace","cursor":{"seenProjects":["גולומב_5_רמת_השרון","בלוך_30_תל_אביב_יפו","קבוצת_רכישה_14_בת_ים","חצור_15_רמת_גן","פארק_צפון","ויתקין_יוסף_16_חיפה","אכזיב","ברנדיס_6_תל_אביב","רמז_דוד_44_תל_אביב","RAMBAM_36_RAANANA","רמז__19_תל_אביב_יפו","פומבדיתא_20_22_תל_אביב","רמז_25_תל_אביב","רמז_21_תל_אביב_יפו","בלוך_דוד_11_תל_אביב_יפו","חברה_חדשה_3_תל_אביב"],"bulletinsOffset":34},"offset":0,"limit":50,"abtests":{"_be_sortMarketplaceByDate":"modeA","_be_sortMarketplaceAgeWeight":"modeA","_be_sortMarketplaceByHasAtLeastOneImage":"modeA"}},"query":"query searchPoi($dealType: String, $userContext: JSONObject, $abtests: JSONObject, $noFee: Boolean, $priceRange: [Int], $ppmRange: [Int], $monthlyTaxRange: [Int], $roomsRange: [Int], $bathsRange: [Float], $buildingClass: [String], $amenities: inputAmenitiesFilter, $generalCondition: [GeneralCondition], $sellerType: [SellerType], $floorRange: [Int], $areaRange: [Int], $tileRanges: [TileRange], $tileRangesExcl: [TileRange], $sort: [SortField], $limit: Int, $offset: Int, $cursor: inputCursor, $poiTypes: [PoiType], $locationDocId: String, $abtests: JSONObject, $searchContext: SearchContext, $underPriceEstimation: Boolean, $priceDrop: Boolean, $isCommercialRealEstate: Boolean, $commercialAmenities: inputCommercialAmenitiesFilter, $qualityClass: [String], $numberOfEmployeesRange: [Float], $creationDaysRange: Int) {\n  searchPoiV2(noFee: $noFee, dealType: $dealType, userContext: $userContext, abtests: $abtests, priceRange: $priceRange, ppmRange: $ppmRange, monthlyTaxRange: $monthlyTaxRange, roomsRange: $roomsRange, bathsRange: $bathsRange, buildingClass: $buildingClass, sellerType: $sellerType, floorRange: $floorRange, areaRange: $areaRange, generalCondition: $generalCondition, amenities: $amenities, tileRanges: $tileRanges, tileRangesExcl: $tileRangesExcl, sort: $sort, limit: $limit, offset: $offset, cursor: $cursor, poiTypes: $poiTypes, locationDocId: $locationDocId, abtests: $abtests, searchContext: $searchContext, underPriceEstimation: $underPriceEstimation, priceDrop: $priceDrop, isCommercialRealEstate: $isCommercialRealEstate, commercialAmenities: $commercialAmenities, qualityClass: $qualityClass, numberOfEmployeesRange: $numberOfEmployeesRange, creationDaysRange: $creationDaysRange) {\n    total\n    cursor {\n      bulletinsOffset\n      projectsOffset\n      seenProjects\n      __typename\n    }\n    totalNearby\n    lastInGeometryId\n    cursor {\n      bulletinsOffset\n      projectsOffset\n      __typename\n    }\n    ...PoiFragment\n    __typename\n  }\n}\n\nfragment PoiFragment on PoiSearchResult {\n  poi {\n    ...PoiInner\n    ... on Bulletin {\n      rentalBrokerFee\n      eventsHistory {\n        eventType\n        price\n        date\n        __typename\n      }\n      insights {\n        insights {\n          category\n          tradeoff {\n            insightPlace\n            value\n            tagLine\n            impactful\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment PoiInner on Poi {\n  id\n  locationPoint {\n    lat\n    lng\n    __typename\n  }\n  type\n  firstTimeSeen\n  addressDetails {\n    docId\n    city\n    borough\n    zipcode\n    streetName\n    neighbourhood\n    neighbourhoodDocId\n    cityDocId\n    resolutionPreferences\n    streetNumber\n    unitNumber\n    district\n    __typename\n  }\n  ... on Project {\n    discount {\n      showDiscount\n      description\n      bannerUrl\n      __typename\n    }\n    dealType\n    phoneNumber\n    apartmentType {\n      size\n      beds\n      apartmentSpecification\n      type\n      price\n      __typename\n    }\n    bedsRange {\n      min\n      max\n      __typename\n    }\n    priceRange {\n      min\n      max\n      __typename\n    }\n    images {\n      path\n      __typename\n    }\n    promotionStatus {\n      status\n      __typename\n    }\n    projectName\n    projectLogo\n    isCommercial\n    projectMessages {\n      listingDescription\n      __typename\n    }\n    previewImage {\n      path\n      __typename\n    }\n    developers {\n      id\n      logoPath\n      __typename\n    }\n    tags {\n      bestSchool\n      bestSecular\n      bestReligious\n      safety\n      parkAccess\n      quietStreet\n      dogPark\n      familyFriendly\n      lightRail\n      commute\n      __typename\n    }\n    buildingStage\n    blockDetails {\n      buildingsNum\n      floorRange {\n        min\n        max\n        __typename\n      }\n      units\n      mishtakenPrice\n      urbanRenewal\n      __typename\n    }\n    __typename\n  }\n  ... on CommercialBulletin {\n    address\n    agentId\n    qualityClass\n    amenities {\n      accessible\n      airConditioner\n      alarm\n      conferenceRoom\n      doorman\n      elevator\n      fullTimeAccess\n      kitchenette\n      outerSpace\n      parkingBikes\n      parkingEmployee\n      parkingVisitors\n      reception\n      secureRoom\n      storage\n      subDivisible\n      __typename\n    }\n    area\n    availabilityType\n    availableDate\n    balconyArea\n    buildingClass\n    buildingType\n    buildingYear\n    currency\n    dealType\n    description\n    estimatedPrice\n    lastUpdated\n    eventsHistory {\n      eventType\n      price\n      date\n      __typename\n    }\n    feeType\n    floor\n    floors\n    fromDateTime\n    furnitureDetails\n    generalCondition\n    images {\n      ...ImageItem\n      __typename\n    }\n    lastActiveMarkDate\n    leaseTerm\n    leaseType\n    matchScore\n    monthlyTaxes\n    newListing\n    numberOfEmployees\n    originalId\n    poc {\n      type\n      ... on BulletinAgent {\n        madadSearchResult\n        officeId\n        officeContact {\n          imageUrl\n          __typename\n        }\n        exclusivity {\n          exclusive\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    ppm\n    price\n    qualityBin\n    rentalBrokerFee\n    rooms\n    source\n    status {\n      promoted\n      __typename\n    }\n    url\n    virtualTours\n    __typename\n  }\n  ... on Bulletin {\n    dealType\n    address\n    matchScore\n    beds\n    floor\n    baths\n    buildingYear\n    area\n    price\n    virtualTours\n    rentalBrokerFee\n    generalCondition\n    lastUpdated\n    eventsHistory {\n      eventType\n      price\n      date\n      __typename\n    }\n    status {\n      promoted\n      __typename\n    }\n    poc {\n      type\n      ... on BulletinAgent {\n        madadSearchResult\n        officeId\n        officeContact {\n          imageUrl\n          __typename\n        }\n        exclusivity {\n          exclusive\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    tags {\n      bestSchool\n      bestSecular\n      bestReligious\n      safety\n      parkAccess\n      quietStreet\n      dogPark\n      familyFriendly\n      lightRail\n      commute\n      __typename\n    }\n    commuteTime\n    dogsParkWalkTime\n    parkWalkTime\n    buildingClass\n    images {\n      ...ImageItem\n      __typename\n    }\n    __typename\n  }\n  ... on Ad {\n    addressDetails {\n      docId\n      city\n      borough\n      zipcode\n      streetName\n      neighbourhood\n      neighbourhoodDocId\n      resolutionPreferences\n      streetNumber\n      unitNumber\n      __typename\n    }\n    city\n    district\n    firstTimeSeen\n    id\n    locationPoint {\n      lat\n      lng\n      __typename\n    }\n    neighbourhood\n    type\n    __typename\n  }\n  __typename\n}\n\nfragment ImageItem on ImageItem {\n  description\n  imageUrl\n  isFloorplan\n  rotation\n  __typename\n}\n"}'

        yield scrapy.Request(
            url=api_url,
            method='POST',
            dont_filter=True,
            headers=headers,
            body=body,
            meta={'type': 'buy', 'first': True}
        )

    def parse(self, response, **kwargs):
        data = response.json()
        _type = response.meta.get('type')
        total = data['data']['searchPoiV2']['total']
        if response.meta.get('first'):
            if _type == "rent":
                for i in range(0, total + 1000, 50):
                    # for i in range(0, 2500, 50):
                    # for i in range(0, 500, 1000):
                    body = '{"operationName":"searchPoi","variables":{"noFee":false,"dealType":"unitRent","numberOfEmployeesRange":[null,null],"commercialAmenities":{},"qualityClass":[],"roomsRange":[null,null],"bathsRange":[null,null],"floorRange":[null,null],"areaRange":[null,null],"buildingClass":[],"sellerType":[],"generalCondition":[],"ppmRange":[null,null],"priceRange":[null,null],"monthlyTaxRange":[null,null],"amenities":{},"sort":[{"field":"geometry","order":"asc","reference":null,"docIds":["ישראל"]}],"priceDrop":false,"underPriceEstimation":false,"isCommercialRealEstate":false,"userContext":null,"tileRanges":[{"x1":156024,"y1":105299,"x2":157211,"y2":108584}],"poiTypes":["bulletin","project"],"searchContext":"marketplace","cursor":{"seenProjects":["עזריאלי_מודיעין","מתחם קופת חולים - עזריאלי"],"bulletinsOffset":48},"offset":' + str(
                        i) + ',"limit":50,"abtests":{"_be_sortMarketplaceByDate":"modeA","_be_sortMarketplaceAgeWeight":"modeA","_be_sortMarketplaceByHasAtLeastOneImage":"modeA"}},"query":"query searchPoi($dealType: String, $userContext: JSONObject, $abtests: JSONObject, $noFee: Boolean, $priceRange: [Int], $ppmRange: [Int], $monthlyTaxRange: [Int], $roomsRange: [Int], $bathsRange: [Float], $buildingClass: [String], $amenities: inputAmenitiesFilter, $generalCondition: [GeneralCondition], $sellerType: [SellerType], $floorRange: [Int], $areaRange: [Int], $tileRanges: [TileRange], $tileRangesExcl: [TileRange], $sort: [SortField], $limit: Int, $offset: Int, $cursor: inputCursor, $poiTypes: [PoiType], $locationDocId: String, $abtests: JSONObject, $searchContext: SearchContext, $underPriceEstimation: Boolean, $priceDrop: Boolean, $isCommercialRealEstate: Boolean, $commercialAmenities: inputCommercialAmenitiesFilter, $qualityClass: [String], $numberOfEmployeesRange: [Float], $creationDaysRange: Int) {\n  searchPoiV2(noFee: $noFee, dealType: $dealType, userContext: $userContext, abtests: $abtests, priceRange: $priceRange, ppmRange: $ppmRange, monthlyTaxRange: $monthlyTaxRange, roomsRange: $roomsRange, bathsRange: $bathsRange, buildingClass: $buildingClass, sellerType: $sellerType, floorRange: $floorRange, areaRange: $areaRange, generalCondition: $generalCondition, amenities: $amenities, tileRanges: $tileRanges, tileRangesExcl: $tileRangesExcl, sort: $sort, limit: $limit, offset: $offset, cursor: $cursor, poiTypes: $poiTypes, locationDocId: $locationDocId, abtests: $abtests, searchContext: $searchContext, underPriceEstimation: $underPriceEstimation, priceDrop: $priceDrop, isCommercialRealEstate: $isCommercialRealEstate, commercialAmenities: $commercialAmenities, qualityClass: $qualityClass, numberOfEmployeesRange: $numberOfEmployeesRange, creationDaysRange: $creationDaysRange) {\n    total\n    cursor {\n      bulletinsOffset\n      projectsOffset\n      seenProjects\n      __typename\n    }\n    totalNearby\n    lastInGeometryId\n    cursor {\n      bulletinsOffset\n      projectsOffset\n      __typename\n    }\n    ...PoiFragment\n    __typename\n  }\n}\n\nfragment PoiFragment on PoiSearchResult {\n  poi {\n    ...PoiInner\n    ... on Bulletin {\n      rentalBrokerFee\n      eventsHistory {\n        eventType\n        price\n        date\n        __typename\n      }\n      insights {\n        insights {\n          category\n          tradeoff {\n            insightPlace\n            value\n            tagLine\n            impactful\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment PoiInner on Poi {\n  id\n  locationPoint {\n    lat\n    lng\n    __typename\n  }\n  type\n  firstTimeSeen\n  addressDetails {\n    docId\n    city\n    borough\n    zipcode\n    streetName\n    neighbourhood\n    neighbourhoodDocId\n    cityDocId\n    resolutionPreferences\n    streetNumber\n    unitNumber\n    district\n    __typename\n  }\n  ... on Project {\n    discount {\n      showDiscount\n      description\n      bannerUrl\n      __typename\n    }\n    dealType\n    phoneNumber\n    apartmentType {\n      size\n      beds\n      apartmentSpecification\n      type\n      price\n      __typename\n    }\n    bedsRange {\n      min\n      max\n      __typename\n    }\n    priceRange {\n      min\n      max\n      __typename\n    }\n    images {\n      path\n      __typename\n    }\n    promotionStatus {\n      status\n      __typename\n    }\n    projectName\n    projectLogo\n    isCommercial\n    projectMessages {\n      listingDescription\n      __typename\n    }\n    previewImage {\n      path\n      __typename\n    }\n    developers {\n      id\n      logoPath\n      __typename\n    }\n    tags {\n      bestSchool\n      bestSecular\n      bestReligious\n      safety\n      parkAccess\n      quietStreet\n      dogPark\n      familyFriendly\n      lightRail\n      commute\n      __typename\n    }\n    buildingStage\n    blockDetails {\n      buildingsNum\n      floorRange {\n        min\n        max\n        __typename\n      }\n      units\n      mishtakenPrice\n      urbanRenewal\n      __typename\n    }\n    __typename\n  }\n  ... on CommercialBulletin {\n    address\n    agentId\n    qualityClass\n    amenities {\n      accessible\n      airConditioner\n      alarm\n      conferenceRoom\n      doorman\n      elevator\n      fullTimeAccess\n      kitchenette\n      outerSpace\n      parkingBikes\n      parkingEmployee\n      parkingVisitors\n      reception\n      secureRoom\n      storage\n      subDivisible\n      __typename\n    }\n    area\n    availabilityType\n    availableDate\n    balconyArea\n    buildingClass\n    buildingType\n    buildingYear\n    currency\n    dealType\n    description\n    estimatedPrice\n    lastUpdated\n    eventsHistory {\n      eventType\n      price\n      date\n      __typename\n    }\n    feeType\n    floor\n    floors\n    fromDateTime\n    furnitureDetails\n    generalCondition\n    images {\n      ...ImageItem\n      __typename\n    }\n    lastActiveMarkDate\n    leaseTerm\n    leaseType\n    matchScore\n    monthlyTaxes\n    newListing\n    numberOfEmployees\n    originalId\n    poc {\n      type\n      ... on BulletinAgent {\n        madadSearchResult\n        officeId\n        officeContact {\n          imageUrl\n          __typename\n        }\n        exclusivity {\n          exclusive\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    ppm\n    price\n    qualityBin\n    rentalBrokerFee\n    rooms\n    source\n    status {\n      promoted\n      __typename\n    }\n    url\n    virtualTours\n    __typename\n  }\n  ... on Bulletin {\n    dealType\n    address\n    matchScore\n    beds\n    floor\n    baths\n    buildingYear\n    area\n    price\n    virtualTours\n    rentalBrokerFee\n    generalCondition\n    lastUpdated\n    eventsHistory {\n      eventType\n      price\n      date\n      __typename\n    }\n    status {\n      promoted\n      __typename\n    }\n    poc {\n      type\n      ... on BulletinAgent {\n        madadSearchResult\n        officeId\n        officeContact {\n          imageUrl\n          __typename\n        }\n        exclusivity {\n          exclusive\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    tags {\n      bestSchool\n      bestSecular\n      bestReligious\n      safety\n      parkAccess\n      quietStreet\n      dogPark\n      familyFriendly\n      lightRail\n      commute\n      __typename\n    }\n    commuteTime\n    dogsParkWalkTime\n    parkWalkTime\n    buildingClass\n    images {\n      ...ImageItem\n      __typename\n    }\n    __typename\n  }\n  ... on Ad {\n    addressDetails {\n      docId\n      city\n      borough\n      zipcode\n      streetName\n      neighbourhood\n      neighbourhoodDocId\n      resolutionPreferences\n      streetNumber\n      unitNumber\n      __typename\n    }\n    city\n    district\n    firstTimeSeen\n    id\n    locationPoint {\n      lat\n      lng\n      __typename\n    }\n    neighbourhood\n    type\n    __typename\n  }\n  __typename\n}\n\nfragment ImageItem on ImageItem {\n  description\n  imageUrl\n  isFloorplan\n  rotation\n  __typename\n}\n"}'
                    yield scrapy.Request(
                        url=api_url,
                        method='POST',
                        dont_filter=True,
                        cookies=cookies,
                        headers=headers,
                        body=body,
                        meta={'type': 'rent'}
                    )
            if _type == "buy":
                for i in range(0, total + 1000, 50):
                    # for i in range(0, 2500, 50):
                    # for i in range(0, 500, 1000):
                    body = '{"operationName":"searchPoi","variables":{"noFee":false,"dealType":"unitBuy","numberOfEmployeesRange":[null,null],"commercialAmenities":{},"qualityClass":[],"roomsRange":[null,null],"bathsRange":[null,null],"floorRange":[null,null],"areaRange":[null,null],"buildingClass":[],"sellerType":[],"generalCondition":[],"ppmRange":[null,null],"priceRange":[null,null],"monthlyTaxRange":[null,null],"amenities":{},"sort":[{"field":"geometry","order":"asc","reference":null,"docIds":["ישראל"]}],"priceDrop":false,"underPriceEstimation":false,"isCommercialRealEstate":false,"userContext":null,"tileRanges":[{"x1":156024,"y1":105299,"x2":157211,"y2":108584}],"poiTypes":["bulletin","project"],"searchContext":"marketplace","cursor":{"seenProjects":["גולומב_5_רמת_השרון","בלוך_30_תל_אביב_יפו","קבוצת_רכישה_14_בת_ים","חצור_15_רמת_גן","פארק_צפון","ויתקין_יוסף_16_חיפה","אכזיב","ברנדיס_6_תל_אביב","רמז_דוד_44_תל_אביב","RAMBAM_36_RAANANA","רמז__19_תל_אביב_יפו","פומבדיתא_20_22_תל_אביב","רמז_25_תל_אביב","רמז_21_תל_אביב_יפו","בלוך_דוד_11_תל_אביב_יפו","חברה_חדשה_3_תל_אביב"],"bulletinsOffset":34},"offset":' + str(
                        i) + ',"limit":50,"abtests":{"_be_sortMarketplaceByDate":"modeA","_be_sortMarketplaceAgeWeight":"modeA","_be_sortMarketplaceByHasAtLeastOneImage":"modeA"}},"query":"query searchPoi($dealType: String, $userContext: JSONObject, $abtests: JSONObject, $noFee: Boolean, $priceRange: [Int], $ppmRange: [Int], $monthlyTaxRange: [Int], $roomsRange: [Int], $bathsRange: [Float], $buildingClass: [String], $amenities: inputAmenitiesFilter, $generalCondition: [GeneralCondition], $sellerType: [SellerType], $floorRange: [Int], $areaRange: [Int], $tileRanges: [TileRange], $tileRangesExcl: [TileRange], $sort: [SortField], $limit: Int, $offset: Int, $cursor: inputCursor, $poiTypes: [PoiType], $locationDocId: String, $abtests: JSONObject, $searchContext: SearchContext, $underPriceEstimation: Boolean, $priceDrop: Boolean, $isCommercialRealEstate: Boolean, $commercialAmenities: inputCommercialAmenitiesFilter, $qualityClass: [String], $numberOfEmployeesRange: [Float], $creationDaysRange: Int) {\n  searchPoiV2(noFee: $noFee, dealType: $dealType, userContext: $userContext, abtests: $abtests, priceRange: $priceRange, ppmRange: $ppmRange, monthlyTaxRange: $monthlyTaxRange, roomsRange: $roomsRange, bathsRange: $bathsRange, buildingClass: $buildingClass, sellerType: $sellerType, floorRange: $floorRange, areaRange: $areaRange, generalCondition: $generalCondition, amenities: $amenities, tileRanges: $tileRanges, tileRangesExcl: $tileRangesExcl, sort: $sort, limit: $limit, offset: $offset, cursor: $cursor, poiTypes: $poiTypes, locationDocId: $locationDocId, abtests: $abtests, searchContext: $searchContext, underPriceEstimation: $underPriceEstimation, priceDrop: $priceDrop, isCommercialRealEstate: $isCommercialRealEstate, commercialAmenities: $commercialAmenities, qualityClass: $qualityClass, numberOfEmployeesRange: $numberOfEmployeesRange, creationDaysRange: $creationDaysRange) {\n    total\n    cursor {\n      bulletinsOffset\n      projectsOffset\n      seenProjects\n      __typename\n    }\n    totalNearby\n    lastInGeometryId\n    cursor {\n      bulletinsOffset\n      projectsOffset\n      __typename\n    }\n    ...PoiFragment\n    __typename\n  }\n}\n\nfragment PoiFragment on PoiSearchResult {\n  poi {\n    ...PoiInner\n    ... on Bulletin {\n      rentalBrokerFee\n      eventsHistory {\n        eventType\n        price\n        date\n        __typename\n      }\n      insights {\n        insights {\n          category\n          tradeoff {\n            insightPlace\n            value\n            tagLine\n            impactful\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment PoiInner on Poi {\n  id\n  locationPoint {\n    lat\n    lng\n    __typename\n  }\n  type\n  firstTimeSeen\n  addressDetails {\n    docId\n    city\n    borough\n    zipcode\n    streetName\n    neighbourhood\n    neighbourhoodDocId\n    cityDocId\n    resolutionPreferences\n    streetNumber\n    unitNumber\n    district\n    __typename\n  }\n  ... on Project {\n    discount {\n      showDiscount\n      description\n      bannerUrl\n      __typename\n    }\n    dealType\n    phoneNumber\n    apartmentType {\n      size\n      beds\n      apartmentSpecification\n      type\n      price\n      __typename\n    }\n    bedsRange {\n      min\n      max\n      __typename\n    }\n    priceRange {\n      min\n      max\n      __typename\n    }\n    images {\n      path\n      __typename\n    }\n    promotionStatus {\n      status\n      __typename\n    }\n    projectName\n    projectLogo\n    isCommercial\n    projectMessages {\n      listingDescription\n      __typename\n    }\n    previewImage {\n      path\n      __typename\n    }\n    developers {\n      id\n      logoPath\n      __typename\n    }\n    tags {\n      bestSchool\n      bestSecular\n      bestReligious\n      safety\n      parkAccess\n      quietStreet\n      dogPark\n      familyFriendly\n      lightRail\n      commute\n      __typename\n    }\n    buildingStage\n    blockDetails {\n      buildingsNum\n      floorRange {\n        min\n        max\n        __typename\n      }\n      units\n      mishtakenPrice\n      urbanRenewal\n      __typename\n    }\n    __typename\n  }\n  ... on CommercialBulletin {\n    address\n    agentId\n    qualityClass\n    amenities {\n      accessible\n      airConditioner\n      alarm\n      conferenceRoom\n      doorman\n      elevator\n      fullTimeAccess\n      kitchenette\n      outerSpace\n      parkingBikes\n      parkingEmployee\n      parkingVisitors\n      reception\n      secureRoom\n      storage\n      subDivisible\n      __typename\n    }\n    area\n    availabilityType\n    availableDate\n    balconyArea\n    buildingClass\n    buildingType\n    buildingYear\n    currency\n    dealType\n    description\n    estimatedPrice\n    lastUpdated\n    eventsHistory {\n      eventType\n      price\n      date\n      __typename\n    }\n    feeType\n    floor\n    floors\n    fromDateTime\n    furnitureDetails\n    generalCondition\n    images {\n      ...ImageItem\n      __typename\n    }\n    lastActiveMarkDate\n    leaseTerm\n    leaseType\n    matchScore\n    monthlyTaxes\n    newListing\n    numberOfEmployees\n    originalId\n    poc {\n      type\n      ... on BulletinAgent {\n        madadSearchResult\n        officeId\n        officeContact {\n          imageUrl\n          __typename\n        }\n        exclusivity {\n          exclusive\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    ppm\n    price\n    qualityBin\n    rentalBrokerFee\n    rooms\n    source\n    status {\n      promoted\n      __typename\n    }\n    url\n    virtualTours\n    __typename\n  }\n  ... on Bulletin {\n    dealType\n    address\n    matchScore\n    beds\n    floor\n    baths\n    buildingYear\n    area\n    price\n    virtualTours\n    rentalBrokerFee\n    generalCondition\n    lastUpdated\n    eventsHistory {\n      eventType\n      price\n      date\n      __typename\n    }\n    status {\n      promoted\n      __typename\n    }\n    poc {\n      type\n      ... on BulletinAgent {\n        madadSearchResult\n        officeId\n        officeContact {\n          imageUrl\n          __typename\n        }\n        exclusivity {\n          exclusive\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    tags {\n      bestSchool\n      bestSecular\n      bestReligious\n      safety\n      parkAccess\n      quietStreet\n      dogPark\n      familyFriendly\n      lightRail\n      commute\n      __typename\n    }\n    commuteTime\n    dogsParkWalkTime\n    parkWalkTime\n    buildingClass\n    images {\n      ...ImageItem\n      __typename\n    }\n    __typename\n  }\n  ... on Ad {\n    addressDetails {\n      docId\n      city\n      borough\n      zipcode\n      streetName\n      neighbourhood\n      neighbourhoodDocId\n      resolutionPreferences\n      streetNumber\n      unitNumber\n      __typename\n    }\n    city\n    district\n    firstTimeSeen\n    id\n    locationPoint {\n      lat\n      lng\n      __typename\n    }\n    neighbourhood\n    type\n    __typename\n  }\n  __typename\n}\n\nfragment ImageItem on ImageItem {\n  description\n  imageUrl\n  isFloorplan\n  rotation\n  __typename\n}\n"}'
                    yield scrapy.Request(
                        url=api_url,
                        method='POST',
                        dont_filter=True,
                        cookies=cookies,
                        headers=headers,
                        body=body,
                        meta={'type': 'buy'}
                    )
        else:
            for _ in data['data']['searchPoiV2']['poi']:
                url = f'https://www.madlan.co.il/listings/{_["id"]}'
                if _["id"] in self.listing_ids:
                    continue

                if _['type'].lower() == 'project':
                    continue
                yield ScrapingBeeRequest(
                    url=url,
                    dont_filter=True,
                    meta={'type': _type, 'data': _},
                    callback=self.parse_details
                )

    def parse_details(self, response, **kwargs):
        amenities_data = {}
        for div in response.css('div[data-auto="amenities-block"] > div div.css-1wpv10e'):
            value = False
            if div.css('div.css-tc23vv'):
                value = True
            text = div.css('div.css-stb5t9::text').get('').strip()
            if "מַעֲלִית" in text:
                amenities_data['Elevator'] = value
            elif "מיזוג אויר" in text:
                amenities_data['Air conditioning'] = value
            elif "חניה" in text:
                amenities_data['parking'] = value
            elif "מרפסת" in text:
                amenities_data['balcony'] = value
            elif "סורגים" in text:
                amenities_data['bars'] = value
            elif "ממ״ד" in text:
                amenities_data['dimension'] = value
            elif "מחסן" in text:
                amenities_data['warehouse'] = value
            elif "לנכים" in text:
                amenities_data['handicap access'] = value
            elif "" in text:
                amenities_data['property mode'] = value

        data = response.meta.get('data')

        try:
            price = response.css('div[data-auto="price"] > div > div::text').get('').lstrip('₪').replace(',',
                                                                                                         '') or response.css(
                'div[data-auto="current-price"] ::text').get('').strip(' ₪‏').replace(',', '')
        except:
            price = data.get('price', '0')
        price = int(price)

        floor_building = response.css('div[data-auto="floor"] div:contains("קומה")::text').get('')
        street = data.get('addressDetails', {}).get('streetName', '').strip()
        StreetNumber = str(data.get('addressDetails', {}).get('streetNumber', '')).strip()
        city = data.get('addressDetails', {}).get('city', '')

        from_broker = True if len(
            response.css('div[data-auto="secondary_address"]  div[data-auto^="agent-tag"]::text').get(
                '').strip()) > 0 else False
        if (not street) or (not StreetNumber):
            from_broker = True

        seo_title = f'{street} {StreetNumber} {city}'.strip()
        item = {
            "seo_title": seo_title,
            "MadlanID": data.get('id'),
            "Status": response.meta.get('type'),
            "City": city,
            "Neighborhood": data.get('addressDetails', {}).get('neighbourhood'),
            "Street": street,
            "StreetNumber": StreetNumber,
            "Latitude": data.get('locationPoint', {}).get('lat', ''),
            "Longitude": data.get('locationPoint', {}).get('lng', ''),
            "PropertyType": response.css('div[data-auto="business-class"] > div::text').get(''),
            "RoomsNumber": data.get('beds'),
            "FloorNumber": data.get('floor'),
            "FloorsInBuilding": floor_building.split()[-1] if floor_building else '',
            "SizeInMeters": data.get('area'),
            "TotalPrice": price,
            "Url": response.url,
            "Active": True,  # pending
            "PriceperSquereMeter": data.get('price') / data.get('area') if data.get('price') and data.get(
                'area') else None,
            "PropertyDescription": response.css('[data-scroll-spy-id="Contact"] > div > h3 ~ div::text').get(
                '').strip(),
            "AdvertiserName": response.css('div[data-auto="poc-name"]::text').get(''),
            "AdvertiserPhone": response.css('a[href^="tel"]::attr(href)').get('').split(":")[-1],
            "FromBroker": from_broker,
            "Elevator": amenities_data.get('Elevator', False),
            "AirConditioning": amenities_data.get('Air conditioning', False),
            "parking": amenities_data.get('parking', False),
            "balcony": amenities_data.get('balcony', False),
            "bars": amenities_data.get('bars', False),
            "dimension": amenities_data.get('dimension', False),
            "warehouse": amenities_data.get('warehouse', False),
            "HandicapAccessibility": amenities_data.get('handicap access', False),
            "PropertyMode": response.css('div[data-auto="unit-general-condition-value"]::text').get('').strip(),
            "DateOfEntry": response.css('div[data-auto="unit-availability-value"]::text').get('').strip(),
            "DateScraped": datetime.now().strftime("%m/%d/%y %H:%M %p"),
            "DateUpdated": None,
            "Taxes": response.css('div[data-auto="unit-monthly-tax-value"]::text').get('').strip(),
            "Furniture": response.css('div[data-auto="unit-furniture-details-value"]::text').get('').strip(),
        }

        gallery_images = {}
        for _, image in enumerate(response.css('div[data-auto="bulletin-image"] > img::attr(src)').getall()):
            gallery_images[str(_)] = image

        item['gallery_images'] = gallery_images
        self.listing_ids.add(data["id"])
        yield item
