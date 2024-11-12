import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import re

from datetime import datetime 
import pandas as pd
global_df = pd.DataFrame()

import time
import requests
import time
from bs4 import BeautifulSoup
import re
import json
import datetime
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobClient
from datetime import datetime, timedelta

start_timefull = time.time()

# Function to set up a new WebDriver instance
def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')  
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument("--log-level=3")  # Suppress logs
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

# Data list to store results
data_list = []

num_pages=2400


# Function to extract car data from JSON data
def data_extract(data):
    for item in data:
        attributes = item.get('attributes', {})       

        Car_ID = item.get('id')
        title = attributes.get('title')
        region = attributes.get('agent_locality')      
        brand = attributes.get('make')
        model = attributes.get('model')
        variant =attributes.get('variant_short')
        price = attributes.get('price')
        dealer=attributes.get('agent_name')
        car_type = attributes.get('new_or_used')
        registration_year = re.search(r'\d+', title)
        registration_year = registration_year.group()
        mileage = attributes.get('mileage')
        transmission = attributes.get('transmission')
        fuel_type = attributes.get('fuel_type')
        seller_type=attributes.get('seller_type')
        manufacturers_colour =None
        manufacturers_colour1 = attributes.get('colour')
        if manufacturers_colour!=None:
            manufacturers_colour = attributes.get('colour')        
        body_type = attributes.get('body_type')
        current_datetime1 = datetime.now()
        latitude=attributes.get('agent_coords_0_coord')
        longitude=attributes.get('agent_coords_1_coord')
        # Add 2 hours to the current datetime
        new_datetime = current_datetime1 + timedelta(hours=2)
        current_datetime = new_datetime.strftime('%Y-%m-%d')
        province = attributes.get('province')
        dealer_attri = item.get('relationships', {}) 
        dealer_id= dealer_attri['seller']['data']['id']            
        data_list.append({
                'Car_ID': Car_ID,
                'Title': title,
                'Region': region,
                'Make': brand,
                'Model': model,
                'Variant': variant,
                'Suburb': None,
                'Province': province,
                'Price': price,
                'ExpectedPaymentPerMonth': None,
                'CarType': car_type,
                'RegistrationYear': registration_year,
                'Mileage': mileage,
                'Transmission': transmission,
                'FuelType': fuel_type,
                'PriceRating': None,
                'Dealer': dealer,
                'LastUpdated': None,
                'PreviousOwners': None,
                'ManufacturersColour': manufacturers_colour,
                'BodyType': body_type,
                'ServiceHistory': None,
                'WarrantyRemaining': None,
                'IntroductionDate': latitude,
                'EndDate': longitude,
                'ServiceIntervalDistance': None,
                'EnginePosition': None,
                'EngineDetail': seller_type,
                'EngineCapacity': None,
                'CylinderLayoutAndQuantity': None,
                'FuelTypeEngine': fuel_type,
                'FuelCapacity': None,
                'FuelConsumption': None,
                'FuelRange': None,
                'PowerMaximum': None,
                'TorqueMaximum': None,
                'Acceleration': None,
                'MaximumSpeed': None,
                'CO2Emissions': None,
                'Version': 1,
                'DealerUrl':dealer_id,
                'Timestamp': current_datetime
            })
# Loop through pages
page_number = 1750 # Start from page 1
max_pages = num_pages 

try:
    while page_number <= max_pages:
        # Setup the driver for each page
        driver = setup_driver()

        # Build the URL for the current page
        url = f"https://www.cars.co.za/usedcars/?sort=price&P={page_number}"
        driver.get(url)
        time.sleep(2)  # Allow time for the page to load

        # Parse the current page HTML
        page_html = driver.page_source
        car_page = BeautifulSoup(page_html, "html.parser")

        if car_page is not None:
            try:
                data_dict = json.loads(car_page.find('script', {'id': '__NEXT_DATA__'}).string)
                datap = data_dict['props']['initialState']['searchCarReducer']['searchResults']
                data = datap.get('data', [])
                data_extract(data)
            except Exception as e:
                print(f"Error processing page {e}")

        # Close the driver after processing the page
        driver.quit()

        # Go to the next page
        page_number += 1

        # Pause every 10 pages to avoid overwhelming the server
        if page_number % 10 == 0 or page_number==max_pages:
            print(f"Scraped {page_number - 1} pages, pausing for a moment.")
    
            data_frame = pd.DataFrame(data_list)
            timenow=datetime.now().strftime('%H')
            current_min = int(datetime.now().strftime('%M'))
            print(timenow)
            print(current_min)
    
    
        ### Save DataFrame to CSV
            car_data_csv = data_frame.to_csv(encoding="utf-8", index=False)
            sas_url = f"https://autotraderstorage.blob.core.windows.net/carscoza/Carscoza_5_{page_number}.csv?sv=2023-01-03&st=2024-11-12T09%3A40%3A08Z&se=2025-12-13T09%3A40%3A00Z&sr=c&sp=racwl&sig=jAVH6gfSPsevwiQyFfJrW6voqdVbUS5Das0%2BulrJ9Z0%3D"
            
            client = BlobClient.from_blob_url(sas_url)
            client.upload_blob(car_data_csv, overwrite=True)                
            
            time.sleep(10)

finally:
    # Save collected data to a CSV file
    data_frame = pd.DataFrame(data_list)
    timenow=datetime.now().strftime('%H')
    current_min = int(datetime.now().strftime('%M'))


    car_data_csv = data_frame.to_csv(encoding="utf-8", index=False)
    sas_url = f"https://autotraderstorage.blob.core.windows.net/carscoza/Carscoza_5_{page_number}.csv?sv=2023-01-03&st=2024-11-12T09%3A40%3A08Z&se=2025-12-13T09%3A40%3A00Z&sr=c&sp=racwl&sig=jAVH6gfSPsevwiQyFfJrW6voqdVbUS5Das0%2BulrJ9Z0%3D"
    client = BlobClient.from_blob_url(sas_url)
    client.upload_blob(car_data_csv, overwrite=True)  

    end_timefull = time.time()
    now = datetime.now()
    print("Current date:", now.date())    
    print(f"Execution Time extractionf: {end_timefull - start_timefull} seconds")  