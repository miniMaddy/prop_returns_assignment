from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from googletrans import Translator
from bs4 import BeautifulSoup
import time
import pandas as pd
from sqlalchemy import create_engine
import os

db_password = os.getenv("PROP_RETURNS_USER_PASS")

# Small utility function to translate text using googletrans
def translate_text(text, src = 'mr', dest = 'en'):
    translator = Translator()
    translated_text = translator.translate(text = text, src = src, dest = dest)
    return translated_text.text

# Scraper class which contains the methods to scrape the website
class Scraper:

    """
    Initialize the webdriver and open the website
    """
    def __init__(self, website) -> None:
        self.driver = webdriver.Firefox()
        self.website = website
        self.driver.get(self.website)

    """
    Getter to fetch the driver in case some operations are needed to be performed outside the class
    """
    def get_driver(self):
        return self.driver

    """
    Method to select the drop down menu. 
    It takes xpath and the desired option as parameters.
    """
    def select_drop_down_menu_by_xpath(self, xpath, option):
        # Selecting the desired option from the drop down menu
        drop_down = self.driver.find_element(By.XPATH, xpath)
        drop_down_drop = Select(drop_down)
        drop_down_drop.select_by_visible_text(option)

        # Waiting for the background to load (This can change in other scenarios, i.e. this is situation specific)
        wait = WebDriverWait(self.driver, 10)
        wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, 'bg')))
    
    """
    Method to fill text.
    It takes xpath and the text as parameters.
    """
    def fill_text_by_xpath(self, xpath, text):
        element = self.driver.find_element(By.XPATH, xpath)
        element.send_keys(text)

    
# Initialising an object of Scraper class
scraper = Scraper('https://pay2igr.igrmaharashtra.gov.in/eDisplay/propertydetails')

# Filling out the form in the website
scraper.select_drop_down_menu_by_xpath("//select[@id='dbselect']", '2023')
scraper.select_drop_down_menu_by_xpath("//select[@id='district_id']", 'मुंबई उपनगर')
scraper.select_drop_down_menu_by_xpath("//select[@id='taluka_id']", 'अंधेरी')
scraper.select_drop_down_menu_by_xpath("//select[@id='village_id']", 'बांद्रा')
scraper.fill_text_by_xpath("//input[@id='free_text']", '2023')

# Waiting to fill the captcha manually
time.sleep(10)

# Clicking the submit button
driver = scraper.get_driver()
submit = driver.find_element(By.ID, 'submit')
submit.click()

# Waiting for the results to load
time.sleep(10)

# Selecting the drop down menu to show 50 entries
scraper.select_drop_down_menu_by_xpath("//select[@name='tableparty_length']", '50')

# Getting the page source and start scraping using BeautifulSoup
page_content = driver.page_source
soup = BeautifulSoup(page_content, "html.parser")
table = soup.find('table', {'id': 'tableparty'})

# Separating the headers and body of the table and translating them accordingly
headers = [header.text.strip() for header in table.find_all('th')]
translated_headers = []
for header in headers:
    translated_content = translate_text(header)
    translated_headers.append(translated_content)

data = []
for row in table.find('tbody').find_all('tr'):
    row_data = []
    for cell in row.find_all('td'):
        # Check if the cell contains an <a> tag
        link = cell.find('a')
        if link:
            row_data.append(link['href'])  # Extract the href attribute
        else:
            text = cell.text.strip()
            try:
                translated_text = translate_text(text)
            except:
                translated_text = text
            row_data.append(translated_text)
    data.append(row_data)

# Creating a dataframe and storing it in a csv file so that we have a backup of scraped data in case something goes wrong.
df = pd.DataFrame(data, columns=translated_headers)

df.to_csv('output.csv', index=False)

# Closing the webdriver
driver.close()

"""
Converting the data into a suitable format for the database
"""

df = pd.read_csv('output.csv')

df['Year'] = pd.to_datetime(df['Year'])

column_name_mapping = {
    'Anu no.': 'S_No',
    'Document no.': 'Doc_No',
    'Type of documentation': 'Doc_type',
    'Well.Ni.Office': 'Office',
    'Year': 'Date',
    'Writing': 'Buyer_name',
    'Writing.1': 'Seller_name',  # Rename this column to match the PostgreSQL table
    'Other information': 'Other_info',
    'List no.1': 'List_No'
}

# Rename the columns in the DataFrame based on the mapping
df.rename(columns=column_name_mapping, inplace=True)

table_name = 'property_details'

# Storing the data to a postgresql table
engine = create_engine(f'postgresql+psycopg2://prop_returns:{db_password}@localhost:5432/db')

df.to_sql(table_name, engine, if_exists='replace', index=False)

engine.dispose()