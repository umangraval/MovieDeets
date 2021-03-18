from selenium import webdriver 
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC 
from selenium.common.exceptions import NoSuchElementException
import time
import pandas as pd
from selenium.webdriver.chrome.options import Options
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')  # Last I checked this was necessary.

browser = webdriver.Chrome(executable_path="/usr/bin/chromedriver",options=options)

def check_exists_by_xpath(xpath):
    try:
        browser.find_element_by_xpath(xpath)
    except NoSuchElementException:
        return False
    return True

scraped = []
output = pd.DataFrame()
print(output.head())
browser.get("https://www.tripadvisor.com/Airline_Review-d8729099-Reviews-JetBlue")
timeout = 3
while True:
    try:
        time.sleep(timeout)
        titles = browser.find_elements_by_class_name("location-review-review-list-parts-ReviewTitle__reviewTitleText--2tFRT")
        main_text = browser.find_elements_by_class_name("location-review-review-list-parts-ExpandableReview__reviewText--gOmRC")
        meta_data = browser.find_elements_by_class_name("location-review-review-list-parts-RatingLine__labelsContainer--rSajH")
        for x,y,z in zip(titles,main_text,meta_data):
            z = z.text.split('\n')
            print({'text':y.text+' '+x.text,'location':z[0],'type':z[1],'class':z[2]})
            scraped.append({'title':x.text,'main_text':y.text,'location':z[0],'type':z[1],'class':z[2]})
            output = output.append({'text':y.text+' '+x.text,'location':z[0],'type':z[1],'class':z[2]}, ignore_index=True)
        # browser.find_element_by_class_name('ui_button nav next primary').click()
        browser.find_element_by_link_text('Next').click()
        output.to_csv('tripadv.csv', sep='\t', encoding='utf-8')
    except Exception as e:
        print(e)
        break



# browser.close()