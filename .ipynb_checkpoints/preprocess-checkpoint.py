from bs4 import BeautifulSoup
import pandas as pd
import requests
import pandas as pd
import time
import lxml
#import cchardet
import concurrent.futures

def get_links(price_diff,min_price, max_price):
    cr_id_list = []
    start_time=time.time()
    for i in range(((max_price-min_price)//price_diff)):
        upper_lim = min_price + (price_diff)*(i+1)
        lower_lim = min_price + (price_diff)*i
        print(f"upper: {upper_lim}" + f" lower: {lower_lim}")
        session_object = requests.Session()
        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
        website = f"https://www.arabam.com/ikinci-el/otomobil?currency=TL&maxPrice={upper_lim}&minPrice={lower_lim}&take=50&view=Detailed"
        response = session_object.get(website,headers=headers)
        soup = BeautifulSoup(response.text,'lxml')
        results = soup.find_all("span", {"class":"bold"})
        page_num = int(results[1].get_text())
        print(page_num)
        for i in range(page_num):
            website = f"https://www.arabam.com/ikinci-el/otomobil?currency=TL&maxPrice={upper_lim}&minPrice={lower_lim}&take=50&view=Detailed&page={i+1}"
            response = session_object.get(website,headers=headers)
            soup = BeautifulSoup(response.text,'lxml')
            results = soup.find_all("div", {"class":"content-container"})
            for elem in results[:-1:]:
                id = elem.find('p',{"class":"id"}).get_text().strip().split()[2]
                cr_id_list.append(f"https://www.arabam.com/ilan/{id}")
    end_time = time.time()
    print(print(end_time-start_time))
    return(cr_id_list)

#def get_car_features():


if __name__ == '__main__':
    car_links = get_links(10000,200000,300000)
