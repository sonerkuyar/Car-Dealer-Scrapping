
from os import link
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool, cpu_count
import pandas as pd
 

ALL_CAR = []


def multilink(upper_lim, lower_lim):
    cr_id_list = []
    start_time=time.time()
    
    session_object = requests.Session()
    
    print(f"upper: {upper_lim}" + f" lower: {lower_lim}")
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"}
    website = f"https://www.arabam.com/ikinci-el/otomobil?currency=TL&maxPrice={upper_lim}&minPrice={lower_lim}&take=50&view=Detailed"
    response = session_object.get(website,headers=headers)
    if response.status_code != 200:
        return
    soup = BeautifulSoup(response.text,'lxml')
    results = soup.find_all("span", {"class":"bold"})
    try:
        page_num = int(results[1].get_text())
    except:
        page_num = int(results[1].get_text().strip().split()[0])
    
    print(page_num)
    new_session = requests.Session()
    for i in range(page_num):
        website = f"https://www.arabam.com/ikinci-el/otomobil?currency=TL&maxPrice={upper_lim}&minPrice={lower_lim}&take=50&view=Detailed&page={i+1}"
        response = new_session.get(website,headers=headers)
        soup = BeautifulSoup(response.text,'lxml')
        results = soup.find_all("div", {"class":"content-container"})
        for elem in results[:-1:]:
            id = elem.find('p',{"class":"id"}).get_text().strip().split()[2]
            cr_id_list.append(f"https://www.arabam.com/ilan/{id}")
    end_time = time.time()
    print(print(end_time-start_time))
    return(cr_id_list)

def get_links(price_diff,min_price, max_price):
    cr_id_list = []
    start_time=time.time()
    for i in range(((max_price-min_price)//price_diff)):
        session_object = requests.Session()
        upper_lim = min_price + (price_diff)*(i+1) - 1
        lower_lim = min_price + (price_diff)*i
        print(f"upper: {upper_lim}" + f" lower: {lower_lim}")
        headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"}
        website = f"https://www.arabam.com/ikinci-el/otomobil?currency=TL&maxPrice={upper_lim}&minPrice={lower_lim}&take=50&view=Detailed"
        response = session_object.get(website,headers=headers)
        if response.status_code != 200:
            return
        soup = BeautifulSoup(response.text,'lxml')
        results = soup.find_all("span", {"class":"bold"})
        page_num = int(results[1].get_text())
        print(page_num)
        new_session = requests.Session()
        for i in range(page_num):
            website = f"https://www.arabam.com/ikinci-el/otomobil?currency=TL&maxPrice={upper_lim}&minPrice={lower_lim}&take=50&view=Detailed&page={i+1}"
            response = new_session.get(website,headers=headers)
            soup = BeautifulSoup(response.text,'lxml')
            results = soup.find_all("div", {"class":"content-container"})
            for elem in results[:-1:]:
                id = elem.find('p',{"class":"id"}).get_text().strip().split()[2]
                cr_id_list.append(f"https://www.arabam.com/ilan/{id}")
    end_time = time.time()
    print(print(end_time-start_time))
    return(cr_id_list)



def fetch(link,curr_session):
    website = link
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}
    
    response = curr_session.get(website, headers=headers,stream=True)
    soup = BeautifulSoup(response.text,'lxml')
    results = soup.find_all("li",{"class":"bcd-list-item"}) + soup.find_all("span",{'class':"color-red4"})
    if len(results) ==0:
        return
    results[-1].get_text().strip().split()
    car_dict = dict()
    if response.status_code !=200:
        return
    for elem in results:
        
        if len(elem) == 1:
            
            price = elem.get_text().strip()
            car_dict.update({'Fiyat':price})
            Price_indicator = True
            continue  
        try:
            features_car = ''.join(elem.get_text().split()).split(":")
            car_dict.update({features_car[0]:features_car[1]})
        except:
            pass
    print('.',end="",flush=True)
    return car_dict
    #car_df = pd.DataFrame([car_dict])
    #print(car_df)
    
    #all_car_df = pd.concat([car_df,all_car_df])
    
    print('.',end="",flush=True)
if __name__ == '__main__':
    #======>
    #preparing pool
    start_time = time.time()
    prices_pool = []
    price_diff, min_price,max_price = 10000,100000,1500000
    for i in range(((max_price-min_price)//price_diff)):
        upper_lim = min_price + (price_diff)*(i+1) - 1
        lower_lim = min_price + (price_diff)*i
        prices_pool.append((upper_lim,lower_lim))
    with Pool(32) as p:
        results = p.starmap(multilink, prices_pool)

    end_time = time.time()
    print("multitime:" +str(end_time-start_time))
    car_links = list()
    for elem in results:
        for link in elem:
            car_links.append(link)
   
    #======>

    all_car_df = pd.DataFrame()
    
    
    print(len(car_links))
    print(len(set(car_links)))
    curr_session = requests.Session()
    headers = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15"}
    start_time = time.time()
    core_count = cpu_count()
    #==================>
    '''for link in car_links[:300:]:
        fetch(link,curr_session,headers)
    '''
    args = [(link,curr_session) for link in car_links]
    with Pool(32) as p:
        results = p.starmap(fetch, args)
    for elem in results:
        if isinstance(elem,type(None)):
            print("None valid elem")
            results.remove(elem)
        
    
    
    print("DONE")
    car_df = pd.DataFrame(results)
    car_df.to_csv(f"car_{min_price//1000}k-{max_price//1000}k.csv")
    
    print("DF DONE")
    #print(results)

    
#==================>
    end_time = time.time()
    print(end_time-start_time)