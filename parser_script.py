import requests
import pandas as pd
import numpy as np
from requests.auth import HTTPBasicAuth
from fake_useragent import UserAgent
import sys
from datetime import datetime
from datetime import timedelta
import time
import random
import csv
import os
from collections import deque

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

os.environ["no_proxy"]="*"

path_urls = '' #path to the .txt file with list of urls to parse from
path_prices = '' #path to the .csv file with prices log
path_errors = '' #path to the .csv file with errors log

proxies = ['185.15.172.212:3128', 
           '89.43.31.134:3128', 
           '193.53.87.220:33128', 
           '204.150.182.168:5395', 
           '204.150.210.188:6666', 
           '217.11.186.12:3128', 
           '76.192.70.58:8025', 
           '125.228.137.63:3128', 
           '200.12.133.6:8080', 
           '51.68.207.81:80'
          ]

def refferer(url):
    '''
    Makes url of previous page 
    '''
    part_1 = url.split('//')[0] + '//'
    part_2_ = url.split('//')[1].split('/')[:-2]
    part_2 = ''
    for i in part_2_:
        part_2 += i
        part_2 += '/'
    return part_1 + part_2

def message_sender(message):
    '''
    Telegram messege sender function
    
    token - Telegram token
    chat_id - ID of chat with Telegram message bot
    '''
    token = "" 
    chat_id = ""
    if message != None:
        url_req = ('https://api.telegram.org/bot{}/sendMessage?chat_id={}&text={}').format(token, 
                                                                                           chat_id, 
                                                                                           message)
        results = requests.get(url_req)
    else:
        pass

def get_headers(url):
    '''
    Makes a headers for the given url
    '''
    user_agent = UserAgent().random    
    accept = 'text/html, application/xhtml+xml, application/xml; q=0.9, image/avif, image/webp, image/apng,*/*; q=0.8, application/signed-exchange; v=b3; q=0.9'
    accept_encoding = 'gzip, deflate, br'
    accept_language = 'ru-RU, ru; q=0.9, en-US; q=0.8, en; q=0.7'
    host = 'https://moscow.petrovich.ru/'
    dnt = '1'
    timestamp = time.time()
    datetime = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(timestamp))
    connection = 'keep-alive'
    updgrade_insecure_requests = '1'
    headers = {'Accept': accept, 
               'User-agent': user_agent, 
               'Accept-Encoding': accept_encoding, 
               'Accept-Language': accept_language, 
               'Accept-Datetime': datetime,
               'Connection': connection, 
               'Referer': refferer(url), 
               'DNT': dnt, 
               'Updgrade-Insecure-Requests': updgrade_insecure_requests, 
              }    
    return headers

def price_finder(txt, first_phrase='', end_symbol=''): 
    '''
    Finds a price value in a page code
    txt - page code (input)
    first_phrase - a phrase that is preceding the price value
    end_symbol - a character that ends the price value
    '''
    first_price_idx = txt.find(first_phrase) + len(first_phrase)    
    price_part = txt[first_price_idx:first_price_idx+10]    
    price = ''
    for i in price_part:
        if i != end_symbol:
            price += i
        else:
            break    
    try: 
        price = int(price)   
    except ValueError:
        price = 'nan'       
    return price

def name_finder(txt, 
                first_phrase='', 
                end_symbol=''): 
    '''
    Finds a name of a product position in a page code
    
    txt - page code (input)
    first_phrase - a phrase that is preceding the price value
    end_symbol - a character that ends the price value
    '''
    first_name_idx = txt.find(first_phrase) + len(first_phrase)    
    name_part = txt[first_name_idx:first_name_idx+150]    
    name = ''
    for i in name_part:
        if i != end_symbol:
            name += i
        else:
            break    
    name = name[:-1]    
    if any(char in name for char in ['<', '>', "'\'", '{', '}', '[', ']', '=']):
        name = 'nan'    
    return name

def shop_finder(url):
    '''
    Finds a domain name in url
    '''
    return url.split('//')[1].split('/')[0].split('.')[-2]

def csv_writer(path, price_dict):
    '''
    Writes .csv file in defined path
    If file not existing it will create it
    
    path - the path of .csv file to write data into that
    price_dict - dict with data to be written into .csv file
    '''
        with open(path, 'a', encoding='utf-8', newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter='|')
            if os.stat(path).st_size == 0:    
                writer.writerow(price_dict.keys())
                writer.writerows(zip(*price_dict.values()))
            else:
                writer.writerows(zip(*price_dict.values()))
    
@dag(dag_id = 'parser', 
     schedule_interval = '00 12 * * *', 
     start_date = datetime(year = 2022, month = 11, day = 8), 
     catchup=False)

def parser():
    '''
    DAG that takes URLs from .csv file, load pages from thoose URLs, takes prices, product names and shop names and than writes it all to .csv file
    BTW it logs errors occcured in pages loading process and in processes of data extracting from them
    And it also sends messages to TG bot with parsing progress and changes in prices and product names
    '''
    
    @task(retries=3)
    def url_loader(path):
        '''
        Takes URLs from .txt file
        '''
        with open(path) as file:
            lines = file.readlines()

            urls = []
            for line in lines:
                line = line.split('\n')[0].split(' ')[0]
                urls.append(line)
        return urls
    
    @task(retries=3, multiple_outputs=True)
    def pages_loader(urls, 
                     proxie=None, 
                     wait_avg = 2):
        '''
        Loads pages and extracts data from them and also logs errors. Then puts this data to a dict 
        Also can use proxies 
        Sends a messege with a number of successfully parsed pages
        Returns a dict with data and errors
        
        urls - list of urls in input
        proxie - list of proxies, default is None
        wait avg - avarage wait time between pages loads
        '''

        url_num = len(urls)    
        proxie_dict = {'https': proxie}    
        price_dict = {} # словарь с распарсенными значениями
        date_time_lst = []
        url_lst = []
        name_lst = []
        price_lst = []
        shop_lst = []    
        error_dict = {} # словарь с логами ошибок
        date_time_err_lst = []
        url_err_lst = []
        err_lst = []    
        wait_low = wait_avg - wait_avg / 2
        wait_up = wait_avg + wait_avg / 2
        sleep_time = np.random.uniform(wait_low, wait_up)
        current_time = datetime.now().strftime("%d-%m-%Y %H:%M")
        error_message = ''    
        error_num = 0 # счетчик ошибок распознавания
        price_error_num = 0
        name_error_num = 0
        price_name_error_num = 0
        bad_status_code_num = 0 # счетчик сбоев доступа к страницам
        correct_pars_num = 0 # счетчик корректно распарсенных страниц
        for url in urls:
            headers = get_headers(url)
            if proxie != None:
                r = requests.get(url, headers=headers, proxies=proxie_dict)
            else:
                r = requests.get(url, headers=headers)
            status_code = r.status_code        
            if status_code == 200:                       
                text = r.text
                price = price_finder(text)    
                date_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S")    
                name = name_finder(text)
                shop = shop_finder(url)
                if name != 'nan' and price == 'nan':
                    error_message_ = ('\U0001F522 {} {}: не распарсилась цена\n\n').format(name, url)
                    error_message += error_message_
                    error_num += 1
                    price_error_num += 1
                    date_time_err_lst.append(date_time)
                    url_err_lst.append(url)
                    err_lst.append('price_err')
                elif name == 'nan' and price == 'nan':
                    error_message_ = ('\U0001F522 \U0001F522 {}: не распарсилось ничего\n\n').format(url)
                    error_message += error_message_
                    error_num += 2
                    price_name_error_num =+ 1
                    date_time_err_lst.append(date_time)
                    url_err_lst.append(url)
                    err_lst.append('price_name_err')
                elif name == 'nan' and price != 'nan':
                    error_message_ = ('\U0001F521 {}: не распарсилось название\n\n').format(url)
                    error_message += error_message_
                    error_num += 1 
                    name_error_num += 1
                    date_time_err_lst.append(date_time)
                    url_err_lst.append(url)
                    err_lst.append('name_err')
                    date_time_lst.append(date_time)
                    url_lst.append(url)
                    name_lst.append(name)
                    price_lst.append(price)
                    shop_lst.append(shop)
                    sleep_time = np.random.uniform(0.5, 1.5)
                    time.sleep(sleep_time)
                else:                
                    date_time_lst.append(date_time)
                    url_lst.append(url)
                    name_lst.append(name)
                    price_lst.append(price)
                    shop_lst.append(shop)
                    correct_pars_num += 1
                    time.sleep(sleep_time)                
            else:
                bad_status_code_num += 1
                error_message_ = ('\U000026D4 {}: ошибка доступа {}\n\n').format(url, status_code)
                date_time = datetime.now().strftime("%d-%m-%Y %H:%M:%S") 
                date_time_err_lst.append(date_time)
                url_err_lst.append(url)
                err_lst.append(status_code)            
                continue    
                
        _error_message = ('\U00002714 {}\n Корректно спарсилось {}/{}\n\n').format(current_time, correct_pars_num, url_num)
        
        if price_error_num == url_num and name_error_num == url_num and bad_status_code_num != url_num:
            error_message = ('\U000026A0 {}\n Ошибка: ничего не парсится!').format(current_time)
            message_sender(error_message)        
        elif bad_status_code_num == url_num:
            error_message = ('\U000026D4 {}\n Ошибка: ничего не грузится!').format(current_time)
            message_sender(error_message)        
        elif price_error_num == url_num and name_error_num != url_num and bad_status_code_num != url_num:
            error_message = ('\U0001F522 {}\n Ошибка: не парсятся цены').format(current_time)
            message_sender(error_message)        
        elif price_error_num != url_num and name_error_num == url_num and bad_status_code_num != url_num:
            error_message = ('\U0001F521 {}\n Ошибка: не парсятся названия').format(current_time)
            message_sender(error_message)        
        elif price_error_num not in [0, url_num] and name_error_num not in [0, url_num] and bad_status_code_num not in [0, url_num]:
            error_message = _error_message + error_message
            message_sender(error_message)
        else:
            error_message = _error_message + error_message
            message_sender(error_message) 
            
        price_dict['datetime'] = date_time_lst
        price_dict['url'] = url_lst
        price_dict['item_name'] = name_lst
        price_dict['price_rrub'] = price_lst
        price_dict['shop'] = shop_lst    
        error_dict['date_time'] = date_time_err_lst
        error_dict['url'] = url_err_lst
        error_dict['error'] = err_lst
        
        return {'data': price_dict, 'errors': error_dict}
    
    @task()
    def csv_writer(path, data): 
        '''
        Writes .csv file in defined path
        If file not existing it will create it
    
        path - the path of .csv file to write data into that
        price_dict - dict with data to be written into .csv file
        '''
        with open(path, 'a', encoding='utf-8', newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter='|')
            if os.stat(path).st_size == 0:    
                writer.writerow(data.keys())
                writer.writerows(zip(*data.values()))
            else:
                writer.writerows(zip(*data.values()))
    
    @task()
    def changes_finder(urls, 
                       csv_path, 
                       combining_fields=['datetime', 'item_name', 'price_rrub'], 
                       delimiter = '|'):  
        '''
        Finds changes in prices and product names
        
        urls - list of urls in input
        csv_path - path to .csv file where to find changes
        combining_fields - list of fields names to compare
        delimiter - delimeter in .csv file
        '''
        num_urls = len(urls)               
        with open(csv_path, 'r', encoding='utf-8') as csv_file:
            lines = csv_file.readlines()        
            header = lines[0].rstrip()
            header_lst = header.split(delimiter)        
            combining_fields_dict = {} 
            for item in combining_fields:
                combining_fields_dict[item]  = header_lst.index(item)                
            count_lines = 1        
            changes_dict = {}        
            for line in lines[::-1]:
                line_splitted = line.split(delimiter)            
                if count_lines <= num_urls:
                    for url in urls[::-1]:
                        if url in line:
                            count_lines += 1
                            changes_dict[url] = {}
                            for name, idx in combining_fields_dict.items():
                                changes_dict[url].update({name: [line_splitted[idx]]})                            
                elif (count_lines > num_urls) and (count_lines <= num_urls * 2):
                    for url in urls[::-1]:
                        if url in line:
                            count_lines += 1
                            for name, idx in combining_fields_dict.items():
                                changes_dict[url][name].append(line_splitted[idx])                            
                else:
                    break             
        return changes_dict
    
    @task()
    def changes_message(changes_dict):
        '''
        Forms a message from dict with changes and than sends it
        '''
        current_time = datetime.now().strftime("%d-%m-%Y %H:%M:")
        message = '\U0001F504 UPDATE на ' + current_time + '\n\n'
        changes_num = 0
        for url, fields in changes_dict.items():
            changes_lst = []
            for name, list_ in fields.items():
                if len(list_) > 1: 
                    if list_[0] != list_[1]:
                        changes_lst.append(name)                      
            if ('price_rrub' in changes_lst) and ('item_name' not in changes_lst):
                old_price = int(fields['price_rrub'][1])
                new_price = int(fields['price_rrub'][0])    
                item_name = fields['item_name'][0]
                old_date = fields['datetime'][1]
                delta_price = abs(old_price - new_price)
                what_happend = None
                what_happend_emo = None
                if old_price < new_price:
                    what_happend = 'подорожала'
                    what_happend_emo = '\U0001F4C8'
                else:
                    what_happend = 'подешевела'
                    what_happend_emo = '\U0001F4C9'          
                message_ = ('{} Позиция "{}" c {} {} на {} RUB с {} до {} RUB\n\n').format(what_happend_emo, 
                                                                                           item_name, 
                                                                                           old_date, 
                                                                                           what_happend, 
                                                                                           delta_price, 
                                                                                           old_price, 
                                                                                           new_price)          
                message += message_
                changes_num += 1
            elif ('price_rrub' not in changes_lst) and ('item_name' in changes_lst):
                old_name = fields['item_name'][1]
                new_name = fields['item_name'][0]
                old_date = fields['datetime'][1]           
                message_ = ('\U0000270F Позиция "{}" с {} сменила название на "{}"\n\n').format(old_name, 
                                                                                                old_date, 
                                                                                                new_name)           

                message += message_
                changes_num += 1
            elif ('price_rrub' in changes_lst) and ('item_name' in changes_lst):
                old_price = int(fields['price_rrub'][1])
                new_price = int(fields['price_rrub'][0])
                old_name = fields['item_name'][1]
                new_name = fields['item_name'][0]
                old_date = fields['datetime'][1]
                delta_price = abs(old_price - new_price)
                what_happend = None
                what_happend_emo = None
                if old_price < new_price:
                    what_happend = 'подорожала'
                    what_happend_emo = '\U0001F4C8'
                else:
                    what_happend = 'подешевела'
                    what_happend_emo = '\U0001F4C9'            
                message_ = ('{} \U0000270F Позиция "{}" с {} '
                            'сменила название на "{}" и {} на {} RUB с {} до {} RUB\n\n'.format(what_happend_emo, 
                                                                                                old_name, 
                                                                                                old_date, 
                                                                                                new_name, 
                                                                                                what_happend, 
                                                                                                delta_price, 
                                                                                                old_price, 
                                                                                                old_price))

                message += message_
                changes_num += 1
            else:
                continue            
        if changes_num != 0:
            message_sender(message)

    url_lst = url_loader(path = path_urls)
    
    load = pages_loader(urls = url_lst)
     
    parsed_data = load['data']
    parsed_errors = load['errors']
    
    write_data = csv_writer(path = path_prices, data = parsed_data)
    write_errors = csv_writer(path = path_errors, data = parsed_errors)
    
    changes = changes_finder(urls = url_lst, csv_path = path_prices)
    message = changes_message(changes)
    
    write_data >> changes
    
parser = parser()