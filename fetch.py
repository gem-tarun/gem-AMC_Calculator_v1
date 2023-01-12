from datetime import date
import requests
import json
import math
import traceback
import time
import datetime

import configparser
from db_conn import insert_data

config = configparser.ConfigParser()
config.read('config.ini')


# Method to get nearest strikes
def round_nearest(x,num=50): return int(math.ceil(float(x)/num)*num)
def nearest_strike_bnf(x): return round_nearest(x,100)
def nearest_strike_nf(x): return round_nearest(x,50)

# Urls for fetching Data
url_oc      = "https://www.nseindia.com/option-chain"
url_bnf     = 'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'
url_nf      = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'
url_indices = "https://www.nseindia.com/api/allIndices"

# Headers
headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8',
            'accept-encoding': 'gzip, deflate, br'}

sess = requests.Session()
cookies = dict()


# Local methods
def set_cookie():
    request = sess.get(url_oc, headers=headers, timeout=5)
    cookies = dict(request.cookies)

def get_data(url):
    set_cookie()
    response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
    if(response.status_code==401):
        set_cookie()
        response = sess.get(url_nf, headers=headers, timeout=5, cookies=cookies)
    if(response.status_code==200):
        return response.text
    return ""

def set_header():
    global bnf_ul
    global nf_ul
    global bnf_nearest
    global nf_nearest
    response_text = get_data(url_indices)
    data = json.loads(response_text)
    for index in data["data"]:
        if index["index"]=="NIFTY 50":
            nf_ul = index["last"]
        if index["index"]=="NIFTY BANK":
            bnf_ul = index["last"]
    bnf_nearest=nearest_strike_bnf(bnf_ul)
    nf_nearest=nearest_strike_nf(nf_ul)


# Fetching CE and PE data based on Nearest Expiry Date
def print_oi(num,step,nearest,url):
    final_data_list = []
    data_dict = {
      'sec_type':'option',
      'mkt_ticker':None,
      'expiryDate':None,
      'strikePrice':None,
      'underlying_ticker':None,
      'underlying_exp_date': None,
      'ticker_name':None,
      'field':None,
      'frequency': 'daily', 
      'source':'NSE',
      'value':None,
      'ticker_type': None
    }
    response_text = get_data(url)
    data = json.loads(response_text)
    currExpiryDate_list = data["records"]["expiryDates"][:4]
    for currExpiryDate in currExpiryDate_list:
      strike = nearest - (step*num)
      start_strike = nearest - (step*num)
      for item in data['records']['data']:

          if item["expiryDate"] == currExpiryDate:
              if item["strikePrice"] == strike and item["strikePrice"] < start_strike+(step*num*2):
                  data_dict['expiryDate'] = item["expiryDate"]
                  data_dict['strikePrice'] = item["strikePrice"]
                  try:
                    if item['CE']:
                      data_dict['mkt_ticker'] = item['CE']['underlying']
                      data_dict['underlying_ticker'] = item['CE']['underlying']
                      data_dict['underlying_exp_date'] = item["expiryDate"]
                      data_dict['ticker_name'] = item['CE']['underlying']+'_CE_'+str(item["strikePrice"])+'_'+str(item["expiryDate"])
                      data_dict['field'] = 'lastPrice'
                      data_dict['value'] = item['CE']['lastPrice']
                      data_dict['ticker_type'] = 'CE'
                      item.pop('CE')
                      final_data_list.append(data_dict.copy())
                      try:
                        if item['PE']:
                          data_dict['mkt_ticker'] = item['PE']['underlying']
                          data_dict['underlying_ticker'] = item['PE']['underlying']
                          data_dict['underlying_exp_date'] = item["expiryDate"]
                          data_dict['ticker_name'] = item['PE']['underlying']+'_PE_'+str(item["strikePrice"])+'_'+str(item["expiryDate"])
                          data_dict['field'] = 'lastPrice'
                          data_dict['value'] = item['PE']['lastPrice']
                          data_dict['ticker_type'] = 'PE'
                          final_data_list.append(data_dict.copy())
                      except:
                        print(traceback.format_exc())
                  except:
                    print(traceback.format_exc())

                    data_dict
                  strike = strike + step
    return final_data_list


set_header()
top_strike = config.getint('Top_Strike','top_strike_price')
nifty_data = print_oi(top_strike,50,nf_nearest,url_nf)
bank_nifty_data = print_oi(top_strike,100,bnf_nearest,url_bnf)
insert_data(nifty_data)
insert_data(bank_nifty_data)