from jugaad_trader import Zerodha
import gspread
import json, os
from mycolorlogger.mylogger import log
import requests
from datetime import datetime, time,date
import pandas as pd
import sys
from oauth2client.service_account import ServiceAccountCredentials


def has_script_been_run_today():
    today = date.today()
    file_path = 'last_run_date.txt'
    if os.path.exists(file_path):
        with open(file_path, 'r') as file:
            last_run_date_str = file.read().strip()
        last_run_date = datetime.strptime(last_run_date_str, '%Y-%m-%d').date()
        return last_run_date == today
    return False

def mark_script_as_run_today():
    today = date.today()
    file_path = 'last_run_date.txt'
    with open(file_path, 'w') as file:
        file.write(today.strftime('%Y-%m-%d'))
    log.logger.info("Script has been marked as run for today.")

class ETFBUY:
    def __init__(self) -> None:
        self.config = self.getConfig
        self.client = self.getGoogleClient
        self.spreadsheet = self.client.open_by_url(self.config['google_sheet_url'])
        pass

    @property
    def getConfig(self, ) -> dict:
        """_summary_

        Returns:
            dict: get the required configration details from config.json file
        """
        if os.path.exists("config.json"):
            with open("config.json") as f:
                config = json.load(f)
                return config
        else:
            assert False, "config.json not found"
    @property
    def getGoogleClient(self,):
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(self.config['auth_json'], scope)
        client = gspread.authorize(creds)
        return client
    def getetf_data(self,):
        worksheet = self.spreadsheet.get_worksheet(2)
        data = worksheet.get_all_values()
        df = pd.DataFrame(data[1:], columns=data[0])
        selected_rows = df.iloc[2:9]
        selected_rows = selected_rows.drop(selected_rows.index[:1])
        selected_rows = selected_rows.reset_index(drop=True)
        selected_rows.columns = selected_rows.iloc[0]
        selected_rows = selected_rows.drop(0)
        selected_rows = selected_rows.reset_index(drop=True)
        selected_rows.columns = selected_rows.columns.str.strip()
        selected_rows = selected_rows.iloc[:, :9]
        new_etf = selected_rows.iloc[:, :4]
        new_etf = new_etf.to_dict(orient='records')
        new_etf = [i for i in new_etf if i['ETF Code'] ]
        already_etf = selected_rows.iloc[:, 5:]
        already_etf = already_etf.to_dict(orient='records')
        already_etf = [i for i in already_etf if i['ETF Code'] ]
        return {"new" : new_etf, "already" : already_etf}


    def getCurrentPrice(self, symbol):
        url = "https://www.nseindia.com/api/quote-equity?symbol={}".format(symbol)
        payload={}
        s = requests.session()
        headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://www.nseindia.com/get-quotes/equity?symbol=PVTBANIETF',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'TE': 'trailers'
        }
        s.headers.update(headers)
        s.get("https://www.nseindia.com/")
        response = s.request("GET", url, headers=headers, data=payload).json()
        return response['priceInfo']['lastPrice']
    def logWriterToSheet(self,etf_code, etf_name, current_price, totalQuantity):
        worksheet = self.spreadsheet.get_worksheet(3)
        existing_data = worksheet.get_all_values()
        first_empty_row = len(existing_data) + 1
        if first_empty_row == 6 :
            first_empty_row = 7
        formatted_date = '-'.join([str(x) for x in datetime.now().date().strftime("%d-%m-%Y").split('-')])
        data_to_write = [formatted_date,etf_code,etf_name,current_price,totalQuantity]
        worksheet.append_row(data_to_write, value_input_option='RAW', insert_data_option='INSERT_ROWS', table_range=f'A{first_empty_row}')
    def placeKiteOrder(self,etf_code, etf_name, currentPrice):
        kite = Zerodha()
        kite.set_access_token()
        totalQuantity = round(self.config['investment_amount'] / currentPrice)
        log.logger.info(f"placing order for buy with current price")
        log.logger.info(f"total quantity : {totalQuantity}")
        log.logger.info(f"current price : {currentPrice}")
        order_id = kite.place_order(tradingsymbol=etf_code,
            exchange=kite.EXCHANGE_NSE,
            transaction_type=kite.TRANSACTION_TYPE_BUY,
            quantity=totalQuantity,
            variety=kite.VARIETY_REGULAR,
            order_type=kite.ORDER_TYPE_LIMIT,
            price = currentPrice,
            product=kite.PRODUCT_CNC,
            #disclosed_quantity = round(totalQuantity* 0.10)+1,
            validity=kite.VALIDITY_DAY)
        log.logger.info(f"{order_id}: order placed")
        log.logger.info('writing to sheet....')
        self.logWriterToSheet(etf_code, etf_name, currentPrice, totalQuantity)
        return True
    def checkisbesttimetobuy(self,):
        current_time = datetime.now().time()
        start_time = time(14, 0)  # 2:00 PM
        end_time = time(15, 0)    # 3:00 PM
        if start_time <= current_time <= end_time:
            return True
        else:
            self.askForRun()
    def askForRun(self):
        log.logger.warning("this is not best time to buy run(between 2:00 PM and 3:00 PM)")
        log.logger.warning("you have to buy only one etf in one day ..")
        flag = "n" #input("Do you still want to buy etf(y/n): ")
        if flag == 'y':
            return True
        log.logger.info("software is closing now..")
        sys.exit()
if __name__ == "__main__":
    etf_buy = ETFBUY()
    etf_buy.checkisbesttimetobuy()
    if  has_script_been_run_today():
        etf_buy.askForRun()
    etf_data = etf_buy.getetf_data()    
    if etf_data['new']:
        for i in etf_data['new']:
            log.logger.info(f"Rank {i['Rank#']} : dma : { i['% Change 20 DMA Vs CMP']}% , {i['Underlying Asset']}, code : {i['ETF Code']}")
            current_price = etf_buy.getCurrentPrice(i['ETF Code'])
            etf_buy.placeKiteOrder(i['ETF Code'],i['Underlying Asset'],current_price)
            mark_script_as_run_today()
            break
    elif etf_data['already']:
        log.logger.info("selecting etf from already avaible in our portfolio")
        for i in etf_data['already']:
            log.logger.info(f"Rank {i['Rank#']} : dma : { i['Fallen from Last Buy Price']}% , {i['Underlying Asset']}, code : {i['ETF Code']}")
            current_price = etf_buy.getCurrentPrice(i['ETF Code'])
            etf_buy.placeKiteOrder(i['ETF Code'],current_price)
            mark_script_as_run_today()
            break
    else:
        log.logger.info("no etf to buy")
        
    