import pymongo
from nsetools import Nse
import datetime
import smtplib, ssl
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText 
from email.mime.base import MIMEBase 
from email import encoders
import requests
import time
from bs4 import BeautifulSoup
import re
from pexecute.process import ProcessLoom

DB_PASS = "XXXXXXX"
GMAIL_PASS = "XXXXXXX"

class Stocks:
    
    def __init__(self):
        self.nse = Nse()
        self.client = pymongo.MongoClient(
            f"mongodb+srv://rachitahuja20:{DB_PASS}@cluster0.toqqc.mongodb.net/Cluster0?retryWrites=true&w=majority"
        )
        self.db = self.client.Stocks
        self.bulk = self.db.Stocks_Historical.initialize_unordered_bulk_op()

    def get_stock_value(self,ticker):
        
        if self.nse.is_valid_code(ticker):
            try:
                q = self.nse.get_quote(ticker)
                lastprice = q["lastPrice"]
                return lastprice
            except:
                return None
        else:
            return None

    def get_request(self,url, headers, timeout=(5,25)):
        start_time = time.time()
        response=requests.get(url, timeout=timeout,headers=headers)
        end_time=time.time() - start_time
        return {'responsetime':end_time,'response':response.text}

    def lastTradedPrice(self,ticker):
        url = 'https://www.nseindia.com/live_market/dynaContent/live_watch/get_quote/GetQuote.jsp?symbol='+ticker+'&illiquid=0&smeFlag=0&itpFlag=0'.format(ticker)

        # url = "https://httpbin.org/user-agent"
        headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36'
                    ,"accept": "application/json"
                    }
        try:
            data_dict = self.get_request(url=url, headers=headers)

            soup = BeautifulSoup(data_dict["response"], 'html.parser')

            text = soup.find_all(id="responseDiv")[0].get_text()


            LTP = re.search('(?<="lastPrice":).*?(?=}])', text).group(0).replace('"','')
            
            return LTP
        
        except Exception as e:
            print("error", e)

    def today_date(self):
        date = datetime.datetime.today().strftime("%d-%m-%Y")
        return date
    
    def Mongo_Data_Push(self,ticker):
        
        #data = self.lastTradedPrice(ticker)
        data = self.get_stock_value(ticker)
        date = str(self.today_date())        
        
        if data is not None:
            
            self.bulk.find({"SYMBOL":ticker}).update({"$set":{date: data}})
    
    def pipeline(self,ticker):
        
        self.Mongo_Data_Push(ticker)

    def get_list(self):
        
        doc = self.db.Nse_Stocks_List.find()
        for docs in doc:
            return docs["List"]

def connect_email(start_time):
    
    s = smtplib.SMTP('smtp.gmail.com', 587)

    # start TLS for security 
    s.starttls() 
  
    # Authentication 
    s.login("poonam.ahuja91971@gmail.com", GMAIL_PASS) 

    fromaddr = "poonam.ahuja91971@gmail.com"
    toaddr = "Rachitahuja20@gmail.com"

    # MIMEMultipart 
    msg = MIMEMultipart() 

    # senders email address 
    msg['From'] = "Rachit Ahuja" 

    # receivers email address 
    msg['To'] = toaddr 

    # the subject of mail
    msg['Subject'] = "Stock Price Alert"
    
    fmessage = "Hey, How are you? Its {} already.".format(start_time)
    
    msg.attach(MIMEText(fmessage, 'plain')) 

    s.sendmail(fromaddr, toaddr, msg.as_string())
    
    s.quit() 

def slave(stockslist, name):

    stock = Stocks()
    
    for tickers in stockslist:
        
        stock.pipeline(tickers)
    
    stock.bulk.execute()

def main():
    stock = Stocks()
    
    start_time = datetime.datetime.now()

    Nselist = stock.get_list()

    slave1list = Nselist[:800]
    slave2list = Nselist[800:] 
    
    loom = ProcessLoom(max_runner_cap=10)

    loom.add_function(slave, [slave1list, "Slave1"],{})
    loom.add_function(slave, [slave2list, "Slave2"],{})

    loom.execute()
    
    end_time = datetime.datetime.now()
    
    totaltime = end_time - start_time
    
    stock.client.close()

    connect_email(totaltime)
    
if __name__ == "__main__":
    main()
    