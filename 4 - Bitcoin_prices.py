from calendar import month
from copy import copy
from operator import index
from os import sep
from time import sleep
from pandas import DataFrame, concat
from random import randrange
from requests.auth import AuthBase
import pandas as pd
import numpy as np
import datetime as dt
import hmac, time, hashlib, requests, base64, tweepy, sys

consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

names_file = r"~Twitter\V2 - mémoire\1 - names_file.csv"
not_found_names_file = r"~Twitter\V2 - mémoire\1 - not_found_names_file.csv"
usernames_file = r"~Twitter\V2 - mémoire\1 - usernames_file.csv"
BDD_raw_file = r"~Twitter\V2 - mémoire\2 - BDD raw - Copie (2).csv"
done_usernames_file = r"~Twitter\V2 - mémoire\2 - done_usernames_file.csv"
prices_file = r"~Twitter\V2 - mémoire\4 - full_prices.csv"
result_file = r"~Twitter\V2 - mémoire\4 - result.csv"
filtered_BDD_file = r"~Twitter\V2 - mémoire\3 - BDD NLP 2 - Copie.csv"


# Make list with #, $ and nothing for each element
for i in range(1):

    #Dataframes
    # DF names
    try : 
        df_names = pd.read_csv(names_file, sep=',')
        print("df_names read OK")
    except Exception as e: 
        # print(e)
        sys.exit()
    # DF not found names
    try : 
        df_not_found = pd.read_csv(not_found_names_file, sep=',')
        print("not_found_names read OK")
    except Exception as e: 
        # print(e)
        print("not_found_names CREATED")
        df_not_found = pd.DataFrame(columns=['Name'])
    # DF usernames found
    try : 
        df_usernames = pd.read_csv(usernames_file, sep=',')
        print("df_usernames read OK")
    except Exception as e: 
        # print(e)
        print("df_usernames CREATED")
        df_usernames = pd.DataFrame(columns=['Name', 'username'])

    try : 
        df_done_usernames = pd.read_csv(done_usernames_file, sep=',')
        print("df_done_usernames read OK")
    except Exception as e: 
        # print(e)
        print("df_done_usernames CREATED")
        df_done_usernames = pd.DataFrame(columns=['username'])
    # DF prices
    try : 
        df_prices = pd.read_csv(prices_file, sep=',')
        print("df_prices read OK")
    except Exception as e: 
        print(e)
        sys.exit()


class CoinbaseExchangeAuth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or b'').decode()
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest()).decode()

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })
        return request


def save_to_csv(dataframe, file, index = False):
    try : 
        dataframe.to_csv(file, index = index, encoding='utf-8-sig')
        print("Dataframe saved corretly")
    except Exception as e:
        print(e)
        backup_file = f"{file[0: -4]}_backup.csv"
        dataframe.to_csv(backup_file, index = index, encoding='utf-8-sig')


def epoch2human(epoch):
    return dt.datetime.fromtimestamp(epoch)


def get_6h_prices(tweet_date):

    API_KEY = ""
    API_SECRET =""
    API_PASS = ""

    auth = CoinbaseExchangeAuth(API_KEY, API_SECRET, API_PASS)

    start = tweet_date
    start = start.replace(second=0, microsecond=0)
    end_CB = start + dt.timedelta(hours=6)

    str_end = end_CB.isoformat()
    str_start = start.isoformat()


    # Make the request
    url = "https://api.pro.coinbase.com/products/"+ "ETH-EUR" + "/candles?start=" + str_start + "Z&end=" + str_end + "Z&granularity=" + str(300)
    print(url)

    extracted_data = requests.get(url, auth=auth).json()
    if not extracted_data : return 0


    timing, low, high, open, close, volume = map(list, zip(*extracted_data))

    humanTimes = [epoch2human(t) for t in timing]
    df_prices = DataFrame({'Time': humanTimes, 'Close': close})
    df_prices['Close'] = round(df_prices['Close'], 2)

    return df_prices


BDD = pd.read_csv(r"~Twitter\V2 - mémoire\3 - BDD NLP.csv", sep=',')

df_prices['Time'] = pd.to_datetime(df_prices['Time'])
df_prices = df_prices.sort_values(by='Time')
BDD['created_at'] = pd.to_datetime(BDD['created_at'])
BDD = BDD.dropna()
BDD = BDD.sort_values(by='created_at')

BDD['6h rdm (%)'] = 0
BDD['6h max gap (%)'] = 0
BDD = BDD.reset_index(drop=True)
print(BDD)


for index, row in BDD.iterrows():
    
    print(index)
    try : 
        df = df_prices.copy()
        tweet_date = row["created_at"]
        end = tweet_date + dt.timedelta(hours=6)
        mask = (df['Time'] >= tweet_date) & (df['Time'] <= end)
        df = df.loc[mask]
        if df.empty == True : continue

        rendement = np.round(((df['Close'].iloc[-1] - df['Close'].iloc[0]) / df['Close'].iloc[0] ) * 100, decimals=2)
        print(index, " ", rendement)

        BDD["6h rdm (%)"].iloc[index] = rendement

        # Set the biggest gap in price during the 6 hours (negative & positive)
        prices = df['Close'].tolist()

        cumax = np.maximum.accumulate
        min_return = np.min((prices - cumax(prices)) / cumax(prices)  * 100 )
        min_return = np.round(min_return, decimals=2)

        cummin = np.minimum.accumulate
        max_return = np.max((prices - cummin(prices)) / cummin(prices)  * 100 )
        max_return = np.round(max_return, decimals=2)
        maximum = max_return if max_return > -min_return else min_return
        BDD['6h max gap (%)'].iloc[index] = maximum

    except KeyboardInterrupt :
        print(BDD)
        BDD = BDD[BDD["6h rdm (%)"] != 0] 
        print(BDD)
        save_to_csv(BDD, r"~Twitter\V2 - mémoire\4 - BDD prices.csv")
        print("DF saved")
        break
    
    except Exception as e:
        print(e)
        print('useranme : ', row['username'])
        print('text : ', row['text'])
        print('date : ', row['created_at'])
        print(index)
        save_to_csv(BDD, r"~Twitter\V2 - mémoire\4 - BDD prices.csv")
        sys.exit()

print("Suite now")
BDD = BDD[BDD["6h rdm (%)"] != 0] 
print(BDD)
save_to_csv(BDD, r"~Twitter\V2 - mémoire\4 - BDD prices.csv")


# Correlaiton rule :
BDD['Correlated'] = np.where( ((BDD['Sentiment'] > 0) & (BDD['6h max gap (%)'] > 0)) | ((BDD['Sentiment'] < 0) & (BDD['6h max gap (%)'] < 0))  , 1, 0)
print(BDD)


df_final_usernames = pd.DataFrame()
unique_usernames = BDD['username'].unique()

for username in unique_usernames:
    row = {}

    copy_BDD = BDD[BDD['username'] == username] 

    total_tweets = len(copy_BDD)
    correlated = copy_BDD['Correlated'].sum()
    gaps = copy_BDD['6h max gap (%)'].tolist()
    gaps = [abs(number) for number in gaps]
    mean_impact = sum(gaps) / float(len(gaps))


    row.update({'username' : username, 'total tweet': int(total_tweets), 'correlated tweets' : int(correlated), 'mean impact' : mean_impact}) 
    df_final_usernames = df_final_usernames.append(row, ignore_index = True)


# Final filter : remove coincidences
df_final_usernames['correlated tweets (%)'] = np.round((df_final_usernames['correlated tweets'] / df_final_usernames['total tweet'] ) * 100, decimals=2)
df_final_usernames['mean impact'] = np.round(df_final_usernames['mean impact'], decimals= 2)
df_final_usernames = df_final_usernames[df_final_usernames['correlated tweets (%)'] > 60]
df_final_usernames = df_final_usernames[df_final_usernames['correlated tweets'] > 2]
df_final_usernames = df_final_usernames.sort_values(by ='correlated tweets (%)', ascending=False)
df_final_usernames = df_final_usernames.set_index('username')
print(df_final_usernames)
save_to_csv(df_final_usernames, result_file, index=True)
