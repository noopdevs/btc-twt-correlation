from os import sep
import tweepy, sys
import pandas as pd
from openpyxl import load_workbook
from time import sleep
from pandas import concat
import csv, re
import numpy as np

consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""
bearer_token = ""


names_file = r"~Twitter\V2 - mémoire\1 - names_file.csv"
not_found_names_file = r"~Twitter\V2 - mémoire\1 - not_found_names_file.csv"
usernames_file = r"~Twitter\V2 - mémoire\1 - usernames_file.csv"
BDD_raw_file = r"~Twitter\V2 - mémoire\2 - BDD raw suite.csv"
done_usernames_file = r"~Twitter\V2 - mémoire\2 - done_usernames_file.csv"
prices_file = r"~Twitter\V2 - mémoire\4 - full_prices.csv"
result_file = r"~Twitter\V2 - mémoire\4 - result.csv"
filtered_BDD_file = r"~Twitter\V2 - mémoire\2 - BDD filtered + NLP.csv"

# Make list with #, $ and nothing for each element
for i in range(1):

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    # the query to be searched
    elements = ['BTC', 'Bitcoin', 'Etherum', 'ETH', "crypto", "cryptocurrency", "cryptocurrencies", "altcoin", "Uniswap", 
                    "Polkadot", "1inch", "PancakeSwap", "SushiSwap", "Ripple", "Algorand", "MATIC", "Chainlink", "BCH", 
                        "Cardano", "Shibtoken", "Dogecoin", "OBJBTC ", "NFT", "BNB", "Tezos", "USDC"]
    hashtag = ["#", "$"]
    tweets_filter = []
    for text in elements:
        for tag in hashtag :
            q = tag + text
            tweets_filter.append(q)
        tweets_filter.append(text)
    print(tweets_filter)

    # Regex to clean emoticon
    emoji_pattern = re.compile("["
            u"\U0001F600-\U0001F64F"  # emoticons
            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
            u"\U0001F680-\U0001F6FF"  # transport & map symbols
            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                            "]+", flags=re.UNICODE)

    text = u'This dog \U0001f602'
    text = emoji_pattern.sub(r'', text)
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
    # DF BDD
    try : 
        BDD = pd.read_csv(BDD_raw_file, sep=',')
        print("BDD read OK")
    except Exception as e: 
        # print(e)
        print("BDD CREATED")
        BDD = pd.DataFrame(columns=['username', 'created_at', 'text'])
    # DF Done Usernames
    try : 
        df_done_usernames = pd.read_csv(done_usernames_file, sep=',')
        print("df_done_usernames read OK")
    except Exception as e: 
        # print(e)
        print("df_done_usernames CREATED")
        df_done_usernames = pd.DataFrame(columns=['username'])


def save_to_csv(dataframe, file):
    try : 
        dataframe.to_csv(file, index = False, encoding='utf-8-sig')
        print("Dataframe saved corretly")
    except Exception as e:
        print(e)
        backup_file = f"{file[0: -4]}_backup.csv"
        dataframe.to_csv(backup_file, index = False, encoding='utf-8-sig')


def get_all_tweets(screen_name, BDD, category):
    intermediate_df = pd.DataFrame()

    print("Scraping : ", screen_name)

    try : 
        df = pd.DataFrame()
        for tweet in tweepy.Cursor(api.user_timeline, screen_name=screen_name, count=200, tweet_mode="extended").items():
            row = {}
            row.update({'ID' : tweet.id_str, 'created_at': tweet.created_at, 'text' : tweet.full_text})
            df = df.append(row, ignore_index = True)
    except Exception as e: 
        print(e)
        sys.exit()


    row = {}
    for x in range(len(df)):
        text = df.iloc[x, df.columns.get_loc("text")]
        text = emoji_pattern.sub(r'', text)
        text = text.replace('\n', ' ')
        text = text.replace(',', ' ')
        text = text.replace(';', ' ')   
        text = text.replace('\n', ' ').replace('\r', '')
        text = text.replace('“', ' ').replace('"', ' ')  
        text = re.sub(r"[A-Za-z\.]*[0-9]+[A-Za-z%°\.]*", "", text)
        text = re.sub(r"(\s\-\s|-$)", "", text)
        text = re.sub(r"[,\!\?\%\(\)\/\"]", "", text)
        text = re.sub(r"\&\S*\s", "", text)
        text = re.sub(r"\&", "", text)
        text = re.sub(r"\+", "", text)
        text = re.sub(r"\#", "", text)
        text = re.sub(r"\$", "", text)
        text = re.sub(r"\£", "", text)
        text = re.sub(r"\%", "", text)
        text = re.sub(r"\:", "", text)
        text = re.sub(r"\@", "", text)
        text = re.sub(r"\-", "", text)    

        row.update({'ID' : df.iloc[x, 0], 'created_at': df.iloc[x, 1], 'text':text, 'username':screen_name, 'category' : category}) 

        intermediate_df = intermediate_df.append(row, ignore_index = True)


    if intermediate_df.empty == True : print("No Bitcoin tweets for ", username) ; return BDD
    frames = [BDD, intermediate_df]
    result = concat(frames)
    BDD = result
    BDD = BDD.reset_index(drop=True)
    print("BBD : \n", BDD.tail(2))

    save_to_csv(BDD, BDD_raw_file)

    return BDD



df_usernames = df_usernames.reset_index(drop=True)
for x in range(len(df_usernames)):

    username = df_usernames['username'].iloc[x]
    category = df_usernames['Category'].iloc[x]
    if df_done_usernames.empty == False : 
        row = ((df_done_usernames['username'] == username)).any()
        if row == True : continue
    

    BDD = get_all_tweets(username, BDD, category)

    row = {}
    row.update({'username': username})
    df_done_usernames = df_done_usernames.append(row, ignore_index=True)
    save_to_csv(df_done_usernames, done_usernames_file)
    
