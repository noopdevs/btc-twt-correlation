import tweepy, sys
import pandas as pd
from time import sleep
from pandas import concat
import re
import numpy as np

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

    #Dataframes
    # DF names
    try : 
        df_names = pd.read_csv(names_file, sep=',')
        print("df_names read OK")
    except Exception as e: 
        print(e)
        sys.exit()
    # DF not found names
    try : 
        df_not_found = pd.read_csv(not_found_names_file, sep=',')
        print("not_found_names read OK")
    except Exception as e: 
        print(e)
        print("not_found_names CREATED")
        df_not_found = pd.DataFrame()
    # DF usernames found
    try : 
        df_usernames = pd.read_csv(usernames_file, sep=',')
        print("df_usernames read OK")
    except Exception as e: 
        print(e)
        print("df_usernames CREATED")
        df_usernames = pd.DataFrame()
    # DF BDD
    try : 
        BDD = pd.read_csv(BDD_raw_file, sep=',')
        print("BDD read OK")
    except Exception as e: 
        print(e)
        print("BDD CREATED")
        BDD = pd.DataFrame()
    # DF Done Usernames
    try : 
        df_done_usernames = pd.read_csv(done_usernames_file, sep=',')
        print("df_done_usernames read OK")
    except Exception as e: 
        # print(e)
        print("df_done_usernames CREATED")
        df_done_usernames = pd.DataFrame()

def save_to_csv(dataframe, file):
    try : 
        dataframe.to_csv(file, index = False, encoding='utf-8-sig')
        print("Dataframe saved corretly")
    except Exception as e:
        print(e)
        backup_file = f"{file[0: -4]}_backup.csv"
        dataframe.to_csv(backup_file, index = False, encoding='utf-8-sig')



df_names = df_names.drop_duplicates(subset ="Users")
print(df_names)

# Script to get Twitter usernames 
try :
    # search the query
    search_list = df_names['Users']
    search_list = df_names.reset_index()[['Users', 'Category']].values.tolist()
    for q in search_list : 
        
        name = q[0]
        category = q[1]

        users = api.search_users(name, 1, wait_on_rate_limit = True, wait_on_rate_limit_notify = True)

        # print the users retrieved
        for user in users:
            row = {}
            if (user.verified == True) or (user.followers_count > 5000) : 
                row.update({'Name' : name, 'Category' : category, 'username': user.screen_name})
                print("Searching for ", name, " FOUND : ", user.screen_name)
                sys.exit()
                df_usernames = df_usernames.append(row, ignore_index=True)

            else : 
                row.update({'Name' : name})
                print("Searching for ", name, " NOT FOUND")
                df_not_found = df_not_found.append(row, ignore_index=True)
except Exception as e:
    print(e)
    print("df_usernames : \n", df_usernames)
    print("df_not_found : \n", df_not_found)
    save_to_csv(df_usernames, usernames_file)
    save_to_csv(df_not_found, not_found_names_file)
    sys.exit()

print("df_usernames : \n", df_usernames)
print("df_not_found : \n", df_not_found)
save_to_csv(df_usernames, usernames_file)
save_to_csv(df_not_found, not_found_names_file)
