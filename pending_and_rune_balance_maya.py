import requests
import pandas as pd
import datetime
import pytz
import time
import json
from pathlib import Path

def calculate_pending_and_rune_balance(day, df):

    def get_pending_rune_my(address, height):

        if height < 41539 or pd.isna(height) == True:
            return 0

        url = f"https://mayanode.mayachain.info/mayachain/pool/THOR.RUNE/liquidity_provider/{address}?height={height}"
        # print(url)

        response = requests.get(url).json()
        #print(response)

        if response != {}:
            return float(response['pending_asset']) / (1 * 10 ** 8)

    def get_rune_balance_my(address, height):

        if height < 41539 or pd.isna(height) == True:
            return 0

        url = f"https://mayanode.mayachain.info/mayachain/pool/THOR.RUNE/liquidity_provider/{address}?height={height}"

        response = requests.get(url).json()
        #print(response)

        if response != {}:
            return float(response['asset_deposit_value']) / (1 * 10 ** 8)
    d1=df
    d1['Pending_RUNE_MY'] = d1.apply(lambda x: get_pending_rune_my(x['THOR Address'], day), axis=1)
    d1['RUNE_Balance_MY'] = d1.apply(lambda x: get_rune_balance_my(x['THOR Address'], day), axis=1)

    file_name = f"Pending_And_Balance_Maya_{day}"
    d1.to_csv(f'{file_name}.csv')
    return

if __name__ == '__main__':
    height_df = pd.read_csv("Maya_Airdrop_Height_Ref.csv")
    df = pd.read_csv("RUNE Owner Airdrop - Day 1.csv")

    for height in height_df['MAYA BLOCK']:
        if pd.isna(height)==True:
            continue
        else:
            print("Start", int(height))
            calculate_pending_and_rune_balance(int(height), df)
            print(f"Height {int(height)} done")
            break