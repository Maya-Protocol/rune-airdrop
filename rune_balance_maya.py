import requests
import pandas as pd
import datetime
import pytz
import time
import json
from pathlib import Path

def calculate_rune_balance_maya(day, df):
    d1 = df
    thor_addresses = set(d1['THOR Address'])
    url = f'https://mayanode.mayachain.info/mayachain/pool/THOR.RUNE/liquidity_providers?height={int(day)}'

    pool_response = requests.get(url).json()
    #print(pool_response)

    temp = pd.DataFrame(pool_response)
    temp = temp[temp['asset_address'].isin(thor_addresses)][['asset', 'asset_deposit_value', 'asset_address']]
    temp['RUNE_Balance_MY'] = temp['asset_deposit_value'].apply(pd.to_numeric) / (1 * 10 ** 8)
    temp = temp[['asset_deposit_value', 'asset_address', 'RUNE_Balance_MY']]
    #print(temp)

    d1 = pd.merge(d1[['THOR Address', 'MAYA Address']], temp, left_on="THOR Address", right_on="asset_address", how='outer')
    d1 = d1.drop(columns={'asset_address', 'asset_deposit_value'})
    print(d1)

    file_name = f"Rune_Balance_Maya_{day}"
    d1.to_csv(f'{file_name}.csv')

if __name__ == '__main__':
    height_df = pd.read_csv("heights_ref.csv")
    df = pd.read_csv("RUNE Owner Airdrop - Day 1.csv")

    for height in height_df['MAYA BLOCK']:
        if pd.isna(height) == True:
            continue
        else:
            print("Start", int(height))
            calculate_rune_balance_maya(int(height), df)
            print(f"Height {int(height)} done")
