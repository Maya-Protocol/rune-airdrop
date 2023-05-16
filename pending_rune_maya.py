import requests
import pandas as pd
import datetime
import pytz
import time
import json
from pathlib import Path

def calculate_pending_rune_maya(day, df):
    d1 = df
    thor_addresses = set(d1['THOR Address'])
    url = f'https://mayanode.mayachain.info/mayachain/pool/THOR.RUNE/liquidity_providers?height={int(day)}'

    pool_response = requests.get(url).json()
    #print(pool_response)

    temp = pd.DataFrame(pool_response)
    temp = temp[temp['asset_address'].isin(thor_addresses)][['asset', 'pending_asset', 'asset_address']]
    temp['Pending_RUNE_MY'] = temp['pending_asset'].apply(pd.to_numeric) / (1 * 10 ** 8)
    temp = temp[['pending_asset', 'asset_address', 'Pending_RUNE_MY']]
    #print(temp)

    d1 = pd.merge(d1[['THOR Address', 'MAYA Address']], temp, left_on="THOR Address", right_on="asset_address", how='outer')
    d1 = d1.drop(columns={'asset_address', 'pending_asset'})
    print(d1)

    file_name = f"Pending_Rune_Maya_{day}"
    d1.to_csv(f'{file_name}.csv')

if __name__ == '__main__':
    height_df = pd.read_csv("Maya_Airdrop_Height_Ref.csv")
    df = pd.read_csv("RUNE Owner Airdrop - Day 1.csv")

    for height in height_df['MAYA BLOCK']:
        if pd.isna(height) == True:
            continue
        else:
            print("Start", int(height))
            calculate_pending_rune_maya(int(height), df)
            print(f"Height {int(height)} done")
