
#!/usr/bin/env python
# coding: utf-8

# # Docs
# https://midgard.ninerealms.com/v2/doc
# https://thornode.ninerealms.com/thorchain/doc
# https://mayanode.mayachain.info/mayachain/doc

import requests
import pandas as pd
import datetime
import pytz
import time
import json
from os.path import exists
from pathlib import Path


def get_rune_lp_tc(pool, address, height):
    if exists(f'./pool_files/{height}/{pool}.json'):
        with open(f'./pool_files/{height}/{pool}.json', 'r') as f:
            data = json.load(f)
            return data


height_df = pd.read_csv("heights_ref.csv")

pools_response = requests.get(
    "https://midgard.ninerealms.com/v2/pools?status=available").json()
pools = [x['asset'] for x in pools_response]
names = [{x['asset'].split("-")[0]:x['asset']} for x in pools_response]
for i in range(1, 43):
  day = height_df[height_df['DAY'] == i]['TC BLOCK'].values[0]

  if exists(f'./pool_files/results/pools_{day}.csv'):
    print(f'Skipping ./pool_files/results/pools_{day}.csv')
    continue

  day_maya = height_df[height_df['DAY'] == i]['MAYA BLOCK'].values[0]
  d1 = pd.read_csv("RUNE Owner Airdrop.csv")
  thor_addresses = set(d1['THOR Address'])


  cols = []
  for name in names:
    for k, v in name.items():
      pool_response = get_rune_lp_tc(k, d1['THOR Address'], day)
      temp = pd.DataFrame(pool_response)
      print(f'height: {day} pool: {v}')
      temp = temp[temp['rune_address'].isin(thor_addresses)][[
          'asset', 'rune_deposit_value', 'rune_address']]
      temp[f'rune_deposit_value_{k}'] = temp['rune_deposit_value'].apply(
          pd.to_numeric)/(1*10**8)
      temp = temp[[f'rune_deposit_value_{k}', 'rune_address']]
      cols.append(f"rune_deposit_value_{k}")
      d1 = pd.merge(d1, temp, left_on="THOR Address",
                    right_on="rune_address", how='outer')
      d1 = d1.drop(columns={'rune_address'})
    d1[cols] = d1[cols].fillna(0)
  d1.to_csv(f'./pool_files/results/pools_{day}.csv', index=False)
