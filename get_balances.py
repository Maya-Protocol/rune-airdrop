
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
import os
from pathlib import Path


def get_rune_balance_tc(address, height):
    if os.stat(f'balances/{height}/{address}.json').st_size == 0:
        return 0

    with open(f'balances/{height}/{address}.json') as f:
        # Check if file is empty
        print(address, height)
        data = json.load(f)
        for coin in data['coins']:
            if coin['asset'] == 'THOR.RUNE':
                return float(coin['amount'])/(1*10**8)
    return 0


d1 = pd.read_csv("RUNE Owner Airdrop.csv")
height_df = pd.read_csv("Maya_Airdrop_Height_Ref.csv")

for h in height_df['TC BLOCK']:
    d1['Rune_Balance_TC'] = d1.apply(
        lambda x: get_rune_balance_tc(x['THOR Address'], h), axis=1)
    d1.to_csv(f'balances/results/balances_{h}.csv')
