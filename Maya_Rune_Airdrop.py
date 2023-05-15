#!/usr/bin/env python

import requests
import pandas as pd
import datetime
import pytz
import time
from pathlib import Path
#from datetime import datetime, timezone


height_df = pd.read_csv("Maya_Airdrop_Height_Ref.csv")
day_1_height = height_df[height_df['DAY']=="Day 1"]['TC BLOCK'].values[0] #9857265
day = height_df[height_df['DAY']=="Day 1"]['TC BLOCK'].values[0]
day_maya = height_df[height_df['DAY']=="Day 1"]['MAYA BLOCK'].values[0]
d1 = pd.read_csv("RUNE Owner Airdrop - Day 1.csv")

def get_rune_balance_tc(address, height):
    #url = f'https://thornode-v1.ninerealms.com/bank/balances/{address}?height={height}'
    url = f'https://midgard.ninerealms.com/v2/balance/{address}?height={height}'
    response = requests.get(url).json()
    print(response)
    
    if response!={}:
        if response['coins']==[]:
            return 0
        return float(response['coins'][0]['amount'])/(1*10**8)


d1['Rune_Balance_TC'] = d1.apply(lambda x: get_rune_balance_tc(x['THOR Address'], day), axis=1 )

pools_response = requests.get("https://midgard.ninerealms.com/v2/pools?status=available").json()

pools = [x['asset'] for x in pools_response]
names = [{x['asset'].split("-")[0]:x['asset']} for x in pools_response]

def get_rune_lp_tc(pool, address, height):
    
    url = f"https://thornode.ninerealms.com/thorchain/pool/{pool}/liquidity_provider/{address}?height={height}"
    print(url)
    print(day)
    
    response = requests.get(url).json()
    print(response)
    print(response['rune_address'] ,response['asset'], response['rune_deposit_value'] )
    
    if response!={}:
        return float(response['rune_deposit_value'])/(1*10**8)

for name in names:
    for k, v in name.items():
        print(k)
        col_name = k
        
        d1[col_name] = d1.apply(lambda x: get_rune_lp_tc(v, x['THOR Address'], day), axis=1)
    
    time.sleep(1)
    print("wait 1s")
    #break



lp_cols = [x['asset'].split("-")[0] for x in pools_response]
d1[lp_cols].sum()
d1['RUNE_deposit_value_TC'] = d1[lp_cols].sum(axis=1)

thor_addresses = set(d1['THOR Address'])
thor_addresses

node_list = []

url = f"https://thornode-v1.ninerealms.com/thorchain/nodes?height={day}"
nodes_response = requests.get(url).json()
nodes_response

for node in nodes_response:
    if (node['status']=="Active" or node['status']=="Standby") and float(node['total_bond'])>=(300000*(1*10**8)):
        
        node_list.append({'pub_key_set':node['pub_key_set'],
                          'node_address':node['node_address'],
                          'node_operator_address':node['node_operator_address'],
                          'total_bond':node['total_bond'],
                          'bond_providers':node['bond_providers']
                         })
    else:
        print("Skip node")
        continue
        


# Add Flags
for node in node_list:
    if node['node_address'] in thor_addresses:
        node.update({'node_address_in_final_list':True})
    else:
        node.update({'node_address_in_final_list':False})
        
    if node['node_operator_address'] in thor_addresses:
        node.update({'node_operator_address_in_final_list':True})
    else:
        node.update({'node_operator_address_in_final_list':False})


# # Add Flags Bond Provider Addresses

for node in node_list:
    if node['bond_providers']!={}:
        for bond_p in node['bond_providers']['providers']:
            if bond_p['bond_address'] in thor_addresses:
                bond_p.update({'bond_address_in_final_list':True})
            else:
                bond_p.update({'bond_address_in_final_list':False})

# ## Pay Bond Providers
for node in node_list:
    if node['bond_providers']!={}:
        for bond_p in node['bond_providers']['providers']:
            if bond_p['bond_address_in_final_list']==True:
                bond_p.update({"payment":bond_p['bond']})
            else:
                 bond_p.update({"payment":0})


# ## Pay Left over
for node in node_list:
    if node['bond_providers']!={}:
        sum_paid = 0
        for bond_p in node['bond_providers']['providers']:
            sum_paid += int(bond_p['payment'])
        if sum_paid < float(node['total_bond']):
            print("Left Over")
            left_over = int(node['total_bond'])- sum_paid
            #print(left_over)
            #print("-----")
            
            if node['node_address_in_final_list']==False and node['node_operator_address_in_final_list']==False:
                node.update({"node_address_payment":0})
                node.update({"node_operator_payment":0})
            
            elif node['node_address_in_final_list']==False and node['node_operator_address_in_final_list']==True:
                node.update({"node_address_payment":0})
                node.update({"node_operator_payment":left_over})
            
            elif node['node_address_in_final_list']==True and node['node_operator_address_in_final_list']==False:
                node.update({"node_address_payment":left_over})
                node.update({"node_operator_payment":0})
            
            elif node['node_address_in_final_list']==True and node['node_operator_address_in_final_list']==True:
                node.update({"node_address_payment":0})
                node.update({"node_operator_payment":left_over})
            

bonded_rune = []

for node in node_list:
    #print(node)
    if 'node_address_payment' in node.keys() and int(node['node_address_payment'])>0:
        bonded_rune.append({'thorchain_address':node['node_address'],
                            'Bonded_RUNE_TC':node['node_address_payment']})
        
    if 'node_operator_payment' in node.keys() and int(node['node_operator_payment'])>0:
        bonded_rune.append({'thorchain_address':node['node_operator_address'],
                            'Bonded_RUNE_TC':node['node_operator_payment']})
        
    for node_p in node['bond_providers']['providers']:
        if int(node_p['payment'])>0:
            bonded_rune.append({'thorchain_address':node_p['bond_address'],
                               'Bonded_RUNE_TC':node_p['payment']})
            
            

bonded_df = pd.DataFrame(bonded_rune)

bonded_df['Bonded_RUNE_TC'] = bonded_df['Bonded_RUNE_TC'].apply(pd.to_numeric)
bonded_df['Bonded_RUNE_TC'] = bonded_df['Bonded_RUNE_TC']/(1*10**8)

bonded_df['Bonded_RUNE_TC'].sum()

bonded_df.to_csv('bonded_rune_day1.csv')


# # Pending_RUNE_MY
def get_pending_rune_my(address, height):
    
    if height<41539 or pd.isna(height)==True:
        return 0
    
    url = f"https://mayanode.mayachain.info/mayachain/pool/THOR.RUNE/liquidity_provider/{address}?height={height}"
    #https://mayanode.mayachain.info/mayachain/pool/BTC.BTC/liquidity_provider/maya1f6cl37zggsu9mja6p4kgnvv35u0ddywju4x97f?height=41539
    #print(url)
    
    response = requests.get(url).json()
    print(response)
    
    if response!={}:
        return float(response['pending_asset'])/(1*10**8)


d1['Pending_RUNE_MY'] = d1.apply(lambda x: get_pending_rune_my(x['THOR Address'], day_maya), axis=1 )


# # RUNE_Balance_MY
def get_rune_balance_my(address, height):
    
    if height<41539 or pd.isna(height)==True:
        return 0
    
    url = f"https://mayanode.mayachain.info/mayachain/pool/THOR.RUNE/liquidity_provider/{address}?height={height}"
    
    response = requests.get(url).json()
    print(response)
    
    if response!={}:
        return float(response['asset_deposit_value'])/(1*10**8)


d1['RUNE_Balance_MY'] = d1.apply(lambda x: get_rune_balance_my(x['THOR Address'], day_maya), axis=1 )

d1.to_csv("day1_draft.csv")
