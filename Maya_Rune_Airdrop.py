#!/usr/bin/env python
# coding: utf-8

# # Docs
# https://midgard.ninerealms.com/v2/doc
# https://thornode.ninerealms.com/thorchain/doc
# https://mayanode.mayachain.info/mayachain/doc

# # Libraries

# In[8]:


from os.path import exists
import requests
import pandas as pd
import datetime
import pytz
import time
import json
from pathlib import Path
# from datetime import datetime, timezone


# # Height

# In[9]:


height_df = pd.read_csv("Maya_Airdrop_Height_Ref.csv")
# ## Select date
day_1_height = height_df[height_df['DAY']
    == 1]['TC BLOCK'].values[0]  # 9857265
day_1_height


# In[38]:


day = height_df[height_df['DAY'] == 1]['TC BLOCK'].values[0]
day


# In[12]:


day_maya = height_df[height_df['DAY'] == 1]['MAYA BLOCK'].values[0]


# # Read csv

# In[13]:


d1 = pd.read_csv("RUNE Owner Airdrop.csv")
d1


# # Rune Balance

# In[14]:


# In[31]:


# def get_rune_balance_tc(address, height):
#    #url = f'https://thornode-v1.ninerealms.com/bank/balances/{address}?height={height}'
#    url = f'https://midgard.ninerealms.com/v2/balance/{address}?height={height}'
#    response = requests.get(url).json()
#    print(response)
#
#    if response!={}:
#        if response['coins']==[]:
#            return 0
#        return float(response['coins'][0]['amount'])/(1*10**8)

def get_rune_balance_tc(address, height):
    with open(f'balances/{height}/{address}.json') as f:
        data = json.load(f)
        print(data)
        for coin in data['coins']:
            if coin['asset'] == 'THOR.RUNE':
                return float(coin['amount'])/(1*10**8)
    return 0


# In[32]:


get_rune_balance_tc(d1['THOR Address'][0], day_1_height)


# In[33]:


get_rune_balance_tc(d1['THOR Address'][12], day_1_height)


# In[34]:


for h in height_df['TC BLOCK']:
    d1['Rune_Balance_TC'] = d1.apply(
        lambda x: get_rune_balance_tc(x['THOR Address'], h), axis=1)
    d1.to_csv(f'balances/{day}/balances.csv')


# # Pools TC

# In[41]:


pools_response = requests.get(
    "https://midgard.ninerealms.com/v2/pools?status=available").json()
pools_response


# In[42]:


pools = [x['asset'] for x in pools_response]
pools


# In[43]:


names = [{x['asset'].split("-")[0]:x['asset']} for x in pools_response]
names


# # Rune LP

# In[175]:


def get_rune_lp_tc(pool, address, height):

    url = f"https://thornode.ninerealms.com/thorchain/pool/{pool}/liquidity_provider/{address}?height={height}"
    print(url)
    print(day)

    response = requests.get(url).json()
    print(response)
    print(response['rune_address'], response['asset'],
          response['rune_deposit_value'])

    if response != {}:
        return float(response['rune_deposit_value'])/(1*10**8)


# In[177]:


get_rune_lp_tc("BTC.BTC", "thor1egxvam70a86jafa8gcg3kqfmfax3s0m2g3m754", day)


# In[178]:


# for name in names:
#     for k, v in name.items():
#         print(k)
#         col_name = k

#         d1[col_name] = d1.apply(lambda x: get_rune_lp_tc(v, x['THOR Address'], day), axis=1)

#     time.sleep(1)
#     print("wait 1s")
#     #break


# In[49]:


# In[52]:


for h in height_df['TC BLOCK']:
    for name in names:
        for k, v in name.items():
            if exists(f'pool_files/{h}/{k}.json'):
                print(f'Skipping {v}.json at {h}')
                continue
            url = f'https://thornode-v1.ninerealms.com/thorchain/pool/{v}/liquidity_providers?height={h}'
            # print(url)

            print(f'Fetching {v}.json at {h}')
            pool_response = requests.get(url).json()
            # print(pool_response)

            with open(f'pool_files/{day}/{k}.json', 'w') as outfile:
                outfile.write(json.dumps(pool_response))


# In[196]:


cols = []

for name in names:
    for k, v in name.items():
        url = f'https://thornode-v1.ninerealms.com/thorchain/pool/{v}/liquidity_providers?height={day}'
        # url = f"https://thornode.ninerealms.com/thorchain/pool/{v}/liquidity_providers?height={day}"
        # print(url)

        pool_response = requests.get(url).json()
        # print(pool_response)

        # with open(f'{k}_{day}.json', 'w') as outfile:
        #    outfile.write(json.dumps(pool_response))
        [
        temp= pd.DataFrame(pool_response)
        temp = temp[temp['rune_address'].isin(thor_addresses)][['asset', 'rune_deposit_value', 'rune_address']]
        temp[f'rune_deposit_value_{k}']= temp['rune_deposit_value'].apply(pd.to_numeric)/(1*10**8)
        temp= temp[[f'rune_deposit_value_{k}', 'rune_address']]
        print(temp)


        cols.append(f"rune_deposit_value_{k}")

        # d1[k] = d1.apply(lambda x: ss'], day), axis=1)
        d1= pd.merge(d1, temp, left_on="THOR Address", right_on="rune_address", how='outer')
        # d1[f'rune_deposit_value_{k}'].fillna(0, inplace=True)
        d1= d1.drop(columns={'rune_address'})

        # break
    # break


# In[197]:


d1[cols]= d1[cols].fillna(0)


# In[198]:


# btc_lp = pd.read_json("BTC.BTC_9857265.json")
# btc_lp


# In[199]:


# btc_lp[btc_lp['rune_address'].isin(thor_addresses)][['asset','rune_deposit_value','rune_address']]


# In[200]:


d1


# In[185]:


# merge = pd.merge(d1, temp, left_on="THOR Address", right_on="rune_address", how='outer')
# merge['rune_deposit_value_AVAX.AVAX'].fillna(0, inplace=True)
# merge


# In[188]:


[[d1[d1['rune_deposit_value_AVAX.AVAX'] != 0]


# # Sum LP Rune

# In[201]:


d1['RUNE_deposit_value_TC']= d1[cols].sum(axis=1)


# In[202]:


d1


# In[203]:


[  # d1.to_csv("day1_pools.csv")


# # Pending RUNE TC

# In[ ]:


# "https://thornode-v1.ninerealms.com/thorchain/pool/btc.btc/liquidity_providers?height=9857265"


# In[ ]:


# def get_pending_rune_tc(address, height):

#     url = f"https://thornode.ninerealms.com/thorchain/pool/BTC.BTC/liquidity_provider/{address}?height={height}"

#     response = requests.get(url).json()
#     print(response)

#     if response!={}:
#         return float(response['pending_rune'])/(1*10**8)


# In[ ]:


# get_pending_rune_tc(d1['THOR Address'][1], day_1_height)


# In[ ]:


# lp_cols = [x['asset'].split("-")[0] for x in pools_response]
# lp_cols


# In[ ]:


# d1[lp_cols].sum()


# In[ ]:


# d1['RUNE_deposit_value_TC'] = d1[lp_cols].sum(axis=1)


# In[ ]:


# def get_rune_deposit_tc(address, height):

#     url = f"https://thornode.ninerealms.com/thorchain/pool/BTC.BTC/liquidity_provider/{address}?height={height}"

#     response = requests.get(url).json()
#     print(response)

#     if response!={}:
#         return float(response['rune_deposit_value'])/(1*10**8)


# In[ ]:


# get_rune_deposit_tc(d1['THOR Address'][8], day_1_height)


# In[ ]:


# def get_members_rune(address, height):

#     url = f"https://midgard.ninerealms.com/v2/member/{address}"

#     response = requests.get(url)#.json()
#     print(response.text)

#     return response


# In[ ]:


# get_members_rune(d1['THOR Address'][2], day_1_height)


# # Bonded Rune

# In[74]:


# def get_bonded_rune(address, height):

#     url = f"https://thornode.ninerealms.com/thorchain/node/{address}?height={height}"


#     response = requests.get(url).json()
#     print(response)

#     return response['total_bond']


# In[75]:


# def extract_node_info(height):
#     url = f"https://thornode-v1.ninerealms.com/thorchain/nodes?height={height}"
#     response = requests.get(url).json()

#     print(response)

#     return response['total_bond']


# In[76]:


thor_addresses= set(d1['THOR Address'])
thor_addresses


# In[77]:


nodes_response


# In[78]:


[node_list= []

url= f"https://thornode-v1.ninerealms.com/thorchain/nodes?height={day}"
nodes_response= requests.get(url).json()
nodes_response

for node in nodes_response:
    if (node['status'] == "Active" or node['status'] == "Standby") and float(node['total_bond']) >= (300000*(1*10**8)):

        node_list.append({'pub_key_set': node['pub_key_set'],
                          'node_address':node['node_address'],
                          'node_operator_address':node['node_operator_address'],
                          'total_bond':node['total_bond'],
                          'bond_providers':node['bond_providers']
                         })
    else:
        print("Skip node")
        continue



# In[79]:


# Add Flags
for node in node_list:
    if node['node_address'] in thor_addresses:
        node.update({'node_address_in_final_list': True})
    else:
        node.update({'node_address_in_final_list': False})

    if node['node_operator_address'] in thor_addresses:
        node.update({'node_operator_address_in_final_list': True})
    else:
        node.update({'node_operator_address_in_final_list': False})


# In[80]:


# # Add Flags Bond Provider Addresses

for node in node_list:
    if node['bond_providers'] != {}:
        for bond_p in node['bond_providers']['providers']:
            if bond_p['bond_address'] in thor_addresses:
                bond_p.update({'bond_address_in_final_list': True})
            else:
                bond_p.update({'bond_address_in_final_list': False})


# In[81]:


node_list


# In[82]:


node_list[5]


# ## Pay Bond Providers

# In[83]:


96122130102265-386702201-37260368368061


# In[84]:


for node in node_list:
    if node['bond_providers'] != {}:
        for bond_p in node['bond_providers']['providers']:
            if bond_p['bond_address_in_final_list'] == True:
                bond_p.update({"payment": bond_p['bond']})
            else:
                 bond_p.update({"payment": 0})


# ## Pay Left over

# In[85]:


thor1xujcdx3vvyw9lk3jfxm7ksglxctrrk2urpe3vd	thor1xujcdx3vvyw9lk3jfxm7ksglxctrrk2urpe3vd	thor1xujcdx3vvyw9lk3jfxm7ksglxctrrk2urpe3vd for node in node_list:
    if node['bond_providers'] != {}:
        sum_paid= 0
        for bond_p in node['bond_providers']['providers']:
            sum_paid += int(bond_p['payment'])
        if sum_paid < float(node['total_bond']):
            print("Left Over")
            left_over= int(node['total_bond']) - sum_paid
            # print(left_over)
            # print("-----")

            if node['node_address_in_final_list'] == False and node['node_operator_address_in_final_list'] == False:
                node.update({"node_address_payment": 0})
                node.update({"node_operator_payment": 0})

            elif node['node_address_in_final_list'] == False and node['node_operator_address_in_final_list'] == True:
                node.update({"node_address_payment": 0})
                node.update({"node_operator_payment": left_over})

            elif node['node_address_in_final_list'] == True and node['node_operator_address_in_final_list'] == False:
                node.update({"node_address_payment": left_over})
                node.update({"node_operator_payment": 0})

            elif node['node_address_in_final_list'] == True and node['node_operator_address_in_final_list'] == True:
                node.update({"node_address_payment": 0})
                node.update({"node_operator_payment": left_over})



# In[86]:


node_list


# In[90]:


'node_address_payment' in node_list[1].keys()


# In[106]:


thor1xujcdx3vvyw9lk3jfxm7ksglxctrrk2urpe3vd	thor1xujcdx3vvyw9lk3jfxm7ksglxctrrk2urpe3vd	bonded_rune= []

for node in node_list:
    # print(node)
    if 'node_address_payment' in node.keys() and int(node['node_address_payment']) > 0:
        bonded_rune.append({'thorchain_address': node['node_address'],
                            'Bonded_RUNE_TC':node['node_address_payment']})

    if 'node_operator_payment' in node.keys() and int(node['node_operator_payment']) > 0:
        bonded_rune.append({'thorchain_address': node['node_operator_address'],
                            'Bonded_RUNE_TC':node['node_operator_payment']})

    for node_p in node['bond_providers']['providers']:
        if int(node_p['payment']) > 0:
            bonded_rune.append({'thorchain_address': node_p['bond_address'],
                               'Bonded_RUNE_TC':node_p['payment']})




# In[110]:


bonded_df= pd.DataFrame(bonded_rune)
bonded_df


# In[112]:


bonded_df['Bonded_RUNE_TC']= bonded_df['Bonded_RUNE_TC'].apply(pd.to_numeric)
bonded_df['Bonded_RUNE_TC']= bonded_df['Bonded_RUNE_TC']/(1*10**8)
bonded_df


# In[114]:


bonded_df['Bonded_RUNE_TC'].sum()


# In[113]:


bonded_df.to_csv('bonded_rune_day1.csv')


# # Pending_RUNE_MY

# In[ ]:


def get_pending_rune_my(address, height):

    if height < 41539 or pd.isna(height) == True:
        return 0

    url= f"https://mayanode.mayachain.info/mayachain/pool/THOR.RUNE/liquidity_provider/{address}?height={height}"
    # https://mayanode.mayachain.info/mayachain/pool/BTC.BTC/liquidity_provider/maya1f6cl37zggsu9mja6p4kgnvv35u0ddywju4x97f?height=41539
    # print(url)

    response= requests.get(url).json()
    print(response)

    if response != {}:
        return float(response['pending_asset'])/(1*10**8)


# In[ ]:


day_10_height= 41539


# In[ ]:


thor1xujcdx3vvyw9lk3jfxm7ksglxctrrk2urpe3vd	d1['MAYA Address'][0]


# In[ ]:


day_maya


# In[ ]:


get_pending_rune_my(d1['MAYA Address'][0], day_maya)


# In[ ]:


d1['Pending_RUNE_MY'] = d1.apply(lambda x: get_pending_rune_my(x['THOR Address'], day_maya), axis=1)


# # RUNE_Balance_MY

# In[ ]:


def get_rune_balance_my(address, height):

    if height < 41539 or pd.isna(height) == True:
        return 0

    url=f"https://mayanode.mayachain.info/mayachain/pool/THOR.RUNE/liquidity_provider/{address}?height={height}"

    response=requests.get(url).json()
    print(response)

    if response != {}:
        return float(response['asset_deposit_value'])/(1*10**8)


# In[ ]:


get_rune_balance_my(d1['MAYA Address'][0], day_10_height)


# In[ ]:


d1['RUNE_Balance_MY']=d1.apply(
    lambda x: get_rune_balance_my(x['THOR Address'], day_maya), axis=1)


# In[ ]:


d1


# In[ ]:


thor1xujcdx3vvyw9lk3jfxm7ksglxctrrk2urpe3vd	thor1xujcdx3vvyw9lk3jfxm7ksglxctrrk2urpe3vd	thor1xujcdx3vvyw9lk3jfxm7ksglxctrrk2urpe3vd	d1.to_csv("day1_draft.csv")


# In[ ]:





# In[ ]:
