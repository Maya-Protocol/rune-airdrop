import requests
import pandas as pd
import datetime
import pytz
import time
import json
from pathlib import Path

def calculate_bonded(day, d1):

    thor_addresses = set(d1['THOR Address'])

    node_list = []

    url = f"https://thornode-v1.ninerealms.com/thorchain/nodes?height={day}"
    nodes_response = requests.get(url).json()

    for node in nodes_response:
        if (node['status'] == "Active" or node['status'] == "Standby") and float(node['total_bond']) >= (
                300000 * (1 * 10 ** 8)):

            node_list.append({'pub_key_set': node['pub_key_set'],
                              'node_address': node['node_address'],
                              'node_operator_address': node['node_operator_address'],
                              'total_bond': node['total_bond'],
                              'bond_providers': node['bond_providers']
                              })
        else:
            #print("Skip node")
            continue

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

    # Add Flags Bond Provider Addresses
    for node in node_list:
        if node['bond_providers'] != {}:
            for bond_p in node['bond_providers']['providers']:
                if bond_p['bond_address'] in thor_addresses:
                    bond_p.update({'bond_address_in_final_list': True})
                else:
                    bond_p.update({'bond_address_in_final_list': False})

    # Pay Bond Providers
    for node in node_list:
        if node['bond_providers'] != {}:
            for bond_p in node['bond_providers']['providers']:
                if bond_p['bond_address_in_final_list'] == True:
                    bond_p.update({"payment": bond_p['bond']})
                else:
                    bond_p.update({"payment": 0})

    # Pay Left Over
    for node in node_list:
        if node['bond_providers'] != {}:
            sum_paid = 0
            for bond_p in node['bond_providers']['providers']:
                sum_paid += int(bond_p['payment'])
            if sum_paid < float(node['total_bond']):
                print("Left Over")
                left_over = int(node['total_bond']) - sum_paid
                # print(left_over)
                # print("-----")

                if node['node_address_in_final_list'] == False and node['node_operator_address_in_final_list'] == False:
                    node.update({"node_address_payment": 0})
                    node.update({"node_operator_payment": 0})

                elif node['node_address_in_final_list'] == False and node[
                    'node_operator_address_in_final_list'] == True:
                    node.update({"node_address_payment": 0})
                    node.update({"node_operator_payment": left_over})

                elif node['node_address_in_final_list'] == True and node[
                    'node_operator_address_in_final_list'] == False:
                    node.update({"node_address_payment": left_over})
                    node.update({"node_operator_payment": 0})

                elif node['node_address_in_final_list'] == True and node['node_operator_address_in_final_list'] == True:
                    node.update({"node_address_payment": 0})
                    node.update({"node_operator_payment": left_over})

    # Make Final DF
    bonded_rune = []

    for node in node_list:
        # print(node)
        if 'node_address_payment' in node.keys() and int(node['node_address_payment']) > 0:
            bonded_rune.append({'thorchain_address': node['node_address'],
                                'Bonded_RUNE_TC': node['node_address_payment']})

        if 'node_operator_payment' in node.keys() and int(node['node_operator_payment']) > 0:
            bonded_rune.append({'thorchain_address': node['node_operator_address'],
                                'Bonded_RUNE_TC': node['node_operator_payment']})

        for node_p in node['bond_providers']['providers']:
            if int(node_p['payment']) > 0:
                bonded_rune.append({'thorchain_address': node_p['bond_address'],
                                    'Bonded_RUNE_TC': node_p['payment']})

    bonded_df = pd.DataFrame(bonded_rune)
    bonded_df['Bonded_RUNE_TC'] = bonded_df['Bonded_RUNE_TC'].apply(pd.to_numeric)
    bonded_df['Bonded_RUNE_TC'] = bonded_df['Bonded_RUNE_TC'] / (1 * 10 ** 8)

    bonded_final = bonded_df.groupby(['thorchain_address']).sum()
    bonded_final = bonded_final.reset_index()

    result_df = pd.merge(d1[['THOR Address','MAYA Address']], bonded_final, left_on="THOR Address", right_on='thorchain_address', how='outer')
    result_df = result_df.drop(columns={'thorchain_address'})

    file_name = f"Bonded_TC_{day}"
    result_df.to_csv(f'{file_name}.csv')
    return

if __name__ == '__main__':
    height_df = pd.read_csv("heights_ref.csv")
    df = pd.read_csv("RUNE Owner Airdrop - Day 1.csv")

    for height in height_df['TC BLOCK']:
        calculate_bonded(height, df)
        print(f"Height {height} done")