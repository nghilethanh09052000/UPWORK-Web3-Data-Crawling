import requests
import json
import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Credentials
JWT_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE"
API_KEY = "hfn5rhXT5j3d2fUQXrs6Mq868HNOOipLoQsa3N6H2582gkFGeWI"
COOKIES = 'non_registered_ga_id=5220f9b9-a673-4c44-bb3f-8964f03f0ce6; _ga=GA1.1.1504934669.1763281373; cebs=1; _ce.clock_data=-27%2C27.64.23.48%2C1%2Ca2cb084c96a1ead15308a8f1e203c3be%2CEdge%2CVN; jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE; dapp_ga_id=dbdcde9c-6238-4ae7-a54e-fea32c682e3c; __cf_bm=ACN3LT.BoSyvycB_iIKd97N7o0awY8d_8hjaPuWJ71w-1763300481-1.0.1.1-1ZcqoR_F.Lmfhm5d4dKBxwJVx1m3eHiNO.NwHMMFZ1RiCofOmLsIKJ.mTSOtW3RQF07LAab24czo7ZUMcokH_DZBXEcYx.satI8pwwfb6sw; cf_clearance=d2_2Zkoc.TVK4i09bEID5jumzV9BvqsB1cMRovqroew-1763300608-1.2.1.1-aShC7EGVx9OfvjmCJnHuXMKWqZqMeeKxIZgOqbWRcnxI_W6rgd4O1B2dCMlVPuKqxL_JSyCvxAwyc0ZLO07JbOVJtjXNWuqzX3U6K.B4pXS3zdMfbIB_T9LmaI3YA_AKn33Ohpw.FDyFurseYsz11vwwCKOdjIVZuv8X5SUEjnLilhdPd7H1DArK.gJDTWnTuDOMm5Mx31OK3P3J7oXzf5naPCglg2X.jzYNSxUZd49G9g2OEvFggeMDJfOzUODh; cebsp_=79; _ce.s=v~66d5ea95637e1f1663b6f30f127b5a3c240389ab~lcw~1763300677096~vir~returning~lva~1763300617982~vpv~0~v11ls~a6a82dd0-c2ee-11f0-854a-4de087e4f113~vdva~1763337599999~v11.cs~448387~v11.s~a6a82dd0-c2ee-11f0-854a-4de087e4f113~v11.vs~66d5ea95637e1f1663b6f30f127b5a3c240389ab~v11.fsvd~eyJ1cmwiOiJkYXBwcmFkYXIuY29tLyoiLCJyZWYiOiJodHRwczovL2RhcHByYWRhci5jb20vZGFwcC93b3JsZC1vZi1keXBpYW5zOz9fX2NmX2NobF90az1UWlBEMlNySjNyeFhGQlRHRlBPZU9leUxaVDJwcURnREtadGdHQzRtb2swLTE3NjMyOTkwNjMtMS4wLjEuMS1TNjB3eDc2SHppU29Oc1pieG13eHF4MnpUMlVGSGVka2d6dk5WZkp2bU84IiwidXRtIjpbXX0%3D~v11.sla~1763299070255~v11.ws~1~v11.wr~3~v11.ss~1763299070257~v11nv~0~gtrk.la~mi1rp45k~v11e~1~lcw~1763300679752; _ga_BTQFKMW6P9=GS2.1.s1763299069$o2$g1$t1763300679$j13$l0$h0; dr_session={"sessionId":"a38bfe6d-a967-41ca-ba39-402a23958c3a","sessionNumber":2,"landingPage":"/404","campaignSource":"{\"source\":null,\"medium\":null,\"campaign\":null}","lastActivity":1763300680297}'

headers = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Accept": "application/json",
    "x-api-sk": API_KEY,
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
    "Cookie": COOKIES
}

# Load chain mapping
with open('all_chains.json', 'r') as f:
    chains_data = json.load(f)

chain_mapping = {}
for chain in chains_data:
    chain_id = chain.get('chainId')
    token_slug = chain.get('tokenSlug')
    if chain_id and token_slug:
        chain_mapping[chain_id] = token_slug

print(f"Loaded {len(chain_mapping)} chain mappings")

# Thread-safe counters
lock = threading.Lock()
success_count = 0
error_count = 0
completed_count = 0

def process_dapp_chain(dapp_chain_data):
    """Process a single dapp-chain combination"""
    global success_count, error_count, completed_count
    
    i, dapp, chain_id, total, chain_mapping, category_name = dapp_chain_data
    slug = dapp.get('slug')
    name = dapp.get('name')
    chain_name = chain_mapping.get(chain_id, f'chain_{chain_id}')
    
    if not slug:
        with lock:
            error_count += 1
            completed_count += 1
            print(f"{completed_count}/{total}: Skipping {name} (no slug)")
        return []
    
    url = f"https://dapps-rankings.dappradar.com/api/v1.0/dapp/{slug}/history/all"
    params = {
        "currency": "USD",
        "chainId[]": chain_id
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            history_data = response.json()
            csv_data = history_data.get('csvData', {})
            data_list = csv_data.get('data', [])
            
            if data_list:
                rows = []
                for row in data_list:
                    csv_row = {
                        'Date': row.get('day', ''),
                        'Dapp Name': name,
                        'Chain Name': chain_name,
                        'UAW': row.get('uaw', ''),
                        'Transactions': row.get('transactions', ''),
                        'Volume': row.get('volume', ''),
                        'Category': category_name
                    }
                    rows.append(csv_row)
                
                with lock:
                    success_count += 1
                    completed_count += 1
                    print(f"{completed_count}/{total}: ✅ {name} on {chain_name} - {len(rows)} rows")
                return rows
            else:
                with lock:
                    error_count += 1
                    completed_count += 1
                    print(f"{completed_count}/{total}: ⚠️  {name} on {chain_name} - No data")
                return []
        else:
            with lock:
                error_count += 1
                completed_count += 1
                print(f"{completed_count}/{total}: ❌ {name} on {chain_name} - Status {response.status_code}")
            return []
        
    except Exception as e:
        with lock:
            error_count += 1
            completed_count += 1
            print(f"{completed_count}/{total}: ❌ {name} on {chain_name} - {e}")
        return []

def fetch_history(category_name):
    """Fetch history for a category"""
    
    # Check input file
    input_file = f"data_{category_name}/all_dapps.json"
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        print(f"Run: python3 fetch_all_dapps.py {category_name}")
        return
    
    # Load dapps
    with open(input_file, 'r') as f:
        all_dapps = json.load(f)
    
    print(f"Category: {category_name}")
    print(f"Loaded {len(all_dapps)} dapps from {input_file}")
    print(f"Using 5 parallel threads")
    print("="*60)
    
    # Prepare tasks
    dapp_chain_tasks = []
    task_counter = 0
    for dapp in all_dapps:
        active_chains = dapp.get('activeChainIds', [])
        if not active_chains:
            continue
        
        for chain_id in active_chains:
            task_counter += 1
            dapp_chain_tasks.append((task_counter, dapp, chain_id, 0, chain_mapping, category_name))
    
    total_tasks = len(dapp_chain_tasks)
    dapp_chain_tasks = [(i, dapp, chain_id, total_tasks, chain_mapping, category_name) for i, dapp, chain_id, _, _, _ in dapp_chain_tasks]
    
    print(f"Total dapp-chain combinations: {total_tasks}")
    print("="*60)
    
    # Run with threads
    all_rows = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_dapp_chain, task) for task in dapp_chain_tasks]
        
        for future in as_completed(futures):
            rows = future.result()
            if rows:
                all_rows.extend(rows)
    
    # Save to category folder
    output_file = f"data_{category_name}/dapp_history_by_chain.csv"
    if all_rows:
        fieldnames = ['Date', 'Dapp Name', 'Chain Name', 'UAW', 'Transactions', 'Volume', 'Category']
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
    
    print("="*60)
    print(f"✅ Done! Category: {category_name}")
    print(f"   Success: {success_count} dapp-chain combinations")
    print(f"   Errors: {error_count} dapp-chain combinations")
    print(f"   Total rows: {len(all_rows)}")
    print(f"   Saved to: {output_file}")

if __name__ == "__main__":
    # Get category from command line (default: games)
    category = sys.argv[1] if len(sys.argv) > 1 else 'games'
    
    fetch_history(category)
