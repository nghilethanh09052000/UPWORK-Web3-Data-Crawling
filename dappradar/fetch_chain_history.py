import requests
import json
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict

# Credentials
JWT_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE"
API_KEY = "LSH0F0qY0mg0dXhW8XeSms3ZaYjXhXKa8YMT1GRobk4Rob600KiX"
COOKIES = 'non_registered_ga_id=5220f9b9-a673-4c44-bb3f-8964f03f0ce6; _ga=GA1.1.1504934669.1763281373; cebs=1; jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE; dapp_ga_id=dbdcde9c-6238-4ae7-a54e-fea32c682e3c; _ce.clock_data=22%2C27.64.23.48%2C1%2Ca2cb084c96a1ead15308a8f1e203c3be%2CEdge%2CVN; __cf_bm=DlL0CVgR_MYucoZFsOBWUa9XVNuTZq7vKxVJmfYvnGA-1763391138-1.0.1.1-v77Fwhtyqi4x0Y7lsxXQ_6pR5c7n2OV.1RiD9pZL_n.qcTRjD1e3333n7xLyj3JAcDyXYhdkIE5B6zrbgvZooU7yAiuESttIFxVCegmOxTM; cf_clearance=uugBOrcAouuRRLK3Pk8_B3QlNZYuxjVAR4tQqoVBQzY-1763391145-1.2.1.1-GZ3MZtB0w.HQQya0t7YbMW6v6CN3xgXVX9yQd38hf4fSstzJVTdZrjovhNzMYNqNWV8JADWNiISfaYjmW7QWo9YXUsskOgggCgBq5jWhxD.mAis1Wf5q68bGBjqXMvKTO_q0SQIB9bapmPm0eWCBYIKXv1pk_MTkR4pLOZg1V_Bz8kFBfhWM57nfFxxPTffhqMeMlilAH63sIRtai41sLThSlD_4h5p7LKBSS5Zq.FYRhCqxV5qiJ4kVetrCyydk; cebsp_=138; _ga_BTQFKMW6P9=GS2.1.s1763391137$o5$g1$t1763391258$j39$l0$h0; _ce.s=v~66d5ea95637e1f1663b6f30f127b5a3c240389ab~lcw~1763391256296~vir~returning~lva~1763374610636~vpv~0~v11ls~087a45d0-c3c5-11f0-9b58-b1d12f738f04~vdva~1763423999999~gtrk.la~mi39moo3~v11.cs~448387~v11.s~087a45d0-c3c5-11f0-9b58-b1d12f738f04~v11.vs~66d5ea95637e1f1663b6f30f127b5a3c240389ab~v11.fsvd~eyJ1cmwiOiJkYXBwcmFkYXIuY29tL3JhbmtpbmdzL2NhdGVnb3J5L2V4Y2hhbmdlcy8qIiwicmVmIjoiaHR0cHM6Ly9kYXBwcmFkYXIuY29tL3JhbmtpbmdzL2NhdGVnb3J5L2V4Y2hhbmdlcy8yOD9yZXN1bHRzUGVyUGFnZT01MCZfX2NmX2NobF90az1EaEc1SzVsSnRiNXNsTWVKdE5WWWZacFVnMlVzUEtfZEdIemJva195czJRLTE3NjMzOTExMzgtMS4wLjEuMS1XcjdOc3Y5OEFUSW5va0xyZDZ3UllaTXhCRHBlQjNjczdMUkJXMjhjX3lrIiwidXRtIjpbXX0%3D~v11.sla~1763391146670~v11.wss~1763391146671~v11.ss~1763391146673~lcw~1763391265635; dr_session={"sessionId":"b0b2adc6-1898-4452-b057-675d665b7c23","sessionNumber":3,"landingPage":"/rankings/category/games","campaignSource":"{\"source\":null,\"medium\":null,\"campaign\":null}","lastActivity":1763391265754}'

headers = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Accept": "application/json",
    "x-api-sk": API_KEY,
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
    "Cookie": COOKIES
}

# Category mapping
CATEGORY_NAMES = {
    2001: 'Layer 1',
    2002: 'Layer 2',
    2003: 'Gaming',
    2004: 'Non EVM',
    2005: 'EVM',
    2006: 'Roll Up',
    2007: 'Avalanche Chain',
    2008: 'ZK'
}

# Load corrected chain data from all_chains.json
print("Loading corrected chain data...")
with open('all_chains.json', 'r') as f:
    all_chains = json.load(f)

# Create chainId -> tokenSlug mapping
chain_names = {}
for chain in all_chains:
    chain_id = chain.get('chainId')
    token_slug = chain.get('tokenSlug')
    if chain_id and token_slug:
        chain_names[chain_id] = token_slug

print(f"Loaded {len(chain_names)} chain names")

# Thread-safe counters
lock = threading.Lock()
success_count = 0
error_count = 0
completed_count = 0

def process_chain(chain_data):
    """Fetch both history and defi data for a single chain"""
    global success_count, error_count, completed_count
    
    i, chain, total = chain_data
    chain_id = chain.get('chainId')
    chain_name = chain_names.get(chain_id, f'chain_{chain_id}')
    
    # Fetch chain details to get categories
    chain_category = 'Unknown'
    try:
        chain_detail_url = f"https://chains-service.dappradar.com/api/v1/chains/{chain_id}"
        detail_response = requests.get(chain_detail_url, headers=headers)
        if detail_response.status_code == 200:
            detail_data = detail_response.json()
            result = detail_data.get('result', {})
            category_ids = result.get('categories', [])
            
            # Map category IDs to names
            category_names = [CATEGORY_NAMES.get(cat_id, f'Cat_{cat_id}') for cat_id in category_ids]
            chain_category = ', '.join(category_names) if category_names else 'Unknown'
    except:
        pass
    
    if not chain_id:
        with lock:
            error_count += 1
            completed_count += 1
        return []
    
    # Fetch history data (UAW, transactions, volume)
    history_url = f"https://dapps-rankings.dappradar.com/api/v1.0/chain/{chain_id}/history/all"
    history_params = {"currency": "USD"}
    
    # Fetch defi data (TVL, adjusted TVL)
    defi_url = "https://defi-tracker.dappradar.com/v2/api/history/all"
    defi_params = {
        "chainId[]": chain_id,
        "currency": "USD"
    }
    
    history_data_dict = {}
    defi_data_dict = {}
    
    try:
        # Fetch history data
        response = requests.get(history_url, headers=headers, params=history_params)
        if response.status_code == 200:
            history_json = response.json()
            csv_data = history_json.get('csvData', {})
            data_list = csv_data.get('data', [])
            
            for row in data_list:
                day = row.get('day', '')
                if day:
                    history_data_dict[day] = {
                        'uaw': row.get('uaw', ''),
                        'transactions': row.get('transactions', ''),
                        'volume': row.get('volume', '')
                    }
        
        # Fetch defi data
        response = requests.get(defi_url, headers=headers, params=defi_params)
        if response.status_code == 200:
            defi_json = response.json()
            results = defi_json.get('results', {})
            csv_data = results.get('csvData', {})
            data_list = csv_data.get('data', [])
            
            for row in data_list:
                day = row.get('day', '')
                if day:
                    # Remove timezone if present (2020-09-12T00:00:00.000Z -> 2020-09-12)
                    day = day.split('T')[0] if 'T' in day else day
                    defi_data_dict[day] = {
                        'tvl': row.get('tvl', ''),
                        'atvl': row.get('atvl', '')  # adjusted TVL
                    }
        
        # Create separate rows for transaction and defi data
        rows = []
        
        # Add transaction data rows
        for date in sorted(history_data_dict.keys()):
            history = history_data_dict[date]
            csv_row = {
                'Chain Name': chain_name,
                'Chain Category': chain_category,
                'Date': date,
                'Type': 'transaction',
                'UAW': history.get('uaw', ''),
                'Transactions': history.get('transactions', ''),
                'Volume': history.get('volume', ''),
                'TVL': '',
                'Adjusted TVL': ''
            }
            rows.append(csv_row)
        
        # Add defi data rows
        for date in sorted(defi_data_dict.keys()):
            defi = defi_data_dict[date]
            csv_row = {
                'Chain Name': chain_name,
                'Chain Category': chain_category,
                'Date': date,
                'Type': 'defi',
                'UAW': '',
                'Transactions': '',
                'Volume': '',
                'TVL': defi.get('tvl', ''),
                'Adjusted TVL': defi.get('atvl', '')
            }
            rows.append(csv_row)
        
        with lock:
            if rows:
                success_count += 1
            else:
                error_count += 1
            completed_count += 1
            
            # Count transaction vs defi rows
            trans_count = len(history_data_dict)
            defi_count = len(defi_data_dict)
            print(f"{completed_count}/{total}: {'✅' if rows else '⚠️ '} {chain_name} ({chain_category}) - {trans_count} trans + {defi_count} defi = {len(rows)} rows")
        
        return rows
        
    except Exception as e:
        with lock:
            error_count += 1
            completed_count += 1
            print(f"{completed_count}/{total}: ❌ {chain_name} - {e}")
        return []

def fetch_chain_history():
    """Fetch history for all chains with categories"""
    
    print(f"Fetching history + defi data for {len(all_chains)} chains")
    print(f"Using 5 parallel threads")
    print("="*60)
    
    # Prepare tasks
    tasks = [(i+1, chain, len(all_chains)) for i, chain in enumerate(all_chains)]
    
    # Run with threads
    all_rows = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_chain, task) for task in tasks]
        
        for future in as_completed(futures):
            rows = future.result()
            if rows:
                all_rows.extend(rows)
    
    # Save to CSV
    output_file = "chain_history.csv"
    if all_rows:
        fieldnames = ['Chain Name', 'Chain Category', 'Date', 'Type', 'UAW', 'Transactions', 'Volume', 'TVL', 'Adjusted TVL']
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
    
    print("="*60)
    print(f"✅ Done!")
    print(f"   Success: {success_count} chains")
    print(f"   Errors: {error_count} chains")
    print(f"   Total rows: {len(all_rows):,}")
    print(f"   Saved to: {output_file}")

if __name__ == "__main__":
    fetch_chain_history()
