import requests
import json
import csv
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Credentials
JWT_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE"
API_KEY = "Z8JM1H2V0S51OW6b4sG9YegUbmc4ahokQX141jpngTKTTo18VrFZK"
COOKIES = 'non_registered_ga_id=5220f9b9-a673-4c44-bb3f-8964f03f0ce6; _ga=GA1.1.1504934669.1763281373; cebs=1; jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE; dapp_ga_id=dbdcde9c-6238-4ae7-a54e-fea32c682e3c; _ce.clock_data=22%2C27.64.23.48%2C1%2Ca2cb084c96a1ead15308a8f1e203c3be%2CEdge%2CVN; __cf_bm=Y7fuxD9pO5ngd.15azxdD.yq9FBYQOvR0AaImFZgDCQ-1763445227-1.0.1.1-9SqAnSphj5X52g3hx1dwttbkki0ZhrLFP0Kq5EkY2T1JX2vD7Spo0A8FQFLOGAP3ViAKK8tCGkVGh83K3boodc_LT0NewvQrXtELdmHffPI; cf_clearance=2C9p3y2lCiQ28X5mY9dMPGGcTA3ORQm582dAx6oPaoc-1763445347-1.2.1.1-7cgYZZqmNfwiaBVy9ArAvNkN01IfmTjnkpdBCQ9RRyBOvbFDz6faXJstQxm1Ik1niChcAlVAM8XQsLX11VntFW8whweOrpCX7JMtE4tui.Pysz2etIK_7QGumZPN899jk9K82gQNt8RHzUDeYDus9aizdwKMFEuxBkfhHemsLvAZ_DGk4aYjXoO7JNy9W4.On3zmvylDPtZwtPDduK2Ext.tF_0T9DAjU7L0gLHHNpPrQUd6nhvVBcK28exxl.LR; _ce.s=v~66d5ea95637e1f1663b6f30f127b5a3c240389ab~lcw~1763445350718~vir~returning~lva~1763392971230~vpv~0~v11ls~3ac7c930-c443-11f0-84a7-7bc3b5f4c54a~vdva~1763423999999~gtrk.la~mi45u0ds~v11.cs~448387~v11.s~3ac7c930-c443-11f0-84a7-7bc3b5f4c54a~v11.vs~66d5ea95637e1f1663b6f30f127b5a3c240389ab~v11.fsvd~eyJ1cmwiOiJkYXBwcmFkYXIuY29tL2RhcHAvanVwaXRlci1leGNoYW5nZS9kZWZpIiwicmVmIjoiIiwidXRtIjpbXX0%3D~v11.sla~1763445347653~v11.wss~1763445347653~v11.ss~1763445347655~lcw~1763445355120; _ga_BTQFKMW6P9=GS2.1.s1763433938$o7$g1$t1763445355$j12$l0$h0; cebsp_=170; dr_session={"sessionId":"b0b2adc6-1898-4452-b057-675d665b7c23","sessionNumber":3,"landingPage":"/rankings/category/games","campaignSource":"{\"source\":null,\"medium\":null,\"campaign\":null}","lastActivity":1763445356047}'

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

def encode_params(page, category_id=2):
    """Generate double base64 encoded params for DeFi category"""
    params = f"DappRadarcurrency=USD&sort=uawCount&order=desc&range=24h&resultsPerPage=25&page={page}&categoryId[]={category_id}&excludedDappId=40013"
    encoded_once = base64.b64encode(params.encode()).decode()
    encoded_twice = base64.b64encode(encoded_once.encode()).decode()
    return encoded_twice

def fetch_all_defi_dapps():
    """Fetch all DeFi dapps"""
    print("Fetching all DeFi dapps...")
    print("="*60)
    
    all_dapps = []
    page = 1
    
    while True:
        encoded_params = encode_params(page, category_id=2)
        url = "https://dapps-rankings.dappradar.com/api/v1.0/rankings/dapps"
        
        try:
            response = requests.get(url, headers=headers, params={"params": encoded_params})
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    print(f"Page {page}: No more results, stopping")
                    break
                
                all_dapps.extend(results)
                print(f"Page {page}: Fetched {len(results)} dapps (Total: {len(all_dapps)})")
                page += 1
                
            else:
                print(f"Page {page}: Error {response.status_code}")
                break
                
        except Exception as e:
            print(f"Page {page}: Exception - {e}")
            break
    
    # Save to JSON
    with open('all_defi_dapps.json', 'w') as f:
        json.dump(all_dapps, f, indent=2)
    
    print("="*60)
    print(f"✅ Fetched {len(all_dapps)} DeFi dapps")
    print(f"Saved to: all_defi_dapps.json")
    return all_dapps

def process_defi_dapp_chain(dapp_chain_data):
    """Fetch TVL history for a single DeFi dapp on a specific chain"""
    global success_count, error_count, completed_count
    
    i, dapp, chain_id, total = dapp_chain_data
    dapp_id = dapp.get('id')  # Changed from 'dappId' to 'id'
    name = dapp.get('name', 'Unknown')
    chain_name = chain_mapping.get(chain_id, f'chain_{chain_id}')
    
    if not dapp_id:
        with lock:
            error_count += 1
            completed_count += 1
            print(f"{completed_count}/{total}: Skipping {name} on {chain_name} (no id)")
        return []
    
    url = "https://defi-tracker.dappradar.com/v2/api/history/all"
    params = {
        "chainId[]": chain_id,
        "dappId": dapp_id,
        "currency": "USD"
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', {})
            csv_data = results.get('csvData', {})
            data_list = csv_data.get('data', [])
            
            if not data_list:
                with lock:
                    error_count += 1
                    completed_count += 1
                    print(f"{completed_count}/{total}: ⚠️  {name} on {chain_name} - No data")
                return []
            
            rows = []
            for row in data_list:
                # Get date (remove timezone if present)
                day = row.get('day', '')
                if day and 'T' in day:
                    day = day.split('T')[0]
                
                csv_row = {
                    'Protocol Name': name,
                    'Chain Name': chain_name,
                    'Date': day,
                    'TVL': row.get('tvl', ''),
                    'TVL Adjusted': row.get('atvl', '')
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
                print(f"{completed_count}/{total}: ❌ {name} on {chain_name} - Status {response.status_code}")
            return []
            
    except Exception as e:
        with lock:
            error_count += 1
            completed_count += 1
            print(f"{completed_count}/{total}: ❌ {name} on {chain_name} - {e}")
        return []

def fetch_defi_tvl_history():
    """Fetch TVL history for all DeFi dapps"""
    
    # Load dapps
    with open('all_defi_dapps.json', 'r') as f:
        all_dapps = json.load(f)
    
    print(f"\nFetching TVL history for {len(all_dapps)} DeFi dapps")
    print(f"Using 5 parallel threads")
    print("="*60)
    
    # Prepare tasks (dapp-chain combinations)
    dapp_chain_tasks = []
    task_counter = 0
    for dapp in all_dapps:
        active_chains = dapp.get('activeChainIds', [])
        if not active_chains:
            continue
        
        for chain_id in active_chains:
            task_counter += 1
            dapp_chain_tasks.append((task_counter, dapp, chain_id, 0))
    
    total_tasks = len(dapp_chain_tasks)
    dapp_chain_tasks = [(i, dapp, chain_id, total_tasks) for i, dapp, chain_id, _ in dapp_chain_tasks]
    
    print(f"Total dapp-chain combinations: {total_tasks}")
    print("="*60)
    
    # Run with threads
    all_rows = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_defi_dapp_chain, task) for task in dapp_chain_tasks]
        
        for future in as_completed(futures):
            rows = future.result()
            if rows:
                all_rows.extend(rows)
    
    # Save to CSV
    output_file = "defi_tvl_history.csv"
    if all_rows:
        fieldnames = ['Protocol Name', 'Chain Name', 'Date', 'TVL', 'TVL Adjusted']
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_rows)
    
    print("="*60)
    print(f"✅ Done!")
    print(f"   Success: {success_count} dapp-chain combinations")
    print(f"   Errors: {error_count} dapp-chain combinations")
    print(f"   Total rows: {len(all_rows):,}")
    print(f"   Saved to: {output_file}")

if __name__ == "__main__":
    # Step 1: Fetch all DeFi dapps (comment out if already have all_defi_dapps.json)
    # all_dapps = fetch_all_defi_dapps()
    
    # Step 2: Fetch TVL history for each dapp-chain combination
    fetch_defi_tvl_history()

