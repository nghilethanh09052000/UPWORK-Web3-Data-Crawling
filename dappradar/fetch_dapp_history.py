import requests
import json
import time
import csv
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Credentials
JWT_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE"
API_KEY = "9c1JlUsp27K1XN73MFZmn7NrHmHVIjmLbP02krbgkZS8jVgo22Q"
COOKIES = 'non_registered_ga_id=5220f9b9-a673-4c44-bb3f-8964f03f0ce6; _ga=GA1.1.1504934669.1763281373; cebs=1; _ce.clock_data=-27%2C27.64.23.48%2C1%2Ca2cb084c96a1ead15308a8f1e203c3be%2CEdge%2CVN; jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE; dapp_ga_id=dbdcde9c-6238-4ae7-a54e-fea32c682e3c; cf_clearance=szxvDZAA4.ojecXu71o8xmfPudi9eKCWrv26wiO1JzU-1763293952-1.2.1.1-nenmswrClzuN.zHC9V9rYd9WrAXF8hvqB3FF4Q7CzWJP9e0ZGHvMGUHiQg6UaUsZUU2V.goRtthAQgjMPZnyRMjM8S3OL7.OJGalYQDDan4nbDAzrKSrsrwEs9LE3zzMY6uajTeyzckkMhao9P.wQWu1a8xbCWhzznbfRmU5YW6MkmEIiGHs4m2CBxHW0yrbeeT0y3LNv1_sPkjPdcBGKWhPZmJ3rfY0j.LbVS1jq4o4SNc_HY3KuNVEHjN1u8qQ; __cf_bm=RfRWsXlu3_MMTG1MJnWPHFWQsd7EoTlsqHCWCFcfIL0-1763294219-1.0.1.1-GF3NZQa3h0vASLAWt1YmpIpjxMMlN9x6e85tHtjX.uY3K9TahxqjSMevDhEuws6d5XfV7hoh7JhqcrvGwJEpBxi0JQ_OENUKqxk_qn.tTpM; cebsp_=58; _ce.s=v~66d5ea95637e1f1663b6f30f127b5a3c240389ab~lcw~1763294233043~vir~returning~lva~1763287637699~vpv~0~v11ls~64d28860-c2df-11f0-9e41-a354b06ae8b5~gtrk.la~mi1nv3xn~vdva~1763337599999~v11.cs~448387~v11.s~64d28860-c2df-11f0-9e41-a354b06ae8b5~v11.vs~66d5ea95637e1f1663b6f30f127b5a3c240389ab~v11.fsvd~eyJ1cmwiOiJkYXBwcmFkYXIuY29tL2RhcHAvYXhpZS1pbmZpbml0eSIsInJlZiI6Imh0dHBzOi8vZGFwcHJhZGFyLmNvbS9yYW5raW5ncy9jYXRlZ29yeS9nYW1lcz9yZXN1bHRzUGVyUGFnZT01MCIsInV0bSI6W119~v11.sla~1763292517352~v11.wss~1763292517352~v11.ss~1763292517354~lcw~1763294240939; _ga_BTQFKMW6P9=GS2.1.s1763281372$o1$g1$t1763294241$j5$l0$h0; dr_session={"sessionId":"f7e26c40-7596-4136-bc55-8cdf845da09c","sessionNumber":1,"landingPage":"/","campaignSource":"{\"source\":null,\"medium\":null,\"campaign\":null}","lastActivity":1763294241228}'

headers = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Accept": "application/json",
    "x-api-sk": API_KEY,
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
    "Cookie": COOKIES
}

# Create csv_data directory
csv_dir = "csv_data"
if not os.path.exists(csv_dir):
    os.makedirs(csv_dir)
    print(f"Created {csv_dir} directory")

# Thread-safe counters
lock = threading.Lock()
success_count = 0
error_count = 0
completed_count = 0

def process_dapp(dapp_data):
    """Process a single dapp - fetch history and save to CSV"""
    global success_count, error_count, completed_count
    
    i, dapp, total = dapp_data
    slug = dapp.get('slug')
    name = dapp.get('name')
    
    if not slug:
        with lock:
            error_count += 1
            completed_count += 1
            print(f"{completed_count}/{total}: Skipping {name} (no slug)")
        return False
    
    # Build history URL
    url = f"https://dapps-rankings.dappradar.com/api/v1.0/dapp/{slug}/history/all"
    params = {"currency": "USD"}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            history_data = response.json()
            
            # Extract csvData from response
            csv_data = history_data.get('csvData', {})
            headers_list = csv_data.get('headers', [])
            data_list = csv_data.get('data', [])
            
            if headers_list and data_list:
                # Write to CSV file
                csv_file = os.path.join(csv_dir, f"{slug}.csv")
                
                with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=headers_list)
                    writer.writeheader()
                    
                    # Write data rows
                    for row in data_list:
                        # Map the data to match headers
                        csv_row = {
                            headers_list[0]: row.get('day', ''),
                            headers_list[1]: row.get('uaw', ''),
                            headers_list[2]: row.get('volume', ''),
                            headers_list[3]: row.get('transactions', '')
                        }
                        writer.writerow(csv_row)
                
                with lock:
                    success_count += 1
                    completed_count += 1
                    print(f"{completed_count}/{total}: ✅ {name} - Saved {len(data_list)} rows to {slug}.csv")
                return True
            else:
                with lock:
                    error_count += 1
                    completed_count += 1
                    print(f"{completed_count}/{total}: ⚠️  {name} - No CSV data available")
                return False
            
        else:
            with lock:
                error_count += 1
                completed_count += 1
                print(f"{completed_count}/{total}: ❌ {name} - Status {response.status_code}")
            return False
        
    except Exception as e:
        with lock:
            error_count += 1
            completed_count += 1
            print(f"{completed_count}/{total}: ❌ {name} - {e}")
        return False

# Read all dapps from JSON
with open('all_dapps.json', 'r') as f:
    all_dapps = json.load(f)

print(f"Loaded {len(all_dapps)} dapps from all_dapps.json")
print(f"Using 3 parallel threads")
print("="*60)

# Prepare data for threading
dapp_tasks = [(i+1, dapp, len(all_dapps)) for i, dapp in enumerate(all_dapps)]

# Run with 3 threads
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(process_dapp, task) for task in dapp_tasks]
    
    # Wait for all to complete
    for future in as_completed(futures):
        pass

print("="*60)
print(f"✅ Done!")
print(f"   Success: {success_count} dapps")
print(f"   Errors: {error_count} dapps")
print(f"   CSV files saved in: {csv_dir}/")

