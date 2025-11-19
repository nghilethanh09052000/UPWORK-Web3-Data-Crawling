import requests
import json
import csv
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Credentials
JWT_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE"
API_KEY = "LSH0F0qY0mg0dXhW8XeSms3ZaYjXhXKa8YMT1GRobk4Rob600KiX"
COOKIES = 'non_registered_ga_id=5220f9b9-a673-4c44-bb3f-8964f03f0ce6; _ga=GA1.1.1504934669.1763281373; cebs=1; jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE; dapp_ga_id=dbdcde9c-6238-4ae7-a54e-fea32c682e3c; _ce.clock_data=22%2C27.64.23.48%2C1%2Ca2cb084c96a1ead15308a8f1e203c3be%2CEdge%2CVN; __cf_bm=DlL0CVgR_MYucoZFsOBWUa9XVNuTZq7vKxVJmfYvnGA-1763391138-1.0.1.1-v77Fwhtyqi4x0Y7lsxXQ_6pR5c7n2OV.1RiD9pZL_n.qcTRjD1e3333n7xLyj3JAcDyXYhdkIE5B6zrbgvZooU7yAiuESttIFxVCegmOxTM; cf_clearance=uugBOrcAouuRRLK3Pk8_B3QlNZYuxjVAR4tQqoVBQzY-1763391145-1.2.1.1-GZ3MZtB0w.HQQya0t7YbMW6v6CN3xgXVX9yQd38hf4fSstzJVTdZrjovhNzMYNqNWV8JADWNiISfaYjmW7QWo9YXUsskOgggCgBq5jWhxD.mAis1Wf5q68bGBjqXMvKTO_q0SQIB9bapmPm0eWCBYIKXv1pk_MTkR4pLOZg1V_Bz8kFBfhWM57nfFxxPTffhqMeMlilAH63sIRtai41sLThSlD_4h5p7LKBSS5Zq.FYRhCqxV5qiJ4kVetrCyydk; cebsp_=138; _ga_BTQFKMW6P9=GS2.1.s1763391137$o5$g1$t1763391258$j39$l0$h0; _ce.s=v~66d5ea95637e1f1663b6f30f127b5a3c240389ab~lcw~1763391256296~vir~returning~lva~1763374610636~vpv~0~v11ls~087a45d0-c3c5-11f0-9b58-b1d12f738f04~vdva~1763423999999~gtrk.la~mi39moo3~v11.cs~448387~v11.s~087a45d0-c3c5-11f0-9b58-b1d12f738f04~v11.vs~66d5ea95637e1f1663b6f30f127b5a3c240389ab~v11.fsvd~eyJ1cmwiOiJkYXBwcmFkYXIuY29tL3JhbmtpbmdzL2NhdGVnb3J5L2V4Y2hhbmdlcy8qIiwicmVmIjoiaHR0cHM6Ly9kYXBwcmFkYXIuY29tL3JhbmtpbmdzL2NhdGVnb3J5L2V4Y2hhbmdlcy8yOD9yZXN1bHRzUGVyUGFnZT01MCZfX2NmX2NobF90az1EaEc1SzVsSnRiNXNsTWVKdE5WWWZacFVnMlVzUEtfZEdIemJva195czJRLTE3NjMzOTExMzgtMS4wLjEuMS1XcjdOc3Y5OEFTSW5va0xyZDZ3UllaTXhCRHBlQjNjczdMUkJXMjhjX3lrIiwidXRtIjpbXX0%3D~v11.sla~1763391146670~v11.wss~1763391146671~v11.ss~1763391146673~lcw~1763391265635; dr_session={"sessionId":"b0b2adc6-1898-4452-b057-675d665b7c23","sessionNumber":3,"landingPage":"/rankings/category/games","campaignSource":"{\"source\":null,\"medium\":null,\"campaign\":null}","lastActivity":1763391265754}'

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

def process_dapp(dapp_data):
    """Fetch details for a single dapp"""
    global success_count, error_count, completed_count
    
    i, dapp, total, category_name = dapp_data
    slug = dapp.get('slug')
    name = dapp.get('name')
    
    if not slug:
        with lock:
            error_count += 1
            completed_count += 1
            print(f"{completed_count}/{total}: Skipping {name} (no slug)")
        return None
    
    url = f"https://dapps-rankings.dappradar.com/api/v1.0/project/{slug}"
    params = {"currency": "USD"}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            # Format images (array to string)
            images = data.get('images', [])
            images_str = ' | '.join([f"{img.get('thumbnail', '')} - {img.get('original', '')}" for img in images])
            
            # Format socialLinks (array to string)
            social_links = data.get('socialLinks', [])
            social_str = ' | '.join([f"{link.get('title', '')} - {link.get('url', '')} = {link.get('type', '')}" for link in social_links])
            
            # Format tags (array to string)
            tags = data.get('tags', [])
            tags_str = ', '.join([tag.get('name', '') for tag in tags])
            
            # Format chainIds (map to chain names)
            chain_ids = data.get('chainIds', [])
            chain_names = [chain_mapping.get(cid, f'chain_{cid}') for cid in chain_ids]
            chains_str = ', '.join(chain_names)
            
            # Format categories (array to string)
            categories = data.get('categories', [])
            categories_str = ', '.join(categories) if isinstance(categories, list) else str(categories)
            
            # Format distributionPlatforms (array to string)
            dist_platforms = data.get('distributionPlatforms', [])
            dist_str = ' | '.join([f"{platform.get('url', '')} - {platform.get('type', '')}" for platform in dist_platforms])
            
            result = {
                'id': data.get('id'),
                'name': data.get('name'),
                'description': data.get('description', ''),
                'video': data.get('video', ''),
                'updatedAt': data.get('updatedAt', ''),
                'createdAt': data.get('createdAt', ''),
                'images': images_str,
                'socialLinks': social_str,
                'shortDescription': data.get('shortDescription', ''),
                'website': data.get('website', ''),
                'tags': tags_str,
                'chainIds': chains_str,
                'isDapp': data.get('isDapp', ''),
                'categories': categories_str,
                'disabledAt': data.get('disabledAt', ''),
                'disableReason': data.get('disableReason', ''),
                'disabled': data.get('disabled', ''),
                'boostable': data.get('boostable', ''),
                'autoIntegrated': data.get('autoIntegrated', ''),
                'distributionPlatforms': dist_str,
                'logo': data.get('logo', ''),
                'upcoming': data.get('upcoming', ''),
                'launchDescription': data.get('launchDescription', ''),
                'preLaunchLink': data.get('preLaunchLink', ''),
                'launchDate': data.get('launchDate', ''),
                'new': data.get('new', ''),
                'listedAt': data.get('listedAt', ''),
                'inactiveSince': data.get('inactiveSince', ''),
                'deeplink': data.get('deeplink', ''),
                'subChain': data.get('subChain', ''),
                'otherChainLabel': data.get('otherChainLabel', ''),
                'mainAggregatedTokenId': data.get('mainAggregatedTokenId', ''),
                'premium': data.get('premium', ''),
                'category': category_name
            }
            
            with lock:
                success_count += 1
                completed_count += 1
                print(f"{completed_count}/{total}: ✅ {name}")
            
            return result
        else:
            with lock:
                error_count += 1
                completed_count += 1
                print(f"{completed_count}/{total}: ❌ {name} - Status {response.status_code}")
            return None
        
    except Exception as e:
        with lock:
            error_count += 1
            completed_count += 1
            print(f"{completed_count}/{total}: ❌ {name} - {e}")
        return None

def fetch_details(category_name):
    """Fetch details for all dapps in category"""
    
    input_file = f"data_{category_name}/all_dapps.json"
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        print(f"Run: python3 fetch_all_dapps.py {category_name}")
        return
    
    # Load dapps
    with open(input_file, 'r') as f:
        all_dapps = json.load(f)
    
    print(f"Category: {category_name}")
    print(f"Loaded {len(all_dapps)} dapps")
    print(f"Using 5 parallel threads")
    print("="*60)
    
    # Prepare tasks
    tasks = [(i+1, dapp, len(all_dapps), category_name) for i, dapp in enumerate(all_dapps)]
    
    # Run with threads
    all_details = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_dapp, task) for task in tasks]
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                all_details.append(result)
    
    # Save to CSV
    output_file = f"data_{category_name}/dapp_details.csv"
    if all_details:
        fieldnames = [
            'id', 'name', 'description', 'video', 'updatedAt', 'createdAt',
            'images', 'socialLinks', 'shortDescription', 'website', 'tags',
            'chainIds', 'isDapp', 'categories', 'disabledAt', 'disableReason',
            'disabled', 'boostable', 'autoIntegrated', 'distributionPlatforms',
            'logo', 'upcoming', 'launchDescription', 'preLaunchLink', 'launchDate',
            'new', 'listedAt', 'inactiveSince', 'deeplink', 'subChain',
            'otherChainLabel', 'mainAggregatedTokenId', 'premium', 'category'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_details)
    
    print("="*60)
    print(f"✅ Done! Category: {category_name}")
    print(f"   Success: {success_count} dapps")
    print(f"   Errors: {error_count} dapps")
    print(f"   Saved to: {output_file}")

if __name__ == "__main__":
    category = sys.argv[1] if len(sys.argv) > 1 else 'games'
    fetch_details(category)

