import requests
import base64
import json
import time
import os
import sys

# Category mapping
CATEGORIES = {
    'games': 1,
    'defi': 2,
    'collectibles': 3,
    'gambling': 4,
    'other': 5,
    'high-risk': 6,
    'marketplaces': 7,
    'exchanges': 8,
    'social': 9
}

# Credentials
JWT_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE"
API_KEY = "HKk3NW7pkfPZpne0ZIR539NpOYSSMGMn5hMm70nqH3e44gMKKe9b"
COOKIES = 'non_registered_ga_id=5220f9b9-a673-4c44-bb3f-8964f03f0ce6; _ga=GA1.1.1504934669.1763281373; cebs=1; jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE; dapp_ga_id=dbdcde9c-6238-4ae7-a54e-fea32c682e3c; cf_clearance=64bupobfMNqcvi5_7wRlSZlkl80_y08a.KYJWpMtQzc-1763367383-1.2.1.1-ukeFJRvghJ4KekF0A5cwgsD_eHBMEBgvIQmfmSmpxA0v3kiXK4QaaYv59gb3rj27hyw3_ddkCw.a7Ixe_XTqX9c.sT4Vn26IZwwVNbVK1k8qwtqVs8DexclKK2dGueKFMlSfFe38Qc8.LeGTz8MzQSFxtPILTAwAjsk6QFFQezugnOpp2yrmYdGPizi9BEd1CQspw1feyDxdEfQuOY0Q9ZJ86JWeuWDcaFynK6YlqlBKF_mc0YS9aSI2KKhY9xV7; __cf_bm=J3U16iLNwLze3IzA4pl6.S_osmavg4_8dRqxEx1VPLc-1763369868-1.0.1.1-jGfrU91cv75wt9lhATB.E4WRSS08rHwQIQrm0vGjHhhvnNhmjpGoAMZjssl_i78AvRQ7EQo9.TiE81ISBYtVfRZv626tGUEn7jI7yfsxHJk; cebsp_=121; _ce.s=v~66d5ea95637e1f1663b6f30f127b5a3c240389ab~lcw~1763370182655~vir~returning~lva~1763367385044~vpv~0~v11ls~e9d976f0-c392-11f0-a7d8-9578943f5fd5~vdva~1763423999999~gtrk.la~mi2x2t5j~v11.cs~448387~v11.s~e9d976f0-c392-11f0-a7d8-9578943f5fd5~v11.vs~66d5ea95637e1f1663b6f30f127b5a3c240389ab~v11.fsvd~eyJ1cmwiOiJkYXBwcmFkYXIuY29tL3JhbmtpbmdzIiwicmVmIjoiIiwidXRtIjpbXX0%3D~v11.sla~1763369620448~v11.ws~1~v11.wr~4~v11.ss~1763369620450~v11e~1~v11nv~0~lcw~1763370182935; _ga_BTQFKMW6P9=GS2.1.s1763365755$o3$g1$t1763370183$j19$l0$h0; dr_session={"sessionId":"b0b2adc6-1898-4452-b057-675d665b7c23","sessionNumber":3,"landingPage":"/rankings/category/games","campaignSource":"{\"source\":null,\"medium\":null,\"campaign\":null}","lastActivity":1763370183244}'

url = "https://dapps-rankings.dappradar.com/api/v1.0/rankings/dapps"

headers = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Accept": "application/json",
    "x-api-sk": API_KEY,
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
    "Cookie": COOKIES
}

def encode_params(page_num, category_id):
    """Encode parameters for page and category"""
    params_dict = {
        'DappRadarcurrency': 'USD',
        'sort': 'uawCount',
        'order': 'desc',
        'range': '24h',
        'resultsPerPage': 50,
        'page': page_num,
        'categoryId[]': category_id,
        'excludedDappId': 40013
    }
    
    params_str = '&'.join([f"{k}={v}" for k, v in params_dict.items()])
    encoded_once = base64.b64encode(params_str.encode()).decode()
    encoded_twice = base64.b64encode(encoded_once.encode()).decode()
    return encoded_twice

def fetch_category(category_name):
    """Fetch all dapps for a category"""
    
    # Get category ID
    if category_name not in CATEGORIES:
        print(f"Error: Unknown category '{category_name}'")
        print(f"Available categories: {', '.join(CATEGORIES.keys())}")
        return
    
    category_id = CATEGORIES[category_name]
    
    # Create category folder
    category_folder = f"data_{category_name}"
    if not os.path.exists(category_folder):
        os.makedirs(category_folder)
    
    output_file = f"{category_folder}/all_dapps.json"
    
    # Load existing data if available
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            all_dapps = json.load(f)
        print(f"Found existing file with {len(all_dapps)} dapps")
    else:
        all_dapps = []
        print("Starting fresh")
    
    print(f"Fetching category: {category_name} (ID: {category_id})")
    print("="*60)
    
    page = 1
    total_new = 0
    
    while True:
        encoded_params = encode_params(page, category_id)
        
        params = {
            "params": encoded_params
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                dapps = data.get('results', [])
                
                # Stop if no results
                if not dapps:
                    print(f"Page {page}: No more results. Stopping.")
                    break
                
                print(f"Page {page}: Found {len(dapps)} dapps")
                
                page_dapps = []
                for dapp in dapps:
                    stats = dapp.get('statistic', {})
                    
                    dapp_data = {
                        'page': page,
                        'category': category_name,
                        'id': dapp.get('id'),
                        'name': dapp.get('name'),
                        'slug': dapp.get('slug'),
                        'logo': dapp.get('logo'),
                        'deeplink': dapp.get('deeplink'),
                        'categoryId': dapp.get('categoryId'),
                        'chainIds': dapp.get('chainIds'),
                        'activeChainIds': dapp.get('activeChainIds'),
                        'totalBalanceInFiat': stats.get('totalBalanceInFiat'),
                        'totalVolumeInFiat': stats.get('totalVolumeInFiat'),
                        'transactionCount': stats.get('transactionCount'),
                        'uawCount': stats.get('uawCount'),
                    }
                    
                    page_dapps.append(dapp_data)
                
                # Append and save
                all_dapps.extend(page_dapps)
                total_new += len(page_dapps)
                
                with open(output_file, 'w') as f:
                    json.dump(all_dapps, f, indent=2)
                
                print(f"  → Saved to {output_file} (Total: {len(all_dapps)} dapps)")
                
                page += 1
                time.sleep(0.5)
                
            else:
                print(f"Page {page}: Error {response.status_code}")
                break
            
        except Exception as e:
            print(f"Page {page}: Exception - {e}")
            break
    
    print("="*60)
    print(f"✅ Done! Category: {category_name}")
    print(f"   Total: {len(all_dapps)} dapps in {output_file}")
    print(f"   New: {total_new} dapps added this run")

if __name__ == "__main__":
    # Get category from command line argument (default: games)
    category = sys.argv[1] if len(sys.argv) > 1 else 'games'
    
    fetch_category(category)
