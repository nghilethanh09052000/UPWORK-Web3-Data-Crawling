import requests
import json
import base64
import time

# Credentials
JWT_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE"
API_KEY = "hfn5rhXT5j3d2fUQXrs6Mq868HNOOipLoQsa3N6H2582gkFGeWI"
COOKIES = 'non_registered_ga_id=5220f9b9-a673-4c44-bb3f-8964f03f0ce6; _ga=GA1.1.1504934669.1763281373; cebs=1; _ce.clock_data=-27%2C27.64.23.48%2C1%2Ca2cb084c96a1ead15308a8f1e203c3be%2CEdge%2CVN; jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyOTA3MTIsImV4cCI6MTc2NTk2OTExMiwicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiJlY2NlOTUyYWU2In0.df0LFpe7ximbOYT7XfQhZ6DVaRkSYwT8VfmhIVy6U9MnYsDpooaF-OozWe5vl7mN-hNiTpugU2HRIVdTSvw-QJG_tBBW75w9n8at9iMhLiN2cuEUM_uKgyH8RF6cLaib5cBKxijTmG4DwzTurr-0R0SV7he1R8l_njnQ3tBnTQFVfZ2UqquV9NPMpAxg87fIWykOjm9zjTx7jnmowIWEoBVs9iQW3urhTE1uM1t2x6u0aabsu4p182nM5lU9jCa2sjzRH2h0SPwZ1m6OQTNPAPKbOb_5H5JXr8H8c5sf8sHmPOaJrQlnkCJCY4KPw9U8nrg7tz_ins76Y50BT6-PAnMWFJPxgx-OcMp3tNZ1OkiVJ_QeDKk1YaYGUWzRdQK0suMPWmPaJQmrlXp1vCf1PxKRWd-DXgICCNZWmGd33x-2v_PlBxRj53rNrP2nKDr7z6LdQZvi2PaSOVChPUkoEyco46MmqvW5AKCfDDfJjN3LausfQd6jZNgCFPB-_jKKMVimts4rk7nGumnT-8JB9TtQ8cmaWuMNNfBYQlIGeZgmPIKXv6fin3mHpvxXcU8c_gl11eRS1UVQCpPQB7buLdQ4muEFZCwgYyvAwunx2755UplZmCPnM6UhkZipW5FiosJ23eOYd4AUEUliysJwPFtAh_OacHsxSwmd2rCfBSE; dapp_ga_id=dbdcde9c-6238-4ae7-a54e-fea32c682e3c; __cf_bm=ACN3LT.BoSyvycB_iIKd97N7o0awY8d_8hjaPuWJ71w-1763300481-1.0.1.1-1ZcqoR_F.Lmfhm5d4dKBxwJVx1m3eHiNO.NwHMMFZ1RiCofOmLsIKJ.mTSOtW3RQF07LAab24czo7ZUMcokH_DZBXEcYx.satI8pwwfb6sw; cf_clearance=d2_2Zkoc.TVK4i09bEID5jumzV9BvqsB1cMRovqroew-1763300608-1.2.1.1-aShC7EGVx9OfvjmCJnHuXMKWqZqMeeKxIZgOqbWRcnxI_W6rgd4O1B2dCMlVPuKqxL_JSyCvxAwyc0ZLO07JbOVJtjXNWuqzX3U6K.B4pXS3zdMfbIB_T9LmaI3YA_AKn33Ohpw.FDyFurseYsz11vwwCKOdjIVZuv8X5SUEjnLilhdPd7H1DArK.gJDTWnTuDOMm5Mx31OK3P3J7oXzf5naPCglg2X.jzYNSxUZd49G9g2OEvFggeMDJfOzUODh; cebsp_=79; _ce.s=v~66d5ea95637e1f1663b6f30f127b5a3c240389ab~lcw~1763300677096~vir~returning~lva~1763300617982~vpv~0~v11ls~a6a82dd0-c2ee-11f0-854a-4de087e4f113~vdva~1763337599999~v11.cs~448387~v11.s~a6a82dd0-c2ee-11f0-854a-4de087e4f113~v11.vs~66d5ea95637e1f1663b6f30f127b5a3c240389ab~v11.fsvd~eyJ1cmwiOiJkYXBwcmFkYXIuY29tLyoiLCJyZWYiOiJodHRwczovL2RhcHByYWRhci5jb20vZGFwcC93b3JsZC1vZi1keXBpYW5zOz9fX2NmX2NobF90az1UWlBEMlNySjNyeFhGQlRHRlBPZU9leUxaVDJwcURnREtadGdHQzRtb2swLTE3NjMyOTkwNjMtMS4wLjEuMS1TNjB3eDc2SHppU29Oc1pieG13eHF4MnpUMlVGSGVka2d6dk5WZkp2bU84IiwidXRtIjpbXX0%3D~v11.sla~1763299070255~v11.ws~1~v11.wr~3~v11.ss~1763299070257~v11nv~0~gtrk.la~mi1rp45k~v11e~1~lcw~1763300679752; _ga_BTQFKMW6P9=GS2.1.s1763299069$o2$g1$t1763300679$j13$l0$h0; dr_session={"sessionId":"a38bfe6d-a967-41ca-ba39-402a23958c3a","sessionNumber":2,"landingPage":"/404","campaignSource":"{\"source\":null,\"medium\":null,\"campaign\":null}","lastActivity":1763300680297}'

url = "https://dapps-rankings.dappradar.com/api/v1.0/rankings/chains"

headers = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Accept": "application/json",
    "x-api-sk": API_KEY,
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
    "Cookie": COOKIES
}

def encode_params(page_num):
    """Encode parameters for page"""
    params_dict = {
        'DappRadarcurrency': 'USD',
        'sort': 'dappCount',
        'order': 'desc',
        'range': 'all',
        'resultsPerPage': 50,
        'page': page_num
    }
    
    params_str = '&'.join([f"{k}={v}" for k, v in params_dict.items()])
    encoded_once = base64.b64encode(params_str.encode()).decode()
    encoded_twice = base64.b64encode(encoded_once.encode()).decode()
    return encoded_twice

all_chains = []

print("Fetching chains from pages 1-5...")
print("="*60)

for page in range(1, 6):
    encoded_params = encode_params(page)
    
    params = {
        "params": encoded_params
    }
    
    try:
        response = requests.get(url, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            chains = data.get('results', [])
            
            print(f"Page {page}: Found {len(chains)} chains")
            
            for chain in chains:
                token = chain.get('token', {})
                
                chain_data = {
                    'page': page,
                    'chainId': chain.get('chainId'),
                    'dappCount': chain.get('dappCount'),
                    'smartContractCount': chain.get('smartContractCount'),
                    'uawCount': chain.get('uawCount'),
                    'uawCountChange': chain.get('uawCountChange'),
                    'transactionCount': chain.get('transactionCount'),
                    'transactionCountChange': chain.get('transactionCountChange'),
                    'totalVolumeInFiat': chain.get('totalVolumeInFiat'),
                    'totalVolumeChange': chain.get('totalVolumeChange'),
                    'tvlInFiat': chain.get('tvlInFiat'),
                    'tvlChange': chain.get('tvlChange'),
                    'totalNftVolumeInFiat': chain.get('totalNftVolumeInFiat'),
                    'totalNftVolumeChange': chain.get('totalNftVolumeChange'),
                    # Token info
                    'tokenSymbol': token.get('symbol'),
                    'tokenSlug': token.get('slug'),
                    'tokenLogo': token.get('logo'),
                    'tokenPrice': token.get('priceInFiat'),
                    'tokenPriceChange': token.get('priceChange')
                }
                
                all_chains.append(chain_data)
        else:
            print(f"Page {page}: Error {response.status_code}")
            break
        
        # Sleep to avoid rate limiting
        time.sleep(0.5)
        
    except Exception as e:
        print(f"Page {page}: Exception - {e}")
        break

# Save to JSON
output_file = "all_chains.json"
with open(output_file, 'w') as f:
    json.dump(all_chains, f, indent=2)

print("="*60)
print(f"✅ Done! Saved {len(all_chains)} chains to {output_file}")
print("\nFirst 10 chains:")
for chain in all_chains[:10]:
    print(f"  Chain ID {chain.get('chainId')}: {chain.get('tokenSymbol')} ({chain.get('tokenSlug')}) - {chain.get('dappCount')} dapps")

