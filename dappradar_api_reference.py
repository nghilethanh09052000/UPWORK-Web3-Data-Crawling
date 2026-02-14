import base64
import requests
import json

# Update these with your credentials
JWT_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyODY3ODMsImV4cCI6MTc2NTk2NTE4Mywicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiIzMmI5MzhjYzJlIn0.ZysMPvKH7k2mIXLD70rc0YVRolOkJHClYlg1C7SZHNADTj0fxgSX-3r9SeL15jpctGyGNO6hw-1UvGB44y1V2bqZXhMksQTnyZdghlAWX_irKNIEOMzCDGX8OUMVcTRxGu3Jt5ddwU4crfyai5IVamEVvjNLjq7lXRXenYVI4dqMQ29AKOxzVr1ixGW4ZJKJTO9asF7Q9eyefso4zyTkVEXS1rgLwM4FpZUxZLZ2XfPz83neY2RumDLAYVCuwpwW9q0QaD-tfT-HMVB0eUGSjbD1Z8f50iAtFUQhZyAMGE9O1yYE4JJ_cIWEza9tNbkeX0GVZY45_Zf8Lj2YIV8EZgH21ya1i35DT0eidbUVnO42gXeQLtKnd8kXLUIQ6fMvSd9JxQ6sxAlTxs77qzXjcR4y3i2iqgqtCz3fPERjgwSvzRbup8BWCY4198n9l5i_fwFYsB6oQ15wdoUFnuBIwxb6INY7sS3D3cwbQTy6eT3I0u37lI-3GeQJO4JGyoi_QFqfZ-hHWA0x1uLrjFm3oNc-ZQVcwWO_K5vSahDMKnFNH5hPwm9b8RpSo8qFl6ehnCHRO2ex6H4X5hpZnP7KKL_CdXBnSqaTrzq_P1DTqg8xXEGJF-XVVj5tfnkBHSdAd69ZcllEs8ZJZzUYSJ-uj3rTAqUcopHf2W5OFryfm_I"
API_KEY = "Yiqb4I9YRnNIgUG1hnMN7jomkoUSliYob1SRLi2qRTqZsNOjncf"

def encode_params(params_dict):
    """Encode parameters with double base64 encoding"""
    params_str = '&'.join([f"{k}={v}" for k, v in params_dict.items()])
    encoded_once = base64.b64encode(params_str.encode()).decode()
    encoded_twice = base64.b64encode(encoded_once.encode()).decode()
    return encoded_twice

def test_api(params_dict):
    """Test DappRadar API call"""
    url = "https://dapps-rankings.dappradar.com/api/v1.0/rankings/dapps"
    
    headers = {
        "Authorization": f"Bearer {JWT_TOKEN}",
        "Accept": "application/json",
        "x-api-sk": API_KEY,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0"
    }
    
    encoded_params = encode_params(params_dict)
    
    response = requests.get(url, headers=headers, params={"params": encoded_params})
    
    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Results: {len(data.get('results', []))} dapps")
        return data
    else:
        print(f"Error: {response.text}")
        return None

# Test with sample parameters
if __name__ == "__main__":
    params = {
        'DappRadarcurrency': 'USD',
        'sort': 'uawCount',
        'order': 'desc',
        'range': '24h',
        'resultsPerPage': 50,
        'page': 1,
        'categoryId[]': 1,
        'excludedDappId': 40013
    }
    
    result = test_api(params)
    if result:
        print("\nFirst dapp:", json.dumps(result.get('results', [{}])[0] if result.get('results') else {}, indent=2))
