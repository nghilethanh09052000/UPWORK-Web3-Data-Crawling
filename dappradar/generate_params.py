import base64
import requests
# How to generate DappRadar API params

# Step 1: Define your parameters
params = {
    'DappRadarcurrency': 'USD',
    'sort': 'uawCount',  # uawCount, volume, balance, transactions
    'order': 'desc',      # desc or asc
    'range': '24h',       # 24h, 7d, 30d
    'resultsPerPage': 50,
    'page': 1,
    'categoryId[]': 1,
    'excludedDappId': 40013
}

# Step 2: Convert to string
params_str = '&'.join([f"{k}={v}" for k, v in params.items()])
print("Step 1 - Plain params:")
print(params_str)
print()

# Step 3: First base64 encode
encoded_once = base64.b64encode(params_str.encode()).decode()
print("Step 2 - Encoded once:")
print(encoded_once)
print()

# Step 4: Second base64 encode (DappRadar uses double encoding)
encoded_twice = base64.b64encode(encoded_once.encode()).decode()
print("Step 3 - Encoded twice (final):")
print(encoded_twice)
print()

# Step 5: Build final URL
url = f"https://dapps-rankings.dappradar.com/api/v1.0/rankings/dapps"

headers = {
    "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyODY3ODMsImV4cCI6MTc2NTk2NTE4Mywicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiIzMmI5MzhjYzJlIn0.ZysMPvKH7k2mIXLD70rc0YVRolOkJHClYlg1C7SZHNADTj0fxgSX-3r9SeL15jpctGyGNO6hw-1UvGB44y1V2bqZXhMksQTnyZdghlAWX_irKNIEOMzCDGX8OUMVcTRxGu3Jt5ddwU4crfyai5IVamEVvjNLjq7lXRXenYVI4dqMQ29AKOxzVr1ixGW4ZJKJTO9asF7Q9eyefso4zyTkVEXS1rgLwM4FpZUxZLZ2XfPz83neY2RumDLAYVCuwpwW9q0QaD-tfT-HMVB0eUGSjbD1Z8f50iAtFUQhZyAMGE9O1yYE4JJ_cIWEza9tNbkeX0GVZY45_Zf8Lj2YIV8EZgH21ya1i35DT0eidbUVnO42gXeQLtKnd8kXLUIQ6fMvSd9JxQ6sxAlTxs77qzXjcR4y3i2iqgqtCz3fPERjgwSvzRbup8BWCY4198n9l5i_fwFYsB6oQ15wdoUFnuBIwxb6INY7sS3D3cwbQTy6eT3I0u37lI-3GeQJO4JGyoi_QFqfZ-hHWA0x1uLrjFm3oNc-ZQVcwWO_K5vSahDMKnFNH5hPwm9b8RpSo8qFl6ehnCHRO2ex6H4X5hpZnP7KKL_CdXBnSqaTrzq_P1DTqg8xXEGJF-XVVj5tfnkBHSdAd69ZcllEs8ZJZzUYSJ-uj3rTAqUcopHf2W5OFryfm_I",
    "Accept": "application/json",
    "x-api-sk": "Yiqb4I9YRnNIgUG1hnMN7jomkoUSliYob1SRLi2qRTqZsNOjncf",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0",
    "Cookie": 'non_registered_ga_id=5220f9b9-a673-4c44-bb3f-8964f03f0ce6; _ga=GA1.1.1504934669.1763281373; cebs=1; _ce.clock_data=-27%2C27.64.23.48%2C1%2Ca2cb084c96a1ead15308a8f1e203c3be%2CEdge%2CVN; dapp_ga_id=dbdcde9c-6238-4ae7-a54e-fea32c682e3c; jwt=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3NjMyODY3ODMsImV4cCI6MTc2NTk2NTE4Mywicm9sZXMiOlsiUk9MRV9VU0VSIl0sImVtYWlsIjpudWxsLCJpZCI6ImYyODIxNzEyLWJmNTAtNDJjZC1hZDM4LTc1MWQyNmZmM2I1MCIsInBybyI6dHJ1ZSwiYWRkcmVzcyI6IjB4OWE1NzIxRjE5Njc4NTgyMDBhRjgyRGY4NTJBMmRERkFDY0E5QzJkMiIsInNhbHQiOiIzMmI5MzhjYzJlIn0.ZysMPvKH7k2mIXLD70rc0YVRolOkJHClYlg1C7SZHNADTj0fxgSX-3r9SeL15jpctGyGNO6hw-1UvGB44y1V2bqZXhMksQTnyZdghlAWX_irKNIEOMzCDGX8OUMVcTRxGu3Jt5ddwU4crfyai5IVamEVvjNLjq7lXRXenYVI4dqMQ29AKOxzVr1ixGW4ZJKJTO9asF7Q9eyefso4zyTkVEXS1rgLwM4FpZUxZLZ2XfPz83neY2RumDLAYVCuwpwW9q0QaD-tfT-HMVB0eUGSjbD1Z8f50iAtFUQhZyAMGE9O1yYE4JJ_cIWEza9tNbkeX0GVZY45_Zf8Lj2YIV8EZgH21ya1i35DT0eidbUVnO42gXeQLtKnd8kXLUIQ6fMvSd9JxQ6sxAlTxs77qzXjcR4y3i2iqgqtCz3fPERjgwSvzRbup8BWCY4198n9l5i_fwFYsB6oQ15wdoUFnuBIwxb6INY7sS3D3cwbQTy6eT3I0u37lI-3GeQJO4JGyoi_QFqfZ-hHWA0x1uLrjFm3oNc-ZQVcwWO_K5vSahDMKnFNH5hPwm9b8RpSo8qFl6ehnCHRO2ex6H4X5hpZnP7KKL_CdXBnSqaTrzq_P1DTqg8xXEGJF-XVVj5tfnkBHSdAd69ZcllEs8ZJZzUYSJ-uj3rTAqUcopHf2W5OFryfm_I; cf_clearance=RMbfNLGl7sdtSZXmVAF.amVmE90fsinwRo5NZ5Xt5tE-1763287221-1.2.1.1-JBdCThZszXWhfrgZ8enynGkC93XQ3m0sGG_eCyA.IpOcWj4D6SRh00UaWBjne8tKFqW5WJaeNyFmgyYFXC4pptfb7UAE3KDKdlHLTJ1PiuEwKW38FHcTDR_QAaU_1V_FnC1xPEumuigvTbx0sjgdkOpm.aV7UUDkB7PCwIchpYAKD8eRJudZ68y84yzm3vLsH.rq_IzkkqqDLp_P3s0bX2h4zF4CbXjiUkvmGLqUS.GRAAkSXjNrr7xT42VmgoqZ; cebsp_=32; __cf_bm=fFGgsUoD51akTGGf1ysDYqqXA.a1DWC0mywLqgb5Hkg-1763287841-1.0.1.1-mz6zQjF5ZnAqTyBdnUfjjVoRfTa0E8ibg8l9ggIvLUgGQXMLWgGfcxarn5_q8O9MSdugQMPpAozhBSmfI28RdAqMd567x842WPfarNb5fjQ; _ce.s=v~66d5ea95637e1f1663b6f30f127b5a3c240389ab~lcw~1763287999667~vir~returning~lva~1763287637699~vpv~0~v11ls~d2affb60-c2d0-11f0-a3b3-8d3a594e02a9~gtrk.la~mi1k5eai~v11.cs~448387~v11.s~d2affb60-c2d0-11f0-a3b3-8d3a594e02a9~v11.vs~66d5ea95637e1f1663b6f30f127b5a3c240389ab~v11.fsvd~eyJ1cmwiOiJkYXBwcmFkYXIuY29tL2FjY291bnQvcHJvLW1lbWJlcnNoaXAiLCJyZWYiOiJodHRwczovL2RhcHByYWRhci5jb20vYWNjb3VudC9wcm8tbWVtYmVyc2hpcCIsInV0bSI6W119~v11.sla~1763286259224~v11.ws~1~v11.wr~2~v11.ss~1763286259227~vdva~1763337599999~v11nv~0~v11e~1~lcw~1763288002459; _ga_BTQFKMW6P9=GS2.1.s1763281372$o1$g1$t1763288002$j54$l0$h0; dr_session={"sessionId":"f7e26c40-7596-4136-bc55-8cdf845da09c","sessionNumber":1,"landingPage":"/","campaignSource":"{\"source\":null,\"medium\":null,\"campaign\":null}","lastActivity":1763288002940}'
}

response = requests.get(url, headers=headers, params={"params": encoded_twice})

print("Status:", response.status_code)
print(response.json())