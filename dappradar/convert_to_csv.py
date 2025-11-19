import json
import csv

# Read JSON file
with open('all_dapps.json', 'r') as f:
    dapps = json.load(f)

# Prepare CSV data
csv_rows = []

for dapp in dapps:
    active_chain_ids = dapp.get('activeChainIds', [])
    
    # If no activeChainIds, create one row with empty chain
    if not active_chain_ids:
        csv_rows.append({
            'page': dapp.get('page'),
            'id': dapp.get('id'),
            'name': dapp.get('name'),
            'slug': dapp.get('slug'),
            'logo': dapp.get('logo'),
            'deeplink': dapp.get('deeplink'),
            'categoryId': dapp.get('categoryId'),
            'activeChainId': '',
            'totalBalanceInFiat': dapp.get('totalBalanceInFiat'),
            'totalVolumeInFiat': dapp.get('totalVolumeInFiat'),
            'transactionCount': dapp.get('transactionCount'),
            'uawCount': dapp.get('uawCount')
        })
    else:
        # Create one row for each activeChainId
        for chain_id in active_chain_ids:
            csv_rows.append({
                'page': dapp.get('page'),
                'id': dapp.get('id'),
                'name': dapp.get('name'),
                'slug': dapp.get('slug'),
                'logo': dapp.get('logo'),
                'deeplink': dapp.get('deeplink'),
                'categoryId': dapp.get('categoryId'),
                'activeChainId': chain_id,
                'totalBalanceInFiat': dapp.get('totalBalanceInFiat'),
                'totalVolumeInFiat': dapp.get('totalVolumeInFiat'),
                'transactionCount': dapp.get('transactionCount'),
                'uawCount': dapp.get('uawCount')
            })

# Write to CSV
output_file = 'all_dapps_by_chain.csv'
fieldnames = [
    'page', 'id', 'name', 'slug', 'logo', 'deeplink', 
    'categoryId', 'activeChainId', 'totalBalanceInFiat', 
    'totalVolumeInFiat', 'transactionCount', 'uawCount'
]

with open(output_file, 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(csv_rows)

print(f"✅ Done!")
print(f"   Total dapps: {len(dapps)}")
print(f"   Total rows (with duplicates): {len(csv_rows)}")
print(f"   Saved to: {output_file}")

