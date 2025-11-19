import csv
import os
from collections import defaultdict

print("Reading dapp_history_by_chain.csv...")
print("="*60)

# Read the big CSV and group by dapp
dapp_data = defaultdict(list)

with open('dapp_history_by_chain.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    
    for row in reader:
        dapp_name = row['Dapp Name']
        dapp_data[dapp_name].append(row)

print(f"Found {len(dapp_data)} unique dapps")
print("="*60)

# Create output directory
output_dir = "csv_data_by_chain"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    print(f"Created {output_dir} directory")

# Write separate CSV files for each dapp
saved_count = 0

fieldnames = ['Date', 'Dapp Name', 'Chain Name', 'UAW', 'Transactions', 'Volume']

for dapp_name, rows in dapp_data.items():
    # Use dapp name as filename, but replace invalid characters
    # Replace / \ : * ? " < > | with -
    safe_filename = dapp_name.replace('/', '-').replace('\\', '-').replace(':', '-').replace('*', '-').replace('?', '-').replace('"', '-').replace('<', '-').replace('>', '-').replace('|', '-')
    csv_file = os.path.join(output_dir, f"{safe_filename}.csv")
    
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    saved_count += 1
    if saved_count % 100 == 0:
        print(f"Processed {saved_count}/{len(dapp_data)} dapps...")

print("="*60)
print(f"✅ Done!")
print(f"   Saved: {saved_count} CSV files")
print(f"   Output directory: {output_dir}/")

