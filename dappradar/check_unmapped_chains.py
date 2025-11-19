import csv
from collections import Counter

print("Reading dapp_history_by_chain.csv...")
print("="*60)

unmapped_chains = []
all_chains = Counter()

with open('dapp_history_by_chain.csv', 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    
    for i, row in enumerate(reader, 1):
        chain_name = row['Chain Name']
        all_chains[chain_name] += 1
        
        # Check if chain starts with "chain_"
        if chain_name.startswith('chain_'):
            unmapped_chains.append({
                'row': i,
                'date': row['Date'],
                'dapp_name': row['Dapp Name'],
                'chain_name': chain_name,
                'uaw': row['UAW'],
                'transactions': row['Transactions'],
                'volume': row['Volume']
            })
        
        if i % 500000 == 0:
            print(f"Processed {i:,} rows...")

print("="*60)
print(f"Total rows processed: {i:,}")
print(f"Unmapped chains found: {len(unmapped_chains)}")
print()

# Show unique unmapped chain IDs
if unmapped_chains:
    unmapped_chain_names = set(row['chain_name'] for row in unmapped_chains)
    print(f"Unique unmapped chains: {len(unmapped_chain_names)}")
    print("="*60)
    
    for chain in sorted(unmapped_chain_names):
        chain_id = chain.replace('chain_', '')
        count = sum(1 for row in unmapped_chains if row['chain_name'] == chain)
        print(f"  {chain} (ID: {chain_id}) - {count:,} rows")
    
    print()
    print("Sample unmapped rows (first 10):")
    print("="*60)
    for row in unmapped_chains[:10]:
        print(f"Row {row['row']:,}: {row['dapp_name']} | {row['chain_name']} | Date: {row['date']} | UAW: {row['uaw']}")

print()
print("All chains in the file:")
print("="*60)
for chain, count in all_chains.most_common():
    print(f"  {chain}: {count:,} rows")

# Export unmapped to CSV for easier viewing
if unmapped_chains:
    output_file = "unmapped_chains.csv"
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['row', 'date', 'dapp_name', 'chain_name', 'uaw', 'transactions', 'volume']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unmapped_chains)
    
    print()
    print(f"✅ Exported unmapped rows to: {output_file}")

