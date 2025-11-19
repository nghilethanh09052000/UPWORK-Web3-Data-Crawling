import json
from collections import defaultdict

print("Reading all_chains.json...")
print("="*60)

with open('all_chains.json', 'r') as f:
    chains = json.load(f)

# Group chains by tokenSymbol
token_to_chains = defaultdict(list)

for chain in chains:
    chain_id = chain.get('chainId')
    token_symbol = chain.get('tokenSymbol')
    token_slug = chain.get('tokenSlug')
    dapp_count = chain.get('dappCount')
    
    if token_symbol:
        token_to_chains[token_symbol].append({
            'chainId': chain_id,
            'tokenSlug': token_slug,
            'dappCount': dapp_count
        })

print(f"Total chains: {len(chains)}")
print(f"Unique token symbols: {len(token_to_chains)}")
print()

# Find tokens with multiple chains
duplicates = {symbol: chain_list for symbol, chain_list in token_to_chains.items() if len(chain_list) > 1}

print(f"Token symbols shared by multiple chains: {len(duplicates)}")
print("="*60)
print()

if duplicates:
    for token_symbol, chain_list in sorted(duplicates.items()):
        print(f"Token Symbol: {token_symbol}")
        print(f"  Used by {len(chain_list)} chains:")
        for chain_info in sorted(chain_list, key=lambda x: x['chainId']):
            print(f"    - Chain ID: {chain_info['chainId']:3d} | Token Slug: {chain_info['tokenSlug']:30s} | Dapps: {chain_info['dappCount']}")
        print()

# Also show single-use tokens
single_tokens = {symbol: chain_list for symbol, chain_list in token_to_chains.items() if len(chain_list) == 1}
print("="*60)
print(f"Token symbols used by only one chain: {len(single_tokens)}")
print("="*60)
for token_symbol, chain_list in sorted(single_tokens.items()):
    chain_info = chain_list[0]
    print(f"{token_symbol:10s} - Chain ID: {chain_info['chainId']:3d} | Token Slug: {chain_info['tokenSlug']:30s} | Dapps: {chain_info['dappCount']}")


