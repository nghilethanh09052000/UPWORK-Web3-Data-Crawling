#!/usr/bin/env python3
"""
Test script to verify treasury calculation from API
"""
import json
import urllib.request
from datetime import datetime

def test_treasury_calculation():
    """Test treasury API and calculate values for timestamp 1675468800"""
    slug = "aave"
    target_timestamp = 1675468800
    
    print(f"Testing treasury API for {slug}")
    print(f"Target timestamp: {target_timestamp} ({datetime.fromtimestamp(target_timestamp).strftime('%Y-%m-%d')})")
    print("-" * 80)
    
    # Fetch treasury data
    url = f'https://api.llama.fi/treasury/{slug}'
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            if response.getcode() != 200:
                print(f"❌ API returned status {response.getcode()}")
                return
            
            data = json.loads(response.read().decode('utf-8'))
        
        # Check structure
        print(f"Top-level keys: {list(data.keys())[:10]}")
        print()
        
        # Get chainTvls
        chain_tvls = data.get('chainTvls', {})
        print(f"Number of chains: {len(chain_tvls)}")
        print(f"Chain names: {list(chain_tvls.keys())}")
        print()
        
        # Method 1: Sum all chains
        print("=" * 80)
        print("METHOD 1: Sum of ALL chains")
        print("=" * 80)
        total_all_chains = 0
        chain_values = {}
        for chain_name, chain_data in chain_tvls.items():
            tvl_list = chain_data.get('tvl', [])
            for entry in tvl_list:
                if entry.get('date') == target_timestamp:
                    value = entry.get('totalLiquidityUSD', 0)
                    if value > 0:
                        chain_values[chain_name] = value
                        total_all_chains += value
                        print(f"  {chain_name}: {value:,}")
        print(f"\nTotal (all chains): {total_all_chains:,}")
        print(f"Expected: 157,330,000")
        print(f"Match: {'✅' if abs(total_all_chains - 157330000) < 10000 else '❌'}")
        print()
        
        # Method 2: Sum only OwnTokens chains
        print("=" * 80)
        print("METHOD 2: Sum of OwnTokens chains only")
        print("=" * 80)
        total_own_tokens = 0
        for chain_name, chain_data in chain_tvls.items():
            if 'OwnTokens' in chain_name:
                tvl_list = chain_data.get('tvl', [])
                for entry in tvl_list:
                    if entry.get('date') == target_timestamp:
                        value = entry.get('totalLiquidityUSD', 0)
                        if value > 0:
                            print(f"  {chain_name}: {value:,}")
                            total_own_tokens += value
        print(f"\nTotal (OwnTokens only): {total_own_tokens:,}")
        print()
        
        # Method 3: Sum non-OwnTokens chains
        print("=" * 80)
        print("METHOD 3: Sum of non-OwnTokens chains")
        print("=" * 80)
        total_non_own = 0
        for chain_name, chain_data in chain_tvls.items():
            if 'OwnTokens' not in chain_name:
                tvl_list = chain_data.get('tvl', [])
                for entry in tvl_list:
                    if entry.get('date') == target_timestamp:
                        value = entry.get('totalLiquidityUSD', 0)
                        if value > 0:
                            print(f"  {chain_name}: {value:,}")
                            total_non_own += value
        print(f"\nTotal (non-OwnTokens): {total_non_own:,}")
        print()
        
        # Method 4: Check if there's a root-level tvl
        print("=" * 80)
        print("METHOD 4: Root-level tvl")
        print("=" * 80)
        if 'tvl' in data:
            tvl_data = data['tvl']
            if isinstance(tvl_data, list):
                for entry in tvl_data:
                    if entry.get('date') == target_timestamp:
                        print(f"Found root tvl: {entry}")
                        break
                else:
                    print("No matching timestamp in root tvl")
            else:
                print(f"Root tvl is not a list: {type(tvl_data)}")
        else:
            print("No root-level tvl found")
        print()
        
        # Method 5: Check what the UI might be showing
        # Maybe it's OwnTokens + specific chains?
        print("=" * 80)
        print("METHOD 5: OwnTokens + regular chains (excluding -OwnTokens)")
        print("=" * 80)
        own_tokens_value = chain_values.get('OwnTokens', 0)
        regular_chains_sum = 0
        for chain_name in chain_values:
            if chain_name != 'OwnTokens' and not chain_name.endswith('-OwnTokens'):
                regular_chains_sum += chain_values[chain_name]
                print(f"  {chain_name}: {chain_values[chain_name]:,}")
        
        total_method5 = own_tokens_value + regular_chains_sum
        print(f"\nOwnTokens: {own_tokens_value:,}")
        print(f"Regular chains: {regular_chains_sum:,}")
        print(f"Total: {total_method5:,}")
        print(f"Expected: 157,330,000")
        print(f"Match: {'✅' if abs(total_method5 - 157330000) < 10000 else '❌'}")
        print()
        
        # Show what the current CSV has
        print("=" * 80)
        print("CURRENT CSV VALUE")
        print("=" * 80)
        print(f"CSV treasury value: 280,244,287")
        print(f"This matches: {'✅' if abs(280244287 - total_all_chains) < 1000 else '❌'} (Method 1 - all chains)")
        print()
        
        # Recommendation
        print("=" * 80)
        print("RECOMMENDATION")
        print("=" * 80)
        if abs(total_method5 - 157330000) < 10000:
            print("✅ Use Method 5: OwnTokens + regular chains (excluding -OwnTokens)")
            print("   This matches the UI value of 157.33m$")
        elif abs(total_all_chains - 157330000) < 10000:
            print("✅ Use Method 1: Sum of all chains")
            print("   This matches the UI value of 157.33m$")
        else:
            print("⚠️  Need to investigate further")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_treasury_calculation()
