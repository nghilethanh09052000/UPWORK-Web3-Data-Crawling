#!/usr/bin/env python3
"""
Verify the treasury calculation matches the expected value
"""
import json
import urllib.request
from datetime import datetime

def calculate_treasury_correct(chain_tvls, target_timestamp):
    """Calculate treasury using the corrected method"""
    own_tokens_by_timestamp = {}
    regular_chains_by_timestamp = {}
    
    for chain_name, chain_data in chain_tvls.items():
        tvl_list = chain_data.get('tvl', [])
        
        # Skip chains ending with "-OwnTokens" (they're subsets)
        if chain_name.endswith('-OwnTokens'):
            continue
        
        for entry in tvl_list:
            timestamp = entry.get('date')
            if timestamp:
                timestamp_str = str(timestamp)
                total_liquidity = entry.get('totalLiquidityUSD', 0)
                
                if total_liquidity > 0:
                    # Separate OwnTokens chain from regular chains
                    if chain_name == 'OwnTokens':
                        own_tokens_by_timestamp[timestamp_str] = total_liquidity
                    else:
                        # Regular chain (Ethereum, Arbitrum, etc.)
                        if timestamp_str not in regular_chains_by_timestamp:
                            regular_chains_by_timestamp[timestamp_str] = 0
                        regular_chains_by_timestamp[timestamp_str] += total_liquidity
    
    # Combine OwnTokens + regular chains for each timestamp
    all_timestamps = set(own_tokens_by_timestamp.keys()) | set(regular_chains_by_timestamp.keys())
    treasury_by_timestamp = {}
    for timestamp_str in all_timestamps:
        own_tokens_value = own_tokens_by_timestamp.get(timestamp_str, 0)
        regular_chains_value = regular_chains_by_timestamp.get(timestamp_str, 0)
        treasury_by_timestamp[timestamp_str] = own_tokens_value + regular_chains_value
    
    return treasury_by_timestamp

def test_verification():
    """Test the corrected calculation"""
    slug = "aave"
    target_timestamp = 1675468800
    
    print(f"Verifying treasury calculation for {slug}")
    print(f"Target timestamp: {target_timestamp} ({datetime.fromtimestamp(target_timestamp).strftime('%Y-%m-%d')})")
    print("-" * 80)
    
    url = f'https://api.llama.fi/treasury/{slug}'
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode('utf-8'))
            chain_tvls = data.get('chainTvls', {})
            
            treasury_by_timestamp = calculate_treasury_correct(chain_tvls, target_timestamp)
            
            timestamp_str = str(target_timestamp)
            calculated_value = treasury_by_timestamp.get(timestamp_str, 0)
            
            print(f"\nCalculated treasury value: {calculated_value:,}")
            print(f"Expected (UI): 157,330,000")
            print(f"Difference: {abs(calculated_value - 157330000):,}")
            
            if abs(calculated_value - 157330000) < 10000:
                print("\n✅ SUCCESS! Calculation matches UI value")
            else:
                print("\n❌ FAILED! Calculation doesn't match")
                
            # Show breakdown
            print("\nBreakdown:")
            own_tokens_value = 0
            regular_chains_sum = 0
            
            for chain_name, chain_data in chain_tvls.items():
                if chain_name.endswith('-OwnTokens'):
                    continue
                tvl_list = chain_data.get('tvl', [])
                for entry in tvl_list:
                    if entry.get('date') == target_timestamp:
                        value = entry.get('totalLiquidityUSD', 0)
                        if value > 0:
                            if chain_name == 'OwnTokens':
                                own_tokens_value = value
                                print(f"  OwnTokens: {value:,}")
                            else:
                                regular_chains_sum += value
                                print(f"  {chain_name}: {value:,}")
            
            print(f"\n  OwnTokens: {own_tokens_value:,}")
            print(f"  Regular chains sum: {regular_chains_sum:,}")
            print(f"  Total: {own_tokens_value + regular_chains_sum:,}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_verification()
