import json
import csv
from collections import defaultdict


def is_github_link(url):
    """Check if URL is actually a GitHub link"""
    if not url:
        return False
    url_lower = url.lower()
    return '//github.com' in url_lower or url_lower.startswith('github.com')


def is_twitter_link(url):
    """Check if URL is actually a Twitter/X link"""
    if not url:
        return False
    url_lower = url.lower()
    return ('//twitter.com' in url_lower or url_lower.startswith('twitter.com') or
            '//x.com/' in url_lower or url_lower.startswith('x.com/'))


def clean_coingecko_data(input_file, output_csv):
    """
    Transform CoinGecko extracted data into clean CSV format
    with numbered columns (github, github2, github3, etc.)
    """
    
    # Read the JSON data
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Process each entry
    coins = []
    
    for item in data:
        coin = {
            'name': item.get('name', ''),
            'symbol': item.get('symbol', ''),
            'chain_name': item.get('chain_name', ''),
            'github': [],
            'twitter': [],
            'website': []
        }
        
        # Process github (it's a list)
        github_list = item.get('github', [])
        if isinstance(github_list, list):
            for gh in github_list:
                if gh and gh not in coin['github']:
                    coin['github'].append(gh)
        
        # Process twitter (it's a string)
        twitter = item.get('twitter', '')
        if twitter:
            coin['twitter'].append(twitter)
        
        # Process website (it's a string)
        website = item.get('website', '')
        if website:
            coin['website'].append(website)
        
        coins.append(coin)
    
    # Find maximum number of links for each type
    max_github = max(len(c['github']) for c in coins) if coins else 0
    max_twitter = max(len(c['twitter']) for c in coins) if coins else 0
    max_website = max(len(c['website']) for c in coins) if coins else 0
    
    # Create CSV headers
    headers = ['name', 'symbol', 'chain_name']
    
    # Add github columns
    if max_github > 0:
        headers.append('github')
        for i in range(2, max_github + 1):
            headers.append(f'github{i}')
    
    # Add twitter columns
    if max_twitter > 0:
        headers.append('twitter')
        for i in range(2, max_twitter + 1):
            headers.append(f'twitter{i}')
    
    # Add website columns
    if max_website > 0:
        headers.append('website')
        for i in range(2, max_website + 1):
            headers.append(f'website{i}')
    
    # Write to CSV
    with open(output_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        
        for coin in coins:
            row = {
                'name': coin['name'],
                'symbol': coin['symbol'],
                'chain_name': coin['chain_name']
            }
            
            # Add github links
            for i, github in enumerate(coin['github'], 1):
                if i == 1:
                    row['github'] = github
                else:
                    row[f'github{i}'] = github
            
            # Add twitter links
            for i, twitter in enumerate(coin['twitter'], 1):
                if i == 1:
                    row['twitter'] = twitter
                else:
                    row[f'twitter{i}'] = twitter
            
            # Add website links
            for i, website in enumerate(coin['website'], 1):
                if i == 1:
                    row['website'] = website
                else:
                    row[f'website{i}'] = website
            
            writer.writerow(row)
    
    print(f"\n{'='*60}")
    print(f"✓ Successfully processed {len(coins)} coins")
    print(f"\n✓ Maximum links found per coin:")
    print(f"  - GitHub: {max_github}")
    print(f"  - Twitter: {max_twitter}")
    print(f"  - Website: {max_website}")
    print(f"\n✓ Saved to {output_csv}")
    print(f"{'='*60}")


if __name__ == '__main__':
    import sys
    import os
    
    # Default files
    default_input = 'coingecko_extracted.json'
    default_output = 'coingecko_data_clean.csv'
    
    # Allow command line arguments
    input_file = sys.argv[1] if len(sys.argv) > 1 else default_input
    output_file = sys.argv[2] if len(sys.argv) > 2 else default_output
    
    if not os.path.exists(input_file):
        print(f"❌ Error: Input file not found: {input_file}")
        print(f"\nUsage: python3 {sys.argv[0]} [input_json] [output_csv]")
        print(f"Example: python3 {sys.argv[0]} coingecko_extracted.json coingecko_clean.csv")
        sys.exit(1)
    
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    
    clean_coingecko_data(input_file, output_file)
