import json
import csv
from collections import defaultdict


def is_github_link(url):
    """Check if URL is actually a GitHub link"""
    if not url:
        return False
    url_lower = url.lower()
    # Check for github.com as actual domain, not substring
    return '//github.com' in url_lower or url_lower.startswith('github.com')


def is_twitter_link(url):
    """Check if URL is actually a Twitter/X link"""
    if not url:
        return False
    url_lower = url.lower()
    # Check for twitter.com or x.com as actual domains, not substrings
    # Must have // before domain (http://twitter.com or https://x.com)
    # OR start with the domain directly (twitter.com/user)
    return ('//twitter.com' in url_lower or url_lower.startswith('twitter.com') or
            '//x.com/' in url_lower or url_lower.startswith('x.com/'))


def clean_protocol_data(input_file, output_csv):
    """
    Transform protocol data where multiple rows exist for the same protocol
    into a single row with numbered columns (github2, twitter2, website2, etc.)
    Also validates and reclassifies misplaced links.
    """
    
    # Read the JSON data
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Group data by name and defillama_url
    protocols = defaultdict(lambda: {
        'name': '',
        'defillama_url': '',
        'github': [],
        'twitter': [],
        'website': []
    })
    
    misclassified_count = {'github': 0, 'twitter': 0}
    
    for item in data:
        key = (item['name'], item['defillama_url'])
        
        if not protocols[key]['name']:
            protocols[key]['name'] = item['name']
            protocols[key]['defillama_url'] = item['defillama_url']
        
        # Validate and collect github links
        if item['github']:
            if is_github_link(item['github']):
                if item['github'] not in protocols[key]['github']:
                    protocols[key]['github'].append(item['github'])
            else:
                # Move to website if not actually GitHub
                if item['github'] not in protocols[key]['website']:
                    protocols[key]['website'].append(item['github'])
                    misclassified_count['github'] += 1
                    print(f"⚠️  Reclassified non-GitHub link to website: {item['github']} (protocol: {item['name']})")
        
        # Validate and collect twitter links
        if item['twitter']:
            if is_twitter_link(item['twitter']):
                if item['twitter'] not in protocols[key]['twitter']:
                    protocols[key]['twitter'].append(item['twitter'])
            else:
                # Move to website if not actually Twitter/X
                if item['twitter'] not in protocols[key]['website']:
                    protocols[key]['website'].append(item['twitter'])
                    misclassified_count['twitter'] += 1
                    print(f"⚠️  Reclassified non-Twitter link to website: {item['twitter']} (protocol: {item['name']})")
        
        # Collect website links (no validation needed, already validated above)
        if item['website'] and item['website'] not in protocols[key]['website']:
            protocols[key]['website'].append(item['website'])
    
    # Find maximum number of links for each type
    max_github = max(len(p['github']) for p in protocols.values()) if protocols else 0
    max_twitter = max(len(p['twitter']) for p in protocols.values()) if protocols else 0
    max_website = max(len(p['website']) for p in protocols.values()) if protocols else 0
    
    # Create CSV headers
    headers = ['name', 'defillama_url']
    
    # Add github columns
    headers.append('github')
    for i in range(2, max_github + 1):
        headers.append(f'github{i}')
    
    # Add twitter columns
    headers.append('twitter')
    for i in range(2, max_twitter + 1):
        headers.append(f'twitter{i}')
    
    # Add website columns
    headers.append('website')
    for i in range(2, max_website + 1):
        headers.append(f'website{i}')
    
    # Write to CSV
    with open(output_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        
        for protocol in protocols.values():
            row = {
                'name': protocol['name'],
                'defillama_url': protocol['defillama_url']
            }
            
            # Add github links
            for i, github in enumerate(protocol['github'], 1):
                if i == 1:
                    row['github'] = github
                else:
                    row[f'github{i}'] = github
            
            # Add twitter links
            for i, twitter in enumerate(protocol['twitter'], 1):
                if i == 1:
                    row['twitter'] = twitter
                else:
                    row[f'twitter{i}'] = twitter
            
            # Add website links
            for i, website in enumerate(protocol['website'], 1):
                if i == 1:
                    row['website'] = website
                else:
                    row[f'website{i}'] = website
            
            writer.writerow(row)
    
    print(f"\n{'='*60}")
    print(f"✓ Successfully processed {len(protocols)} unique protocols")
    print(f"\n✓ Maximum links found per protocol:")
    print(f"  - GitHub: {max_github}")
    print(f"  - Twitter: {max_twitter}")
    print(f"  - Website: {max_website}")
    
    if misclassified_count['github'] > 0 or misclassified_count['twitter'] > 0:
        print(f"\n✓ Reclassified misplaced links:")
        if misclassified_count['github'] > 0:
            print(f"  - Non-GitHub links moved to website: {misclassified_count['github']}")
        if misclassified_count['twitter'] > 0:
            print(f"  - Non-Twitter links moved to website: {misclassified_count['twitter']}")
    
    print(f"\n✓ Saved to {output_csv}")
    print(f"{'='*60}")


if __name__ == '__main__':
    import sys
    import os
    
    # Default files
    default_input = 'project/protocol_data.json'
    default_output = 'protocol_data_clean.csv'
    
    # Check if raw_protocol_data.json exists, otherwise use protocol_data.json
    if not os.path.exists(default_input):
        default_input = 'project/protocol_data.json'
    
    # Allow command line arguments
    input_file = sys.argv[1] if len(sys.argv) > 1 else default_input
    output_file = sys.argv[2] if len(sys.argv) > 2 else default_output
    
    print(f"Input: {input_file}")
    print(f"Output: {output_file}")
    
    clean_protocol_data(input_file, output_file)

