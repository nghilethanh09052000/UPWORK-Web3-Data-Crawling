#!/usr/bin/env python3
"""
Script to find protocols in protocols_list.json that are NOT in protocol_details.json
This helps identify which protocols need to be recrawled.
"""

import json
import os

def extract_slug_from_protocol_details(protocol_detail):
    """Extract slug from protocol_details.json entry"""
    # Try to extract slug from logo URL: https://icons.llamao.fi/icons/protocols/{slug}
    logo = protocol_detail.get('logo', '')
    if logo and 'protocols/' in logo:
        slug = logo.split('protocols/')[-1]
        return slug
    
    # Fallback: try to derive from name (lowercase, replace spaces with hyphens)
    name = protocol_detail.get('name', '').lower()
    if name:
        slug = name.replace(' ', '-').replace('(', '').replace(')', '')
        return slug
    
    return None

def main():
    # File paths
    project_dir = os.path.dirname(os.path.abspath(__file__))
    protocols_list_file = os.path.join(project_dir, 'protocols_list.json')
    protocol_details_file = os.path.join(project_dir, 'protocol_details.json')
    
    # Load protocols_list.json
    print(f"Loading {protocols_list_file}...")
    with open(protocols_list_file, 'r', encoding='utf-8') as f:
        protocols_list = json.load(f)
    
    print(f"Found {len(protocols_list)} protocols in protocols_list.json")
    
    # Load protocol_details.json
    print(f"Loading {protocol_details_file}...")
    with open(protocol_details_file, 'r', encoding='utf-8') as f:
        protocol_details = json.load(f)
    
    print(f"Found {len(protocol_details)} protocols in protocol_details.json")
    
    # Create a set of slugs from protocol_details.json
    crawled_slugs = set()
    for detail in protocol_details:
        slug = extract_slug_from_protocol_details(detail)
        if slug:
            crawled_slugs.add(slug)
        # Also try matching by name (case-insensitive)
        name = detail.get('name', '').lower()
        if name:
            crawled_slugs.add(name.lower())
    
    print(f"Extracted {len(crawled_slugs)} unique slugs/names from protocol_details.json")
    print("="*60)
    
    # Find missing protocols
    missing_protocols = []
    for protocol in protocols_list:
        slug = protocol.get('slug', '')
        name = protocol.get('name', '').lower()
        
        # Check if protocol is missing (by slug or name)
        if slug and slug not in crawled_slugs and name not in crawled_slugs:
            missing_protocols.append(protocol)
    
    print(f"\nFound {len(missing_protocols)} missing protocols:")
    print("="*60)
    
    # Save missing protocols to new file
    output_file = os.path.join(project_dir, 'protocols_list_missing.json')
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(missing_protocols, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Saved {len(missing_protocols)} missing protocols to: {output_file}")
    
    # Print first 10 missing protocols as sample
    if missing_protocols:
        print("\n📋 Sample of missing protocols (first 10):")
        for i, protocol in enumerate(missing_protocols[:10], 1):
            slug = protocol.get('slug', 'N/A')
            name = protocol.get('name', 'N/A')
            print(f"  {i}. {name} (slug: {slug})")
        
        if len(missing_protocols) > 10:
            print(f"  ... and {len(missing_protocols) - 10} more")
    
    # Also create a summary
    print("\n" + "="*60)
    print("SUMMARY:")
    print(f"  Total protocols in protocols_list.json: {len(protocols_list)}")
    print(f"  Protocols already crawled: {len(protocols_list) - len(missing_protocols)}")
    print(f"  Protocols missing: {len(missing_protocols)}")
    print("="*60)
    
    # Instructions
    print("\n💡 To recrawl missing protocols:")
    print(f"  1. Backup current protocols_list.json")
    print(f"  2. Replace protocols_list.json with protocols_list_missing.json")
    print(f"  3. Run: scrapy crawl defillama_protocol_details -o protocol_details_new.json")
    print(f"  4. Merge protocol_details.json and protocol_details_new.json")

if __name__ == "__main__":
    main()

