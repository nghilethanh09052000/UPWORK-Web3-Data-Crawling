#!/usr/bin/env python3
"""
Script to remove duplicate entries from protocol_details_1.json by slug name.
Slug is extracted from the logo URL: https://icons.llamao.fi/icons/protocols/{slug}
"""

import json
import os

def extract_slug_from_logo(logo_url):
    """Extract slug from logo URL"""
    if logo_url and 'protocols/' in logo_url:
        slug = logo_url.split('protocols/')[-1]
        # Remove trailing slash if present
        slug = slug.rstrip('/')
        return slug
    return None

def main():
    # File paths
    project_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(project_dir, 'protocol_details_1.json')
    output_file = os.path.join(project_dir, 'protocol_details_1.json')  # Overwrite same file
    
    # Load JSON file
    print(f"Loading {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        protocols = json.load(f)
    
    print(f"Loaded {len(protocols)} protocols")
    
    # Deduplicate by slug
    seen_slugs = {}
    unique_protocols = []
    duplicates_count = 0
    
    for protocol in protocols:
        logo = protocol.get('logo', '')
        slug = extract_slug_from_logo(logo)
        
        if slug:
            if slug not in seen_slugs:
                # First occurrence - keep it
                seen_slugs[slug] = True
                unique_protocols.append(protocol)
            else:
                # Duplicate - skip it
                duplicates_count += 1
                name = protocol.get('name', 'N/A')
                print(f"  Removing duplicate: {name} (slug: {slug})")
        else:
            # No slug found - keep it but warn
            name = protocol.get('name', 'N/A')
            print(f"  Warning: Could not extract slug for {name}, keeping anyway")
            unique_protocols.append(protocol)
    
    # Save deduplicated data
    print(f"\nSaving deduplicated data to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(unique_protocols, f, indent=2, ensure_ascii=False)
    
    print("="*60)
    print("SUMMARY:")
    print(f"  Original protocols: {len(protocols)}")
    print(f"  Unique protocols: {len(unique_protocols)}")
    print(f"  Duplicates removed: {duplicates_count}")
    print(f"  Saved to: {output_file}")
    print("="*60)
    print("✅ Done!")

if __name__ == "__main__":
    main()

