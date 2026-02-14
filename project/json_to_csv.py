#!/usr/bin/env python3
"""
Convert protocol_details.json to CSV format
"""

import json
import csv
import os

def main():
    # File paths
    project_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(project_dir, 'protocol_details.json')
    output_file = os.path.join(project_dir, 'protocol_details.csv')
    
    # Load JSON file
    print(f"Loading {input_file}...")
    with open(input_file, 'r', encoding='utf-8') as f:
        protocols = json.load(f)
    
    print(f"Loaded {len(protocols)} protocols")
    
    if not protocols:
        print("No data to convert!")
        return
    
    # Get all field names from first protocol (handle different structures)
    fieldnames = list(protocols[0].keys())
    
    # Handle chains field if it's a list (convert to string)
    print(f"Fields: {fieldnames}")
    print(f"Converting to CSV...")
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for protocol in protocols:
            # Convert lists/dicts to JSON strings for CSV compatibility
            row = {}
            for key, value in protocol.items():
                if isinstance(value, (list, dict)):
                    # Convert list/dict to JSON string
                    row[key] = json.dumps(value) if value else ''
                elif value is None:
                    row[key] = ''
                else:
                    row[key] = value
            writer.writerow(row)
    
    print("="*60)
    print("SUMMARY:")
    print(f"  Protocols converted: {len(protocols)}")
    print(f"  Output file: {output_file}")
    print(f"  Fields: {len(fieldnames)}")
    print("="*60)
    print("✅ Done!")

if __name__ == "__main__":
    main()

