#!/usr/bin/env python3
"""
Simple script to fetch emission data from DefiLlama API and save to local JSON file.
"""

import json
import urllib.request
import urllib.error
import gzip
import csv
from datetime import datetime
from collections import defaultdict


def fetch_emission_data(protocol_slug="aave"):
    """
    Fetch emission data from DefiLlama API
    
    Args:
        protocol_slug: Protocol slug (default: "aave")
    
    Returns:
        dict: Parsed JSON data
    """
    url = f"https://api.llama.fi/emission/{protocol_slug}"
    
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "origin": "https://defillama.com",
        "referer": "https://defillama.com/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.0.0"
    }
    
    print(f"Fetching emission data from: {url}")
    
    try:
        # Create request with headers
        req = urllib.request.Request(url, headers=headers)
        
        # Make the request
        with urllib.request.urlopen(req, timeout=30) as response:
            response_bytes = response.read()
            
            # Check if response is gzipped (starts with 0x1f 0x8b)
            if response_bytes[:2] == b'\x1f\x8b':
                response_data = gzip.decompress(response_bytes).decode('utf-8')
            else:
                response_data = response_bytes.decode('utf-8')
        
        # Parse the response
        data = json.loads(response_data)
        
        # Extract the body field which contains a JSON string
        body_str = data.get("body", "")
        last_modified = data.get("lastModified", "")
        
        print(f"✅ Successfully fetched data")
        print(f"📅 Last Modified: {last_modified}")
        
        if not body_str:
            print("⚠️  Warning: 'body' field is empty")
            return None
        
        # Parse the JSON string in the body field
        try:
            parsed_body = json.loads(body_str)
            return {
                "lastModified": last_modified,
                "data": parsed_body
            }
        except json.JSONDecodeError as e:
            print(f"❌ Error parsing JSON in body field: {e}")
            print(f"Body preview (first 500 chars): {body_str[:500]}")
            return None
            
    except urllib.error.URLError as e:
        print(f"❌ Error fetching data: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing response JSON: {e}")
        return None


def save_to_file(data, output_file="emission_data.json"):
    """
    Save parsed data to a local JSON file
    
    Args:
        data: Dictionary containing the parsed data
        output_file: Output filename
    """
    if data is None:
        print("⚠️  No data to save")
        return
    
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Data saved to: {output_file}")
        
        # Print some basic info about the data
        if "data" in data:
            data_content = data["data"]
            if isinstance(data_content, dict):
                print(f"📊 Data keys: {list(data_content.keys())}")
            elif isinstance(data_content, list):
                print(f"📊 Data items: {len(data_content)}")
        
    except Exception as e:
        print(f"❌ Error saving file: {e}")


def aggregate_emission_data(parsed_body):
    """
    Aggregate emission data by timestamp from documentedData.data.data
    
    Args:
        parsed_body: Parsed JSON body containing emission data
    
    Returns:
        list: List of dictionaries with aggregated data by timestamp
    """
    # Dictionary to aggregate data by timestamp
    aggregated = defaultdict(lambda: {
        'unlocked': 0,
        'burned': 0,
        'rawEmission': 0
    })
    
    # Navigate to documentedData.data
    documented_data = parsed_body.get('documentedData', {})
    data_array = documented_data.get('data', [])
    
    print(f"📊 Processing {len(data_array)} emission categories...")
    
    # Iterate through each category (e.g., "LEND core development")
    for category in data_array:
        label = category.get('label', 'Unknown')
        category_data = category.get('data', [])
        
        # Iterate through each entry in the category
        for entry in category_data:
            timestamp = entry.get('timestamp')
            unlocked = entry.get('unlocked', 0)
            burned = entry.get('burned', 0)
            raw_emission = entry.get('rawEmission', 0)
            
            if timestamp is not None:
                # Sum values for the same timestamp
                aggregated[timestamp]['unlocked'] += unlocked
                aggregated[timestamp]['burned'] += burned
                aggregated[timestamp]['rawEmission'] += raw_emission
    
    # Convert to list of dictionaries sorted by timestamp
    result = []
    for timestamp in sorted(aggregated.keys()):
        date_str = datetime.fromtimestamp(int(timestamp)).strftime('%Y-%m-%d')
        result.append({
            'timestamp': timestamp,
            'date': date_str,
            'unlock': aggregated[timestamp]['unlocked'],
            'burned': aggregated[timestamp]['burned'],
            'rawEmission': aggregated[timestamp]['rawEmission']
        })
    
    print(f"✅ Aggregated {len(result)} unique timestamps")
    return result


def export_to_csv(aggregated_data, csv_file="emission_aggregated.csv"):
    """
    Export aggregated emission data to CSV
    
    Args:
        aggregated_data: List of dictionaries with aggregated data
        csv_file: Output CSV filename
    """
    if not aggregated_data:
        print("⚠️  No data to export")
        return
    
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['timestamp', 'date', 'unlock', 'burned', 'rawEmission']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(aggregated_data)
        
        print(f"✅ CSV exported to: {csv_file}")
        print(f"📊 Total rows: {len(aggregated_data)}")
        
        # Show first few rows as preview
        if len(aggregated_data) > 0:
            print("\n📋 Preview (first 5 rows):")
            print(f"{'Timestamp':<15} {'Date':<12} {'Unlock':<15} {'Burned':<15} {'RawEmission':<15}")
            print("-" * 75)
            for row in aggregated_data[:5]:
                print(f"{row['timestamp']:<15} {row['date']:<12} {row['unlock']:<15.2f} {row['burned']:<15.2f} {row['rawEmission']:<15.2f}")
        
    except Exception as e:
        print(f"❌ Error exporting CSV: {e}")


def main():
    """Main function"""
    import sys
    
    # Allow protocol slug as command line argument
    protocol_slug = sys.argv[1] if len(sys.argv) > 1 else "aave"
    output_json = sys.argv[2] if len(sys.argv) > 2 else f"emission_{protocol_slug}.json"
    output_csv = sys.argv[3] if len(sys.argv) > 3 else f"emission_{protocol_slug}_aggregated.csv"
    
    print("=" * 60)
    print("DefiLlama Emission Data Fetcher & Aggregator")
    print("=" * 60)
    print(f"Protocol: {protocol_slug}")
    print(f"JSON output: {output_json}")
    print(f"CSV output: {output_csv}")
    print()
    
    # Fetch data
    data = fetch_emission_data(protocol_slug)
    
    if data:
        # Save JSON to file
        save_to_file(data, output_json)
        print()
        
        # Aggregate and export to CSV
        parsed_body = data.get('data', {})
        aggregated_data = aggregate_emission_data(parsed_body)
        
        if aggregated_data:
            export_to_csv(aggregated_data, output_csv)
        
        print()
        print("=" * 60)
        print("✅ Done!")
        print("=" * 60)
    else:
        print()
        print("=" * 60)
        print("❌ Failed to fetch or parse data")
        print("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()
