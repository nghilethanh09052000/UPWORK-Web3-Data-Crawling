import csv

def remove_twitter2_column(input_file, output_file):
    """
    Remove the 'twitter2' column from CSV file
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Get all fieldnames except twitter2
        fieldnames = [field for field in reader.fieldnames if field != 'twitter2']
        
        # Read all rows
        rows = []
        for row in reader:
            # Remove twitter2 from each row
            if 'twitter2' in row:
                del row['twitter2']
            rows.append(row)
    
    # Write back to file
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"✓ Removed 'twitter2' column from {input_file}")
    print(f"✓ Saved to {output_file}")
    print(f"✓ Total rows: {len(rows)}")


if __name__ == '__main__':
    import sys
    
    # Default files
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'protocol_data_clean.csv'
    output_file = sys.argv[2] if len(sys.argv) > 2 else input_file  # Overwrite by default
    
    remove_twitter2_column(input_file, output_file)

