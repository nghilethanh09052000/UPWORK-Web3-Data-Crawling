import csv
import sys
import os

def add_category_column(category_name):
    """Add category column to existing CSV"""
    
    input_file = f"data_{category_name}/dapp_history_by_chain.csv"
    
    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        return
    
    print(f"Processing: {input_file}")
    print("="*60)
    
    # Read all rows
    rows = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            # Add category column
            row['Category'] = category_name
            rows.append(row)
    
    print(f"Read {len(rows)} rows")
    
    # Write back with new column
    fieldnames = ['Date', 'Dapp Name', 'Chain Name', 'UAW', 'Transactions', 'Volume', 'Category']
    
    with open(input_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"✅ Added 'Category' column with value: {category_name}")
    print(f"   Updated: {input_file}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 add_category_column.py <category_name>")
        print("Example: python3 add_category_column.py games")
        sys.exit(1)
    
    category = sys.argv[1]
    add_category_column(category)

