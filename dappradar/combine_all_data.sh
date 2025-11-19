#!/bin/bash

echo "=========================================="
echo "Combining all CSV data from categories"
echo "=========================================="

# Combine dapp_history_by_chain.csv from all data_* folders
echo ""
echo "📊 Combining dapp_history_by_chain.csv files..."
OUTPUT_HISTORY="combined_dapp_history.csv"

# Remove old combined file if exists
rm -f "$OUTPUT_HISTORY"

# Find all data_* folders with dapp_history_by_chain.csv
first_file=true
for folder in data_*/; do
    if [ -f "${folder}dapp_history_by_chain.csv" ]; then
        if [ "$first_file" = true ]; then
            # First file: include header
            cat "${folder}dapp_history_by_chain.csv" > "$OUTPUT_HISTORY"
            first_file=false
            echo "  ✅ ${folder}dapp_history_by_chain.csv (with header)"
        else
            # Subsequent files: skip header (first line)
            tail -n +2 "${folder}dapp_history_by_chain.csv" >> "$OUTPUT_HISTORY"
            echo "  ✅ ${folder}dapp_history_by_chain.csv"
        fi
    fi
done

if [ -f "$OUTPUT_HISTORY" ]; then
    lines=$(wc -l < "$OUTPUT_HISTORY")
    echo ""
    echo "✅ Created: $OUTPUT_HISTORY"
    echo "   Total lines: $lines"
else
    echo "⚠️  No dapp_history_by_chain.csv files found"
fi

# Combine dapp_details.csv from all data_* folders
echo ""
echo "📋 Combining dapp_details.csv files..."
OUTPUT_DETAILS="combined_dapp_details.csv"

# Remove old combined file if exists
rm -f "$OUTPUT_DETAILS"

# Find all data_* folders with dapp_details.csv
first_file=true
for folder in data_*/; do
    if [ -f "${folder}dapp_details.csv" ]; then
        if [ "$first_file" = true ]; then
            # First file: include header
            cat "${folder}dapp_details.csv" > "$OUTPUT_DETAILS"
            first_file=false
            echo "  ✅ ${folder}dapp_details.csv (with header)"
        else
            # Subsequent files: skip header (first line)
            tail -n +2 "${folder}dapp_details.csv" >> "$OUTPUT_DETAILS"
            echo "  ✅ ${folder}dapp_details.csv"
        fi
    fi
done

if [ -f "$OUTPUT_DETAILS" ]; then
    lines=$(wc -l < "$OUTPUT_DETAILS")
    echo ""
    echo "✅ Created: $OUTPUT_DETAILS"
    echo "   Total lines: $lines"
else
    echo "⚠️  No dapp_details.csv files found"
fi

echo ""
echo "=========================================="
echo "✅ Done!"
echo "=========================================="

