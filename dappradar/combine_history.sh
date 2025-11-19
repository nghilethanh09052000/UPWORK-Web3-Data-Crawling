#!/bin/bash

echo "=========================================="
echo "Combining dapp_history_by_chain.csv"
echo "=========================================="

OUTPUT_FILE="combined_dapp_history.csv"

# Remove old file if exists
rm -f "$OUTPUT_FILE"

# Find all data_* folders with dapp_history_by_chain.csv
first_file=true
count=0

for folder in data_*/; do
    if [ -f "${folder}dapp_history_by_chain.csv" ]; then
        count=$((count + 1))
        if [ "$first_file" = true ]; then
            # First file: include header
            cat "${folder}dapp_history_by_chain.csv" > "$OUTPUT_FILE"
            first_file=false
            echo "  ✅ ${folder}dapp_history_by_chain.csv (with header)"
        else
            # Subsequent files: skip header (first line)
            tail -n +2 "${folder}dapp_history_by_chain.csv" >> "$OUTPUT_FILE"
            echo "  ✅ ${folder}dapp_history_by_chain.csv"
        fi
    fi
done

echo ""
echo "=========================================="
if [ -f "$OUTPUT_FILE" ]; then
    lines=$(wc -l < "$OUTPUT_FILE")
    size=$(ls -lh "$OUTPUT_FILE" | awk '{print $5}')
    echo "✅ Created: $OUTPUT_FILE"
    echo "   Categories: $count"
    echo "   Total lines: $lines"
    echo "   File size: $size"
else
    echo "⚠️  No dapp_history_by_chain.csv files found"
fi
echo "=========================================="

