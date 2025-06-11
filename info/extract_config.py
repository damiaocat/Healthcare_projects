import json
import csv

# Load JSON with error handling
try:
    data = json.load(open('config.json'))
except:
    print("Error loading JSON file")
    exit()

# Open CSV file
with open('output.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    
    # Write header
    writer.writerow(['_id', 'name', 'sourcePath', 'isAutoIncrement', 'filePath', 'targetDatasetTable', 'fullyQualifiedName'])
    
    # Loop through each item
    for item in data:
        try:
            # Get nested values safely
            source_config = item.get('sourceConfiguration') or {}
            target_config = item.get('targetConfiguration') or {}
            
            writer.writerow([
                item.get('_id') or '',
                item.get('name') or '',
                source_config.get('sourcePath') or '',
                target_config.get('isAutoIncrement') or '',
                target_config.get('filePath') or '',
                target_config.get('targetDatasetTable') or '',
                target_config.get('fullyQualifiedName') or ''
            ])
        except:
            # If any error, write a row with empty values
            writer.writerow(['', '', '', '', '', '', ''])

print("Done!")