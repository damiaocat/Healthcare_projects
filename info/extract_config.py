import json
import csv

# Load JSON data
with open('config.json', 'r') as file:
    data = json.load(file)

# Extract and save to CSV
with open('job_configurations.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Write header
    writer.writerow(['job_id', 'job_name', 'source_path', 'is_auto_increment', 
                     'target_file_path', 'target_dataset_table', 'fully_qualified_name'])
    
    # Write data rows
    for job in data:
        writer.writerow([
            job.get('_id', ''),
            job.get('name', ''),
            job.get('sourceConfiguration', {}).get('sourcePath', ''),
            job.get('targetConfiguration', {}).get('isAutoIncrement', False),
            job.get('targetConfiguration', {}).get('filePath', ''),
            job.get('targetConfiguration', {}).get('targetDatasetTable', ''),
            job.get('targetConfiguration', {}).get('fullyQualifiedName', '')
        ])

print(f"Extracted {len(data)} configurations to job_configurations.csv")