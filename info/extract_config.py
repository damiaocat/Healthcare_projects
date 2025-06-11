import json
import csv

# Load JSON data
with open('config.json', 'r') as file:
    data = json.load(file)

# Extract and save to CSV
with open('job_configurations.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    
    # Write header
    writer.writerow(['job_id', 'job_name', 'description', 'source_path', 'source_bucket', 
                     'is_auto_increment', 'target_file_path', 'target_dataset_table', 
                     'fully_qualified_name', 'owner_name', 'owner_email'])
    
    # Write data rows
    for job in data:
        # Get basic info
        job_id = job.get('_id', '')
        job_name = job.get('name', '')
        description = job.get('description', '')
        
        # Get owner info
        owner = job.get('owner', [{}])[0] if job.get('owner') else {}
        owner_name = owner.get('name', '') if owner else ''
        owner_email = owner.get('email', '') if owner else ''
        
        # Get source configuration
        source_config = job.get('sourceConfiguration', {})
        source_path = source_config.get('sourcePath', '')
        source_bucket = source_config.get('landingBucket', '')
        
        # Get target configuration  
        target_config = job.get('targetConfiguration', {})
        is_auto_increment = target_config.get('isAutoIncrement', False)
        target_file_path = target_config.get('filePath', '')
        target_dataset_table = target_config.get('targetDatasetTable', '')
        fully_qualified_name = target_config.get('fullyQualifiedName', '')
        
        writer.writerow([
            job_id,
            job_name, 
            description,
            source_path,
            source_bucket,
            is_auto_increment,
            target_file_path,
            target_dataset_table,
            fully_qualified_name,
            owner_name,
            owner_email
        ])

print(f"Extracted {len(data)} configurations to job_configurations.csv")

print(f"Extracted {len(data)} configurations to job_configurations.csv")

print(f"Extracted {len(data)} configurations to job_configurations.csv")