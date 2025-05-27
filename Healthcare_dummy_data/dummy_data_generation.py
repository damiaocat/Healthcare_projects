#!/usr/bin/env python3
"""
Healthcare Survey Data Generator - Clean + Messy Versions
Generates both clean baseline data and controlled messy data for ETL testing.
"""

import random
import csv
import datetime
from faker import Faker
import numpy as np
import pandas as pd
import json

fake = Faker()
random.seed(42)  # For reproducible results
np.random.seed(42)

# Schema from the provided image
SCHEMA = {
    'user_id': 'Integer',
    'room_id': 'Integer', 
    'survey_id': 'Integer',
    'survey_scale_result_id': 'Integer',
    'survey_created_at': 'DateTime',
    'survey_completed_at': 'DateTime',
    'survey_name': 'String',
    'scale_name': 'String',
    'scale_result_category': 'String',
    'scale_result': 'Integer',
    'baseline_scale_result': 'Integer',
    'improvement_from_initial': 'Float',
    'improvement_from_previous': 'Float',
    'is_latest': 'Integer',
    'initial_scale_result_value': 'Integer',
    'initial_scale_result_category': 'String',
    'improvement_from_initial_value': 'Float',
    'is_improvable': 'Integer',
    'is_sig_improvable_phq_gad': 'Float',
    'is_sig_improved_phq_gad': 'Float',
    'initial_survey_completed_at': 'DateTime',
    'file_date': 'DateTime'
}

# Sample realistic healthcare survey data
SURVEY_NAMES = [
    'PHQ-9 Depression Scale', 'GAD-7 Anxiety Scale', 'Beck Depression Inventory',
    'Hamilton Anxiety Scale', 'DASS-21', 'Edinburgh Postnatal Depression Scale',
    'Generalized Anxiety Disorder Scale', 'Patient Health Questionnaire',
    'Mental Health Inventory', 'Stress Assessment Scale'
]

SCALE_NAMES = [
    'PHQ-9', 'GAD-7', 'BDI-II', 'HAM-A', 'DASS-21', 'EPDS',
    'Depression Scale', 'Anxiety Scale', 'Stress Scale'
]

SCALE_CATEGORIES = [
    'Minimal', 'Mild', 'Moderate', 'Moderately Severe', 'Severe',
    'Normal', 'Low', 'High', 'Clinical', 'Subclinical'
]

class DataQualityTracker:
    """Track which records have which data quality issues"""
    def __init__(self):
        self.issues = {}
        self.issue_counts = {}
    
    def add_issue(self, record_id, issue_type, field_name, original_value, corrupted_value):
        if record_id not in self.issues:
            self.issues[record_id] = []
        
        self.issues[record_id].append({
            'issue_type': issue_type,
            'field': field_name,
            'original': original_value,
            'corrupted': corrupted_value
        })
        
        if issue_type not in self.issue_counts:
            self.issue_counts[issue_type] = 0
        self.issue_counts[issue_type] += 1
    
    def save_report(self, filename):
        """Save detailed issue report"""
        report = {
            'summary': self.issue_counts,
            'total_records_with_issues': len(self.issues),
            'detailed_issues': self.issues
        }
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)

def generate_base_record(record_id):
    """Generate a clean, valid healthcare survey record"""
    user_id = random.randint(1000, 99999)
    room_id = random.randint(100, 9999)
    survey_id = random.randint(1, 500)
    survey_scale_result_id = record_id
    
    # Generate clean dates in YYYY-MM-DD format
    base_date = fake.date_time_between(start_date='-2y', end_date='now')
    survey_created_at = base_date.strftime('%Y-%m-%d %H:%M:%S')
    survey_completed_at = (base_date + datetime.timedelta(hours=random.randint(1, 48))).strftime('%Y-%m-%d %H:%M:%S')
    initial_survey_completed_at = (base_date - datetime.timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d %H:%M:%S')
    file_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Generate survey info
    survey_name = random.choice(SURVEY_NAMES)
    scale_name = random.choice(SCALE_NAMES) 
    scale_result_category = random.choice(SCALE_CATEGORIES)
    initial_scale_result_category = random.choice(SCALE_CATEGORIES)
    
    # Generate clean numeric values
    scale_result = random.randint(0, 27)
    baseline_scale_result = random.randint(0, 27) 
    initial_scale_result_value = random.randint(0, 27)
    improvement_from_initial = round(random.uniform(-10.0, 15.0), 2)
    improvement_from_previous = round(random.uniform(-5.0, 10.0), 2)
    improvement_from_initial_value = round(random.uniform(-10.0, 15.0), 2)
    is_sig_improvable_phq_gad = round(random.uniform(0.0, 1.0), 3)
    is_sig_improved_phq_gad = round(random.uniform(0.0, 1.0), 3)
    is_latest = random.randint(0, 1)
    is_improvable = random.randint(0, 1)
    
    return [
        user_id, room_id, survey_id, survey_scale_result_id,
        survey_created_at, survey_completed_at, survey_name, scale_name,
        scale_result_category, scale_result, baseline_scale_result,
        improvement_from_initial, improvement_from_previous, is_latest,
        initial_scale_result_value, initial_scale_result_category,
        improvement_from_initial_value, is_improvable,
        is_sig_improvable_phq_gad, is_sig_improved_phq_gad,
        initial_survey_completed_at, file_date
    ]

def apply_targeted_corruption(record, record_id, tracker, corruption_probability=0.15):
    """Apply specific data quality issues to targeted records"""
    
    corrupted_record = record.copy()
    field_names = list(SCHEMA.keys())
    
    # Decide if this record should have issues
    if random.random() > corruption_probability:
        return corrupted_record, False  # Return clean record
    
    # Apply 1-3 issues per corrupted record (not all fields)
    num_issues = random.randint(1, 3)
    fields_to_corrupt = random.sample(range(len(record)), min(num_issues, len(record)))
    
    for field_idx in fields_to_corrupt:
        field_name = field_names[field_idx]
        data_type = SCHEMA[field_name]
        original_value = record[field_idx]
        
        # Choose specific issue type
        issue_type = random.choice([
            'missing_value', 'whitespace', 'wrong_type', 'negative_value',
            'special_chars', 'null_string', 'date_format', 'encoding'
        ])
        
        corrupted_value = original_value
        
        if issue_type == 'missing_value':
            corrupted_value = random.choice(['', None, 'NULL', 'null', 'N/A'])
            
        elif issue_type == 'whitespace':
            if original_value is not None:
                corrupted_value = random.choice([
                    f'  {original_value}', f'{original_value}  ', 
                    f'  {original_value}  ', f'\t{original_value}\n'
                ])
        
        elif issue_type == 'wrong_type':
            if data_type == 'Integer':
                corrupted_value = random.choice([
                    f'{original_value}.0', f'{original_value}.5', 'null',
                    f'"{original_value}"', f'{original_value}L'
                ])
            elif data_type == 'Float':
                corrupted_value = random.choice([
                    str(int(original_value)) if original_value else '0',
                    'null', 'NaN', f'"{original_value}"'
                ])
        
        elif issue_type == 'negative_value':
            if data_type in ['Integer', 'Float'] and isinstance(original_value, (int, float)) and original_value > 0:
                corrupted_value = -abs(original_value)
        
        elif issue_type == 'special_chars':
            if original_value is not None:
                chars = ['@', '#', '$', '%', '^', '&', '*']
                corrupted_value = f'{original_value}{random.choice(chars)}'
        
        elif issue_type == 'null_string':
            corrupted_value = random.choice(['null', 'NULL', 'None', 'undefined'])
        
        elif issue_type == 'date_format' and data_type == 'DateTime':
            try:
                date_obj = datetime.datetime.strptime(str(original_value), '%Y-%m-%d %H:%M:%S')
                bad_formats = ['%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%m-%d-%Y']
                corrupted_value = date_obj.strftime(random.choice(bad_formats))
            except:
                corrupted_value = 'invalid_date'
        
        elif issue_type == 'encoding':
            if isinstance(original_value, str):
                corrupted_value = f'{original_value}café'  # Add encoding issue
        
        corrupted_record[field_idx] = corrupted_value
        tracker.add_issue(record_id, issue_type, field_name, original_value, corrupted_value)
    
    return corrupted_record, True

def apply_csv_formatting_issues(records, tracker):
    """Apply CSV-specific formatting issues to select records"""
    
    problematic_records = []
    
    for i, record in enumerate(records):
        # Most records stay normal
        if random.random() > 0.05:  # Only 5% get CSV issues
            problematic_records.append(record)
            continue
        
        issue_type = random.choice([
            'extra_columns', 'missing_columns', 'quote_issues', 
            'line_breaks', 'delimiter_issues'
        ])
        
        if issue_type == 'extra_columns':
            new_record = record + ['extra1', 'extra2']
            tracker.add_issue(i, 'extra_columns', 'row_structure', len(record), len(new_record))
            
        elif issue_type == 'missing_columns':
            cols_to_remove = random.randint(1, 3)
            new_record = record[:-cols_to_remove]
            tracker.add_issue(i, 'missing_columns', 'row_structure', len(record), len(new_record))
            
        elif issue_type == 'quote_issues':
            new_record = record.copy()
            for j in range(len(new_record)):
                if new_record[j] is not None and random.random() < 0.3:
                    original = new_record[j]
                    new_record[j] = f'"{original}""extra"'  # Problematic quotes
                    tracker.add_issue(i, 'quote_issues', f'field_{j}', original, new_record[j])
            
        elif issue_type == 'line_breaks':
            new_record = record.copy()
            field_idx = random.randint(0, len(new_record)-1)
            if new_record[field_idx] is not None:
                original = new_record[field_idx]
                new_record[field_idx] = f'"{original}\nline\nbreak"'
                tracker.add_issue(i, 'line_breaks', f'field_{field_idx}', original, new_record[field_idx])
                
        elif issue_type == 'delimiter_issues':
            new_record = []
            for field in record:
                if field is not None and random.random() < 0.2:
                    new_field = str(field).replace(',', ';')
                    new_record.append(new_field)
                    tracker.add_issue(i, 'delimiter_issues', 'field_content', field, new_field)
                else:
                    new_record.append(field)
        else:
            new_record = record
            
        problematic_records.append(new_record)
    
    return problematic_records

def generate_healthcare_data(num_records=50000):
    """Generate both clean and messy healthcare survey data"""
    
    print(f"Generating {num_records} healthcare survey records...")
    
    # Generate base clean data
    clean_records = []
    messy_records = []
    tracker = DataQualityTracker()
    
    print("Step 1: Generating clean baseline data...")
    for i in range(num_records):
        if i % 10000 == 0:
            print(f"  Generated {i} clean records...")
        clean_records.append(generate_base_record(i))
    
    print("Step 2: Creating messy version with targeted corruption...")
    for i, record in enumerate(clean_records):
        if i % 10000 == 0:
            print(f"  Processed {i} records for corruption...")
        
        corrupted_record, was_corrupted = apply_targeted_corruption(record, i, tracker)
        messy_records.append(corrupted_record)
    
    print("Step 3: Applying CSV formatting issues...")
    messy_records = apply_csv_formatting_issues(messy_records, tracker)
    
    # Write clean file
    print("Step 4: Writing clean baseline file...")
    clean_filename = 'healthcare_survey_clean_baseline.csv'
    with open(clean_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(list(SCHEMA.keys()))  # Clean header
        writer.writerows(clean_records)
    
    # Write messy file
    print("Step 5: Writing messy test file...")
    messy_filename = 'healthcare_survey_messy_test.csv' 
    with open(messy_filename, 'w', newline='', encoding='utf-8') as csvfile:
        # Write potentially problematic header
        header_variation = random.choice([
            list(SCHEMA.keys()),  # Normal
            list(SCHEMA.keys()) + ['extra_col'],  # Extra column
            [col.upper() for col in SCHEMA.keys()],  # Case issues
        ])
        csvfile.write(','.join(header_variation) + '\n')
        
        # Write messy records
        for record in messy_records:
            try:
                row_str = ','.join([str(x) if x is not None else '' for x in record])
                csvfile.write(row_str + '\n')
            except:
                # Handle encoding issues gracefully
                safe_row = [str(x).encode('utf-8', errors='ignore').decode('utf-8') 
                           if x is not None else '' for x in record]
                csvfile.write(','.join(safe_row) + '\n')
        
        # Add a few completely malformed rows for extreme testing
        malformed_rows = [
            ',,,,malformed,row,with,issues',
            '"unclosed quote field, causing issues',
            'field1,field2,"field with\nmultiple\nlines",field4',
        ]
        for row in malformed_rows:
            csvfile.write(row + '\n')
            tracker.add_issue('malformed', 'csv_structure', 'entire_row', 'valid_csv', row)
    
    # Save detailed issue report
    print("Step 6: Generating data quality report...")
    tracker.save_report('data_quality_issues_report.json')
    
    # Print summary
    print("\n" + "="*60)
    print("🎉 DATA GENERATION COMPLETE!")
    print("="*60)
    print(f"📁 Clean baseline file: {clean_filename}")
    print(f"📁 Messy test file: {messy_filename}")
    print(f"📁 Issue report: data_quality_issues_report.json")
    print(f"\n📊 SUMMARY:")
    print(f"  • Total records: {num_records:,}")
    print(f"  • Records with issues: {len(tracker.issues):,} ({len(tracker.issues)/num_records*100:.1f}%)")
    print(f"  • Clean records: {num_records - len(tracker.issues):,} ({(num_records - len(tracker.issues))/num_records*100:.1f}%)")
    
    print(f"\n🔍 ISSUE BREAKDOWN:")
    for issue_type, count in sorted(tracker.issue_counts.items()):
        print(f"  • {issue_type}: {count}")
    
    print(f"\n✅ Use the clean file as your baseline for comparison")
    print(f"✅ Use the messy file to test your ETL validation")
    print(f"✅ Check the JSON report for detailed issue tracking")
    
    return clean_filename, messy_filename

if __name__ == "__main__":
    # Install required packages first:
    # pip install faker numpy pandas
    
    try:
        clean_file, messy_file = generate_healthcare_data(50000)
        print(f"\n🚀 Ready for Databricks ETL testing!")
        
    except ImportError as e:
        print(f"Missing required package: {e}")
        print("Please install required packages:")
        print("pip install faker numpy pandas")
    except Exception as e:
        print(f"Error generating data: {e}")