# Healthcare Survey Data Generator

A Python script that generates realistic healthcare survey data with controlled data quality issues for testing ETL validation and data cleaning processes in Databricks.

## 🎯 Purpose

This tool generates two versions of healthcare survey data:
- **Clean baseline data** - Perfect reference data for comparison
- **Messy test data** - Data with realistic quality issues for ETL testing

## 📋 Prerequisites

### Required Python Packages
```bash
pip install faker numpy pandas
```

### Python Version
- Python 3.6 or higher

## 🚀 Quick Start

1. **Install dependencies:**
   ```bash
   pip install faker numpy pandas
   ```

2. **Run the generator:**
   ```bash
   python healthcare_data_generator.py
   ```

3. **Output files created:**
   - `healthcare_survey_clean_baseline.csv` - Clean reference data
   - `healthcare_survey_messy_test.csv` - Data with quality issues
   - `data_quality_issues_report.json` - Detailed issue tracking

## 📊 Data Schema

The generated data follows this healthcare survey schema:

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| user_id | Integer | Patient identifier |
| room_id | Integer | Room/location identifier |
| survey_id | Integer | Survey identifier |
| survey_scale_result_id | Integer | Unique result identifier |
| survey_created_at | DateTime | Survey creation timestamp |
| survey_completed_at | DateTime | Survey completion timestamp |
| survey_name | String | Survey instrument name (PHQ-9, GAD-7, etc.) |
| scale_name | String | Scale abbreviation |
| scale_result_category | String | Result category (Minimal, Mild, Moderate, etc.) |
| scale_result | Integer | Numeric scale result (0-27) |
| baseline_scale_result | Integer | Baseline comparison score |
| improvement_from_initial | Float | Change from initial assessment |
| improvement_from_previous | Float | Change from previous assessment |
| is_latest | Integer | Latest record flag (0/1) |
| initial_scale_result_value | Integer | Initial assessment value |
| initial_scale_result_category | String | Initial assessment category |
| improvement_from_initial_value | Float | Numeric improvement value |
| is_improvable | Integer | Improvable status flag (0/1) |
| is_sig_improvable_phq_gad | Float | Significance flag (0.0-1.0) |
| is_sig_improved_phq_gad | Float | Improvement significance (0.0-1.0) |
| initial_survey_completed_at | DateTime | Initial survey completion |
| file_date | DateTime | File processing date |

## 🔧 Configuration

### Default Settings
- **Total Records:** 50,000
- **Corruption Rate:** ~15% of records have issues
- **CSV Issues Rate:** ~5% of records have formatting problems
- **Issues per Record:** 1-3 issues maximum per corrupted record

### Customizing Generation
Edit these variables in the script:

```python
# Change record count
num_records = 50000

# Adjust corruption probability (0.15 = 15%)
corruption_probability = 0.15

# Modify CSV issue rate in apply_csv_formatting_issues()
if random.random() > 0.05:  # 5% get CSV issues
```

## 🐛 Data Quality Issues Included

### Field-Level Issues (15% of records)
- **Missing Values:** NULL, empty strings, 'N/A', whitespace-only
- **Wrong Data Types:** Floats in integer fields, strings in numeric fields
- **Date Format Issues:** Non-YYYY-MM-DD formats (MM/DD/YYYY, DD/MM/YYYY, etc.)
- **Negative Values:** Inappropriate negative ages, scores, measurements
- **Whitespace Problems:** Leading/trailing spaces, tabs, newlines
- **Special Characters:** Random symbols in unexpected places
- **Null Strings:** 'null', 'NULL', 'None', 'undefined' as text
- **Encoding Issues:** Mixed character encodings

### CSV Structure Issues (5% of records)
- **Extra Columns:** Additional fields beyond schema
- **Missing Columns:** Fewer fields than expected
- **Quote Problems:** Mixed quote styles, unclosed quotes, embedded quotes
- **Line Breaks:** Newlines within quoted fields
- **Delimiter Issues:** Mixed separators (semicolons, pipes)
- **Malformed Rows:** Completely broken CSV structure

## 📁 Output Files

### 1. Clean Baseline (`healthcare_survey_clean_baseline.csv`)
- 50,000 perfectly formatted records
- All dates in YYYY-MM-DD format
- Proper data types and no missing values
- Standard CSV structure
- **Use this as your expected output for ETL testing**

### 2. Messy Test Data (`healthcare_survey_messy_test.csv`)
- ~42,500 clean records (85%)
- ~7,500 records with data quality issues (15%)
- ~2,500 records with CSV formatting problems (5%)
- **Use this as input for your ETL validation**

### 3. Issue Report (`data_quality_issues_report.json`)
Detailed tracking of all issues introduced:

```json
{
  "summary": {
    "missing_value": 1250,
    "wrong_type": 890,
    "date_format": 445,
    "negative_value": 234,
    "whitespace": 567,
    "quote_issues": 123,
    "extra_columns": 67
  },
  "total_records_with_issues": 7891,
  "detailed_issues": {
    "record_123": [
      {
        "issue_type": "missing_value",
        "field": "survey_name",
        "original": "PHQ-9 Depression Scale",
        "corrupted": "NULL"
      }
    ]
  }
}
```

## 🧪 Testing Your ETL Pipeline

### Recommended Testing Workflow

1. **Baseline Validation:**
   ```python
   # Load clean data - this should process without issues
   clean_df = spark.read.csv("healthcare_survey_clean_baseline.csv", header=True)
   ```

2. **ETL Testing:**
   ```python
   # Load messy data - test your validation rules
   messy_df = spark.read.csv("healthcare_survey_messy_test.csv", header=True)
   cleaned_df = your_etl_function(messy_df)
   ```

3. **Validation Checking:**
   ```python
   # Compare results with issue report
   import json
   with open('data_quality_issues_report.json') as f:
       issues = json.load(f)
   
   # Verify your ETL fixed the specific issues
   for record_id, record_issues in issues['detailed_issues'].items():
       # Check if your ETL properly handled each issue
   ```

### Common ETL Validations to Test

- **Date Format Standardization:** All dates → YYYY-MM-DD
- **Data Type Enforcement:** Proper integer/float/string types
- **Missing Value Handling:** NULL strategy (impute, flag, reject)
- **Range Validation:** Age > 0, scores in valid ranges
- **Whitespace Cleaning:** Trim leading/trailing spaces
- **Quote Handling:** Proper CSV parsing with embedded quotes
- **Column Count Validation:** Consistent field numbers
- **Encoding Normalization:** UTF-8 standardization

## 🔍 Monitoring and Debugging

### Check Generation Success
The script outputs a summary report:
```
📊 SUMMARY:
  • Total records: 50,000
  • Records with issues: 7,891 (15.8%)
  • Clean records: 42,109 (84.2%)

🔍 ISSUE BREAKDOWN:
  • missing_value: 1,250
  • wrong_type: 890
  • date_format: 445
  • negative_value: 234
```

### Troubleshooting Common Issues

**ImportError: No module named 'faker'**
```bash
pip install faker numpy pandas
```

**UnicodeEncodeError during generation**
- The script handles encoding issues automatically
- Output files use UTF-8 encoding

**Memory issues with large datasets**
- Reduce `num_records` for testing
- Process in batches for production use

## 🎛️ Advanced Usage

### Custom Survey Types
Modify the survey data lists:

```python
SURVEY_NAMES = [
    'Your Custom Survey',
    'Another Assessment Tool'
]

SCALE_CATEGORIES = [
    'Your Custom Categories'
]
```

### Specific Issue Testing
Target specific data quality issues:

```python
# Test only date format issues
issue_type = 'date_format'  # Force specific issue type

# Test only CSV structure issues
csv_issue_rate = 0.2  # Increase CSV problems to 20%
```

### Integration with Databricks
```python
# In Databricks notebook
%sh pip install faker numpy pandas

# Upload the generator script to DBFS
dbutils.fs.cp("file:/path/to/script.py", "dbfs:/tmp/generator.py")

# Run generation
%run /tmp/generator.py

# Load generated data
clean_df = spark.read.csv("dbfs:/tmp/healthcare_survey_clean_baseline.csv", 
                         header=True, inferSchema=True)
```

## 📝 License

This tool is provided as-is for testing and development purposes. The generated healthcare data is entirely synthetic and contains no real patient information.

## 🤝 Contributing

To extend this generator:
1. Add new issue types in `apply_targeted_corruption()`
2. Modify healthcare-specific data in the constants
3. Extend the schema by updating the `SCHEMA` dictionary
4. Add new CSV formatting issues in `apply_csv_formatting_issues()`

## 📞 Support

For issues or questions:
1. Check the JSON issue report for detailed problem tracking
2. Verify all dependencies are installed correctly
3. Ensure sufficient disk space for large datasets
4. Review the console output for generation progress and errors