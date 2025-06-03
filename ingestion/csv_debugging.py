#!/usr/bin/env python3
"""
CSV Malformed Records Debugger - Pure Python Version
Comprehensive script to identify and analyze malformed records in CSV files
No Spark dependency required - uses pandas and standard library
"""

import pandas as pd
import csv
import argparse
import sys
import json
from typing import Optional, List, Dict, Any, Union
from collections import Counter, defaultdict
import chardet
import os

class CSVDebugger:
    def __init__(self):
        self.encoding = None
        self.delimiter = None
        self.quote_char = '"'
        
    def detect_encoding(self, file_path: str, sample_size: int = 10000) -> str:
        """Detect file encoding"""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(sample_size)
                result = chardet.detect(raw_data)
                encoding = result['encoding']
                confidence = result['confidence']
                print(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
                return encoding
        except Exception as e:
            print(f"Encoding detection failed: {e}. Using utf-8.")
            return 'utf-8'
    
    def analyze_csv_structure(self, file_path: str, sample_size: int = 100) -> Dict[str, Any]:
        """Analyze the basic structure of the CSV file"""
        print("=" * 80)
        print("CSV STRUCTURE ANALYSIS")
        print("=" * 80)
        
        # Detect encoding
        self.encoding = self.detect_encoding(file_path)
        
        try:
            with open(file_path, 'r', encoding=self.encoding, errors='replace') as f:
                lines = []
                total_lines = 0
                
                # Count total lines and get sample
                for i, line in enumerate(f):
                    total_lines += 1
                    if i < sample_size:
                        lines.append(line.rstrip('\n\r'))
                    
                    # For very large files, don't load everything into memory
                    if i > 1000000:  # Stop counting after 1M lines
                        total_lines = f">{total_lines}"
                        break
        
        except Exception as e:
            print(f"Error reading file: {e}")
            return {}
        
        print(f"Total lines in file: {total_lines}")
        print(f"Analyzing first {len(lines)} lines...")
        
        # Analyze potential delimiters
        delimiter_analysis = {',': 0, ';': 0, '\t': 0, '|': 0}
        field_counts = Counter()
        quote_analysis = {'"': 0, "'": 0}
        
        for i, line in enumerate(lines):
            # Count potential delimiters
            for delim in delimiter_analysis:
                delimiter_analysis[delim] += line.count(delim)
            
            # Count quotes
            for quote in quote_analysis:
                quote_analysis[quote] += line.count(quote)
            
            if i < 10:  # Show first 10 lines
                print(f"Line {i+1}: {len(line)} chars | {line[:100]}{'...' if len(line) > 100 else ''}")
        
        # Determine most likely delimiter
        likely_delimiter = max(delimiter_analysis, key=delimiter_analysis.get)
        self.delimiter = likely_delimiter
        
        # Analyze field counts with likely delimiter
        for line in lines:
            field_count = line.count(likely_delimiter) + 1
            field_counts[field_count] += 1
        
        print(f"\nDelimiter analysis: {delimiter_analysis}")
        print(f"Most likely delimiter: '{likely_delimiter}'")
        print(f"Quote analysis: {quote_analysis}")
        print(f"Field count distribution: {dict(field_counts.most_common(10))}")
        
        # Detect header
        header_line = lines[0] if lines else None
        if header_line:
            print(f"\nPossible header: {header_line}")
            headers = self._split_csv_line(header_line, likely_delimiter)
            print(f"Detected {len(headers)} columns: {headers}")
        
        return {
            'total_lines': total_lines,
            'field_counts': dict(field_counts),
            'likely_delimiter': likely_delimiter,
            'delimiter_analysis': delimiter_analysis,
            'header_line': header_line,
            'encoding': self.encoding,
            'sample_lines': lines[:20]  # Keep first 20 lines for further analysis
        }
    
    def _split_csv_line(self, line: str, delimiter: str) -> List[str]:
        """Split CSV line handling quotes properly"""
        try:
            reader = csv.reader([line], delimiter=delimiter, quotechar='"')
            return next(reader)
        except:
            # Fallback to simple split if CSV parsing fails
            return line.split(delimiter)
    
    def detect_malformed_records(self, file_path: str, delimiter: str = None, 
                               has_header: bool = True, expected_columns: Optional[List[str]] = None) -> Dict[str, Any]:
        """Detect malformed records by parsing the entire file"""
        print("\n" + "=" * 80)
        print("MALFORMED RECORDS DETECTION")
        print("=" * 80)
        
        if delimiter is None:
            delimiter = self.delimiter or ','
        
        malformed_records = []
        valid_records = 0
        total_records = 0
        field_count_issues = []
        quote_issues = []
        encoding_issues = []
        empty_lines = []
        
        expected_field_count = None
        headers = None
        
        try:
            with open(file_path, 'r', encoding=self.encoding, errors='replace') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.rstrip('\n\r')
                    total_records += 1
                    
                    # Skip empty lines
                    if not line.strip():
                        empty_lines.append(line_num)
                        continue
                    
                    # Get headers from first line
                    if line_num == 1 and has_header:
                        headers = self._split_csv_line(line, delimiter)
                        expected_field_count = len(headers)
                        print(f"Expected {expected_field_count} fields based on header")
                        continue
                    
                    # If no header, use first data line to determine field count
                    if expected_field_count is None:
                        fields = self._split_csv_line(line, delimiter)
                        expected_field_count = len(fields)
                        print(f"Expected {expected_field_count} fields based on first line")
                    
                    # Parse the line
                    try:
                        fields = self._split_csv_line(line, delimiter)
                        field_count = len(fields)
                        
                        # Check field count
                        if field_count != expected_field_count:
                            field_count_issues.append({
                                'line_num': line_num,
                                'expected': expected_field_count,
                                'actual': field_count,
                                'content': line[:200] + ('...' if len(line) > 200 else '')
                            })
                            malformed_records.append(line_num)
                        else:
                            valid_records += 1
                            
                    except Exception as e:
                        malformed_records.append(line_num)
                        if 'quote' in str(e).lower():
                            quote_issues.append({
                                'line_num': line_num,
                                'error': str(e),
                                'content': line[:200] + ('...' if len(line) > 200 else '')
                            })
                        else:
                            encoding_issues.append({
                                'line_num': line_num,
                                'error': str(e),
                                'content': line[:200] + ('...' if len(line) > 200 else '')
                            })
                    
                    # Stop after analyzing reasonable number of records for performance
                    if total_records > 100000:
                        print(f"Analyzed first 100,000 records...")
                        break
        
        except Exception as e:
            print(f"Error reading file: {e}")
            return {}
        
        # Print results
        print(f"\nAnalysis Results:")
        print(f"Total records analyzed: {total_records}")
        print(f"Valid records: {valid_records}")
        print(f"Malformed records: {len(malformed_records)}")
        print(f"Empty lines: {len(empty_lines)}")
        
        if field_count_issues:
            print(f"\nField Count Issues ({len(field_count_issues)} records):")
            print("-" * 60)
            for issue in field_count_issues[:10]:  # Show first 10
                print(f"Line {issue['line_num']}: Expected {issue['expected']}, got {issue['actual']}")
                print(f"  Content: {issue['content']}")
        
        if quote_issues:
            print(f"\nQuote Issues ({len(quote_issues)} records):")
            print("-" * 60)
            for issue in quote_issues[:5]:  # Show first 5
                print(f"Line {issue['line_num']}: {issue['error']}")
                print(f"  Content: {issue['content']}")
        
        if encoding_issues:
            print(f"\nEncoding/Parsing Issues ({len(encoding_issues)} records):")
            print("-" * 60)
            for issue in encoding_issues[:5]:  # Show first 5
                print(f"Line {issue['line_num']}: {issue['error']}")
                print(f"  Content: {issue['content']}")
        
        return {
            'total_records': total_records,
            'valid_records': valid_records,
            'malformed_records': malformed_records,
            'field_count_issues': field_count_issues,
            'quote_issues': quote_issues,
            'encoding_issues': encoding_issues,
            'empty_lines': empty_lines,
            'headers': headers
        }
    
    def pandas_validation(self, file_path: str, delimiter: str = None, 
                         expected_schema: Optional[Dict[str, str]] = None) -> None:
        """Use pandas to validate and identify issues"""
        print("\n" + "=" * 80)
        print("PANDAS VALIDATION")
        print("=" * 80)
        
        if delimiter is None:
            delimiter = self.delimiter or ','
        
        try:
            # Try to read with pandas
            df = pd.read_csv(file_path, delimiter=delimiter, encoding=self.encoding, 
                           on_bad_lines='warn', engine='python')
            
            print(f"Pandas successfully loaded {len(df)} rows and {len(df.columns)} columns")
            print(f"\nColumn names: {list(df.columns)}")
            print(f"\nData types:")
            print(df.dtypes)
            
            # Check for missing values
            missing_values = df.isnull().sum()
            if missing_values.any():
                print(f"\nMissing values per column:")
                for col, count in missing_values.items():
                    if count > 0:
                        percentage = (count / len(df)) * 100
                        print(f"  {col}: {count} ({percentage:.2f}%)")
            
            # Show sample data
            print(f"\nFirst 5 rows:")
            print(df.head().to_string())
            
            # Schema validation if provided
            if expected_schema:
                print(f"\nSchema Validation:")
                for col, expected_type in expected_schema.items():
                    if col in df.columns:
                        actual_type = str(df[col].dtype)
                        if expected_type.lower() not in actual_type.lower():
                            print(f"  {col}: Expected {expected_type}, got {actual_type}")
                    else:
                        print(f"  Missing column: {col}")
            
        except Exception as e:
            print(f"Pandas failed to read file: {e}")
            
            # Try with error_bad_lines=False for older pandas versions
            try:
                df = pd.read_csv(file_path, delimiter=delimiter, encoding=self.encoding, 
                               error_bad_lines=False, warn_bad_lines=True, engine='python')
                print(f"Pandas loaded {len(df)} rows with some lines skipped")
            except Exception as e2:
                print(f"Alternative pandas approach also failed: {e2}")
    
    def generate_summary_report(self, file_path: str, structure_info: Dict, 
                              malformed_info: Dict) -> None:
        """Generate a comprehensive summary report"""
        print("\n" + "=" * 80)
        print("SUMMARY REPORT")
        print("=" * 80)
        
        print(f"File: {file_path}")
        print(f"File size: {os.path.getsize(file_path):,} bytes")
        print(f"Encoding: {structure_info.get('encoding', 'unknown')}")
        print(f"Delimiter: '{structure_info.get('likely_delimiter', 'unknown')}'")
        
        if malformed_info:
            total_records = malformed_info.get('total_records', 0)
            valid_records = malformed_info.get('valid_records', 0)
            malformed_count = len(malformed_info.get('malformed_records', []))
            
            print(f"\nRecord Analysis:")
            print(f"  Total records: {total_records:,}")
            print(f"  Valid records: {valid_records:,}")
            print(f"  Malformed records: {malformed_count:,}")
            
            if total_records > 0:
                success_rate = (valid_records / total_records) * 100
                print(f"  Success rate: {success_rate:.2f}%")
        
        # Issue breakdown
        if malformed_info:
            field_issues = len(malformed_info.get('field_count_issues', []))
            quote_issues = len(malformed_info.get('quote_issues', []))
            encoding_issues = len(malformed_info.get('encoding_issues', []))
            empty_lines = len(malformed_info.get('empty_lines', []))
            
            print(f"\nIssue Breakdown:")
            if field_issues: print(f"  Field count mismatches: {field_issues:,}")
            if quote_issues: print(f"  Quote/parsing issues: {quote_issues:,}")
            if encoding_issues: print(f"  Encoding issues: {encoding_issues:,}")
            if empty_lines: print(f"  Empty lines: {empty_lines:,}")
        
        # Recommendations
        if malformed_info and malformed_info.get('malformed_records'):
            print(f"\nRecommendations:")
            
            field_issues = malformed_info.get('field_count_issues', [])
            if field_issues:
                print("- Fix inconsistent field counts in malformed records")
                print("- Consider using 'on_bad_lines=\"skip\"' in pandas to ignore bad lines")
            
            quote_issues = malformed_info.get('quote_issues', [])
            if quote_issues:
                print("- Fix unescaped quotes in the data")
                print("- Consider using different quote character or escaping")
            
            encoding_issues = malformed_info.get('encoding_issues', [])
            if encoding_issues:
                print("- Check file encoding and special characters")
                print("- Consider cleaning data or using 'errors=\"replace\"' when reading")
            
            print("- Review the specific malformed records shown above")
            print("- Consider data preprocessing before ingestion")

def parse_schema_string(schema_str: str) -> Dict[str, str]:
    """Parse schema string into dictionary"""
    schema = {}
    for field_def in schema_str.split(','):
        parts = field_def.strip().split(':')
        if len(parts) == 2:
            col_name, col_type = parts
            schema[col_name.strip()] = col_type.strip()
    return schema

def main():
    parser = argparse.ArgumentParser(description='Debug CSV files for malformed records (Pure Python)')
    parser.add_argument('file_path', help='Path to CSV file')
    parser.add_argument('--delimiter', help='CSV delimiter (auto-detected if not specified)')
    parser.add_argument('--no-header', action='store_true', help='CSV has no header row')
    parser.add_argument('--schema', help='Expected schema as string (e.g., "col1:string,col2:int")')
    parser.add_argument('--sample-size', type=int, default=100, help='Sample size for initial analysis')
    parser.add_argument('--encoding', help='File encoding (auto-detected if not specified)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.file_path):
        print(f"Error: File '{args.file_path}' not found.")
        sys.exit(1)
    
    try:
        debugger = CSVDebugger()
        
        # Override encoding if specified
        if args.encoding:
            debugger.encoding = args.encoding
        
        # Step 1: Analyze CSV structure
        print(f"Analyzing CSV file: {args.file_path}")
        structure_info = debugger.analyze_csv_structure(args.file_path, args.sample_size)
        
        if not structure_info:
            print("Failed to analyze file structure.")
            sys.exit(1)
        
        # Step 2: Parse schema if provided
        expected_schema = None
        if args.schema:
            try:
                expected_schema = parse_schema_string(args.schema)
                print(f"\nUsing provided schema: {expected_schema}")
            except Exception as e:
                print(f"Error parsing schema: {e}")
        
        # Step 3: Detect malformed records
        delimiter = args.delimiter or structure_info.get('likely_delimiter')
        malformed_info = debugger.detect_malformed_records(
            args.file_path, 
            delimiter=delimiter,
            has_header=not args.no_header
        )
        
        # Step 4: Pandas validation
        debugger.pandas_validation(args.file_path, delimiter, expected_schema)
        
        # Step 5: Generate summary report
        debugger.generate_summary_report(args.file_path, structure_info, malformed_info)
        
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

# Example usage:
# python csv_debugger.py /path/to/file.csv
# python csv_debugger.py /path/to/file.csv --delimiter "|" --schema "id:int,name:string"
# python csv_debugger.py /path/to/file.csv --no-header --encoding utf-8