#!/usr/bin/env python3
"""
NSE Zip File Extractor

This script extracts all zip files in a specified directory,
showing progress and handling errors.
"""

import os
import zipfile
import argparse
from pathlib import Path
from tqdm import tqdm
import csv
import pandas as pd
import datetime
import re

def extract_all_zips(input_dir, output_dir, organize_by_year_month=True):
    """
    Extract all zip files from the input directory to the output directory.
    
    Args:
        input_dir (str): Directory containing zip files
        output_dir (str): Directory to extract files to
        organize_by_year_month (bool): If True, organize extracted files by year/month subdirectories
    
    Returns:
        tuple: (success_count, fail_count, total_files_extracted)
    """
    # Create output directory if it doesn't exist
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Find all zip files
    input_path = Path(input_dir)
    zip_files = list(input_path.glob('**/*.zip'))
    
    if not zip_files:
        print(f"No zip files found in {input_dir}")
        return 0, 0, 0
    
    print(f"Found {len(zip_files)} zip files to extract")
    
    # Initialize counters
    success_count = 0
    fail_count = 0
    total_files_extracted = 0
    
    # Process each zip file with progress bar
    for zip_file in tqdm(zip_files, desc="Extracting", unit="zip"):
        try:
            # Parse date from filename (format: mrg_trading_DDMMYY.zip)
            date_match = re.search(r'mrg_trading_(\d{2})(\d{2})(\d{2})\.zip', zip_file.name)
            
            if date_match and organize_by_year_month:
                day, month, year = date_match.groups()
                # Assume 20xx for year
                full_year = f"20{year}"
                # Create year/month subdirectory
                extract_dir = output_path / full_year / month
                extract_dir.mkdir(parents=True, exist_ok=True)
            else:
                # Use output directory directly
                extract_dir = output_path
            
            # Extract the zip file
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                # Get list of files in the zip
                file_list = zip_ref.namelist()
                
                # Extract all files
                zip_ref.extractall(extract_dir)
                
                # Update counters
                total_files_extracted += len(file_list)
                success_count += 1
                
                # Print details about extracted files
                tqdm.write(f"Extracted {len(file_list)} file(s) from {zip_file.name} to {extract_dir}")
                
        except zipfile.BadZipFile:
            tqdm.write(f"Error: {zip_file.name} is not a valid zip file")
            fail_count += 1
        except Exception as e:
            tqdm.write(f"Error extracting {zip_file.name}: {str(e)}")
            fail_count += 1
    
    return success_count, fail_count, total_files_extracted

def main():
    """Main function to parse arguments and coordinate extraction"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract NSE margin trading data zip files')
    parser.add_argument('--input-dir', '-i', type=str, default='data',
                        help='Directory containing zip files to extract')
    parser.add_argument('--output-dir', '-o', type=str, default='data/extracted',
                        help='Directory to extract files to')
    parser.add_argument('--flat', '-f', action='store_true',
                       help='Extract all files to a flat directory instead of organizing by year/month')
    args = parser.parse_args()
    
    # Extract all zip files
    success_count, fail_count, total_files_extracted = extract_all_zips(
        args.input_dir, 
        args.output_dir,
        organize_by_year_month=not args.flat
    )
    
    # Print summary
    print("\nExtraction Summary:")
    print(f"  Total zip files: {success_count + fail_count}")
    print(f"  Successfully extracted: {success_count}")
    print(f"  Failed to extract: {fail_count}")
    print(f"  Total files extracted: {total_files_extracted}")
    print(f"All files extracted to: {Path(args.output_dir).absolute()}")
    
    # Organize data (optional post-processing)
    if success_count > 0:
        print("\nYou can now analyze the extracted data using tools like pandas:")
        print("  python -c \"import pandas as pd; df = pd.read_csv('path/to/extracted/csv'); print(df.head())\"")

if __name__ == "__main__":
    main()
