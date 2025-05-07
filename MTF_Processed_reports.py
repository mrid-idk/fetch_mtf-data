import os
import time
import logging
import shutil
import tempfile
import re
from datetime import datetime
from pathlib import Path

# Check for required packages and install if missing
try:
    import pandas as pd
except ImportError:
    print("Pandas is not installed. Installing now...")
    import subprocess
    subprocess.check_call(["pip", "install", "pandas"])
    import pandas as pd

try:
    import schedule
except ImportError:
    print("Schedule is not installed. Installing now...")
    import subprocess
    subprocess.check_call(["pip", "install", "schedule"])
    import schedule

import zipfile
import glob

# Set up logging
log_dir = os.path.expanduser("~/Documents/MTF_Processing_Logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"mtf_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Configuration
download_path = os.path.expanduser("~/Downloads")
output_folder = os.path.expanduser("~/Documents/MTF_Reports_Processed")
os.makedirs(output_folder, exist_ok=True)

# Create a file to track processed zip files
processed_files_log = os.path.join(log_dir, "processed_mtf_files.txt")
if not os.path.exists(processed_files_log):
    with open(processed_files_log, 'w') as f:
        f.write("")

def get_processed_files():
    """Read the list of already processed files"""
    if os.path.exists(processed_files_log):
        with open(processed_files_log, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()

def mark_as_processed(filepath):
    """Add a filepath to the processed files log"""
    with open(processed_files_log, 'a') as f:
        f.write(f"{filepath}\n")

def convert_to_crores(value):
    """Convert text representations of numbers from lakhs to crores"""
    if pd.isna(value) or not isinstance(value, str):
        return value
        
    # Try to extract number from the text using regex
    number_match = re.search(r'([-+]?\d*\.?\d+)', value)
    if number_match:
        number = float(number_match.group(1))
        # Convert from lakhs to crores
        crore_value = number / 100
        # Replace the original number with the crore value in the string
        return value.replace(number_match.group(1), str(crore_value))
    return value

def process_csv_file(csv_path, output_dir):
    """Process a single CSV file, converting lakhs to crores in columns C and D"""
    csv_filename = os.path.basename(csv_path)
    logging.info(f"Processing CSV file: {csv_filename}")
    
    try:
        # Read in chunks for large files
        chunks = pd.read_csv(csv_path, chunksize=10000)
        
        # Process each chunk and collect results
        processed_chunks = []
        for chunk_num, chunk in enumerate(chunks):
            logging.info(f"  Processing chunk {chunk_num+1}...")
            
            # Convert numeric values in column C and D if they exist
            for col in ['C', 'D']:
                if col in chunk.columns:
                    # Handle mixed content (text and numbers)
                    # For cells that are numeric, divide by 100
                    numeric_mask = pd.to_numeric(chunk[col], errors='coerce').notna()
                    chunk.loc[numeric_mask, col] = pd.to_numeric(chunk.loc[numeric_mask, col]) / 100
                    
                    # For cells with text that contain numbers, process them separately
                    text_mask = ~numeric_mask & ~chunk[col].isna()
                    chunk.loc[text_mask, col] = chunk.loc[text_mask, col].apply(convert_to_crores)
            
            processed_chunks.append(chunk)
        
        # Combine all processed chunks
        if processed_chunks:
            df_processed = pd.concat(processed_chunks, ignore_index=True)
            
            # Save the processed data
            output_filename = f"crores_{csv_filename}"
            output_path = os.path.join(output_dir, output_filename)
            df_processed.to_csv(output_path, index=False)
            logging.info(f"  Saved processed file to {output_path}")
            return True
        else:
            logging.warning(f"  No data processed for {csv_filename}")
            return False
                
    except Exception as e:
        logging.error(f"  Error processing {csv_filename}: {str(e)}")
        return False

def process_nested_zip_files():
    """Process the fetch_mtf.zip file which contains multiple zip files in a data folder"""
    main_zip_path = os.path.join(download_path, "fetch_mtf.zip")
    
    if not os.path.exists(main_zip_path):
        logging.error(f"Main zip file not found at {main_zip_path}")
        return False
    
    logging.info(f"Processing main zip file: {main_zip_path}")
    processed_files = get_processed_files()
    
    # Create a temporary directory to extract files
    with tempfile.TemporaryDirectory() as temp_dir:
        logging.info(f"Created temporary directory: {temp_dir}")
        
        try:
            # Extract the main zip file
            with zipfile.ZipFile(main_zip_path, 'r') as main_zip:
                # List all files to find the data folder and zip files
                all_files = main_zip.namelist()
                zip_files = [f for f in all_files if f.lower().endswith('.zip') and 'data/' in f.lower()]
                
                if not zip_files:
                    logging.error("No zip files found in the data folder")
                    return False
                
                logging.info(f"Found {len(zip_files)} zip files in the data folder")
                
                # Process each nested zip file
                for zip_file_path in zip_files:
                    relative_path = zip_file_path
                    
                    # Skip if already processed
                    if relative_path in processed_files:
                        logging.info(f"Skipping already processed: {relative_path}")
                        continue
                    
                    logging.info(f"Processing nested zip file: {relative_path}")
                    
                    # Extract the nested zip file to the temp directory
                    main_zip.extract(zip_file_path, temp_dir)
                    nested_zip_path = os.path.join(temp_dir, zip_file_path)
                    
                    # Create output directory for this specific zip
                    zip_name = os.path.basename(zip_file_path)
                    zip_output_dir = os.path.join(output_folder, os.path.splitext(zip_name)[0])
                    os.makedirs(zip_output_dir, exist_ok=True)
                    
                    # Process the nested zip file
                    try:
                        with zipfile.ZipFile(nested_zip_path, 'r') as nested_zip:
                            # Get list of CSV files in the nested zip
                            csv_files = [f for f in nested_zip.namelist() if f.lower().endswith('.csv')]
                            logging.info(f"Found {len(csv_files)} CSV files in {zip_name}")
                            
                            # Extract and process each CSV file
                            for csv_file in csv_files:
                                # Extract the CSV to the temp directory
                                csv_extract_path = os.path.join(temp_dir, f"temp_{os.path.basename(csv_file)}")
                                with nested_zip.open(csv_file) as source, open(csv_extract_path, 'wb') as target:
                                    shutil.copyfileobj(source, target)
                                
                                # Process the CSV file
                                process_csv_file(csv_extract_path, zip_output_dir)
                                
                                # Clean up the temporary CSV file
                                os.remove(csv_extract_path)
                        
                        # Mark this zip file as processed
                        mark_as_processed(relative_path)
                        logging.info(f"Completed processing nested zip: {relative_path}")
                        
                    except Exception as e:
                        logging.error(f"Error processing nested zip {zip_name}: {str(e)}")
        
        except Exception as e:
            logging.error(f"Error processing main zip file: {str(e)}")
            return False
    
    logging.info("Completed processing all nested zip files")
    return True

def check_for_updates():
    """Check if the main zip file has been updated and process it"""
    main_zip_path = os.path.join(download_path, "fetch_mtf.zip")
    
    if not os.path.exists(main_zip_path):
        logging.info("Main zip file not found. Waiting for it to appear.")
        return
    
    # Check if the zip file has been modified since last check
    last_modified = os.path.getmtime(main_zip_path)
    last_modified_date = datetime.fromtimestamp(last_modified)
    
    # Store the last processed time in a file
    last_processed_file = os.path.join(log_dir, "last_processed_time.txt")
    
    if os.path.exists(last_processed_file):
        with open(last_processed_file, 'r') as f:
            try:
                last_processed_time = float(f.read().strip())
                last_processed_date = datetime.fromtimestamp(last_processed_time)
            except:
                last_processed_time = 0
                last_processed_date = datetime.fromtimestamp(0)
    else:
        last_processed_time = 0
        last_processed_date = datetime.fromtimestamp(0)
    
    if last_modified > last_processed_time:
        logging.info(f"Detected new or updated main zip file (modified: {last_modified_date})")
        process_nested_zip_files()
        
        # Update the last processed time
        with open(last_processed_file, 'w') as f:
            f.write(str(last_modified))
    else:
        logging.info(f"No changes to main zip file since last processing ({last_processed_date})")

def main():
    """Main function to run the automated processing"""
    logging.info("Starting MTF disclosure reports processing automation")
    
    # Process existing files first
    check_for_updates()
    
    # Schedule regular checks
    schedule.every(6).hours.do(check_for_updates)
    
    logging.info("Automated processing set up. Will check for updates every 6 hours.")
    logging.info(f"Processed files will be saved to: {output_folder}")
    
    # Keep the script running
    try:
        while True:
            schedule.run_pending()
            time.sleep(1800)  # Check every 30 minutes if there are pending tasks
    except KeyboardInterrupt:
        logging.info("Processing automation stopped by user")

if __name__ == "__main__":
    # Print Python environment info
    import sys
    logging.info(f"Python version: {sys.version}")
    logging.info(f"Running script from: {os.getcwd()}")
    
    try:
        main()
    except Exception as e:
        logging.error(f"Fatal error in main program: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
