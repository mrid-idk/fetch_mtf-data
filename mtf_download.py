#!/usr/bin/env python3
"""
NSE Archive Downloader Script

This script downloads NSE margin trading data from a list of URLs.
It first gets cookies from NSE website to authenticate requests
and shows progress while downloading files.
"""

import os
import time
import argparse
import requests
from pathlib import Path
from tqdm import tqdm
from urllib.parse import urlparse

# Custom User-Agent to mimic Chrome browser
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.54 Safari/537.36'

# Default headers for all requests
HEADERS = {
    'authority': 'www.nseindia.com',
    'accept': '*/*',
    'accept-language': 'en',
    'dnt': '1',
    'user-agent': USER_AGENT,
    'x-requested-with': 'XMLHttpRequest'
}

def get_cookies():
    """
    Visit NSE website to get cookies necessary for authenticated requests.
    
    Returns:
        requests.cookies.RequestsCookieJar: The cookie jar with authenticated cookies
    """
    print("Getting cookies from NSE website...")
    
    # URL to visit for cookies
    cookie_url = 'https://www.nseindia.com/products-services/equity-derivatives-individual-securities'
    
    # Create a session to maintain cookies
    session = requests.Session()
    
    try:
        # Visit the NSE website to get cookies
        response = session.get(cookie_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        # Return the cookies
        print(f"Successfully got cookies. Collected {len(session.cookies)} cookies.")
        return session.cookies
    
    except requests.exceptions.RequestException as e:
        print(f"Error getting cookies: {e}")
        return None

def download_file(url, cookies, output_dir, delay=1, max_retries=3):
    """
    Download a file from the given URL using the provided cookies.
    
    Args:
        url (str): The URL of the file to download
        cookies (requests.cookies.RequestsCookieJar): Cookies for authentication
        output_dir (str): Directory to save the downloaded file
        delay (int): Number of seconds to wait after download
        max_retries (int): Maximum number of retry attempts
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    # Parse filename from URL
    parsed_url = urlparse(url)
    filename = os.path.basename(parsed_url.path)
    output_path = os.path.join(output_dir, filename)
    
    # Skip if file already exists
    if os.path.exists(output_path):
        return (True, f"Skipped (already exists): {filename}")
    
    # Initialize retry counter
    retries = 0
    
    while retries < max_retries:
        try:
            # Download the file
            response = requests.get(
                url, 
                headers=HEADERS, 
                cookies=cookies, 
                stream=True,
                timeout=30
            )
            
            # Check if request was successful
            response.raise_for_status()
            
            # Check if we got an empty file or error page (suspicious small size)
            if int(response.headers.get('content-length', 0)) < 100:
                # If size is small, check if it contains error text
                content_sample = response.content[:100].decode('utf-8', errors='ignore')
                if 'error' in content_sample.lower() or 'not found' in content_sample.lower():
                    return (False, f"Error page received for: {filename}")
            
            # Save the file
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            # Wait to avoid overwhelming the server
            time.sleep(delay)
            
            return (True, f"Downloaded: {filename}")
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # File not found, no need to retry
                return (False, f"File not found (404): {filename}")
            
            # For other HTTP errors, retry
            retries += 1
            if retries < max_retries:
                print(f"HTTP error ({e.response.status_code}), retrying ({retries}/{max_retries})...")
                time.sleep(delay * 2)  # Longer delay on errors
            else:
                return (False, f"Failed after {max_retries} retries: {filename}")
                
        except requests.exceptions.RequestException as e:
            # Network error, retry
            retries += 1
            if retries < max_retries:
                print(f"Network error, retrying ({retries}/{max_retries}): {e}")
                time.sleep(delay * 3)  # Even longer delay on network errors
            else:
                return (False, f"Network error after {max_retries} retries: {filename}")

def main():
    """Main function to parse arguments and coordinate downloading"""
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Download NSE margin trading data files')
    parser.add_argument('--input', '-i', type=str, default='nse_urls.txt', 
                        help='Input file containing URLs to download (one per line)')
    parser.add_argument('--output-dir', '-o', type=str, default='data',
                        help='Directory to save downloaded files')
    parser.add_argument('--delay', '-d', type=float, default=1.0,
                        help='Delay in seconds between downloads (default: 1.0)')
    parser.add_argument('--max-files', '-m', type=int, default=0,
                        help='Maximum number of files to download (0 for unlimited)')
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Get authentication cookies
    cookies = get_cookies()
    if not cookies:
        print("Failed to get authentication cookies. Exiting.")
        return
    
    # Read URLs from file
    try:
        with open(args.input, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading URL file: {e}")
        return
    
    print(f"Found {len(urls)} URLs to process")
    
    # Limit number of downloads if specified
    if args.max_files > 0 and args.max_files < len(urls):
        print(f"Limiting to {args.max_files} downloads as specified")
        urls = urls[:args.max_files]
    
    # Download files with progress bar
    success_count = 0
    fail_count = 0
    skipped_count = 0
    
    with tqdm(total=len(urls), desc="Downloading", unit="file") as pbar:
        for url in urls:
            success, message = download_file(url, cookies, output_dir, args.delay)
            
            # Update stats
            if success:
                if "Skipped" in message:
                    skipped_count += 1
                else:
                    success_count += 1
            else:
                fail_count += 1
            
            # Update progress bar with message
            pbar.set_postfix_str(message)
            pbar.update(1)
    
    # Print summary
    print("\nDownload Summary:")
    print(f"  Total URLs: {len(urls)}")
    print(f"  Successfully downloaded: {success_count}")
    print(f"  Skipped (already exist): {skipped_count}")
    print(f"  Failed: {fail_count}")
    print(f"All files saved to: {output_dir.absolute()}")

if __name__ == "__main__":
    main()
