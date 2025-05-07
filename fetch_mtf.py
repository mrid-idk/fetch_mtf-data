import datetime
from dateutil.relativedelta import relativedelta

def generate_nse_urls(years=10):
    """
    Generate NSE margin trading data URLs for the specified number of years for ALL days.
    
    Args:
        years (int): Number of years to look back from today
        
    Returns:
        list: List of all URLs
    """
    # Start from today
    today = datetime.datetime.now()
    
    # Calculate the start date (years ago)
    start_date = today - relativedelta(years=years)
    
    # Initialize empty list to store URLs
    urls = []
    
    # Iterate through each day from start_date to today
    current_date = start_date
    while current_date <= today:
        # Format date as DDMMYY
        date_str = current_date.strftime("%d%m%y")
        
        # Construct URL
        url = f"https://nsearchives.nseindia.com/content/equities/mrg_trading_{date_str}.zip"
        
        # Add URL to list
        urls.append(url)
        
        # Move to next day
        current_date += datetime.timedelta(days=1)
    
    return urls

def save_urls_to_file(urls, filename="nse_urls_all_days.txt"):
    """
    Save the generated URLs to a text file
    
    Args:
        urls (list): List of URLs
        filename (str): Name of output file
    """
    with open(filename, 'w') as f:
        for url in urls:
            f.write(url + '\n')
    print(f"Saved {len(urls)} URLs to {filename}")

def main():
    # Generate URLs for last 10 years (all days)
    urls = generate_nse_urls(years=10)
    
    # Print the total number of URLs generated
    print(f"Generated {len(urls)} URLs for all days in the last 10 years")
    
    # Print the first and last 3 URLs as examples
    print("\nFirst 3 URLs:")
    for url in urls[:3]:
        print(url)
    
    print("\nLast 3 URLs:")
    for url in urls[-3:]:
        print(url)
    
    # Save URLs to file
    save_urls_to_file(urls)
    
    return urls

if __name__ == "__main__":
    main()
