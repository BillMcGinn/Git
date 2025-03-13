import requests
import ads
import pandas as pd
import time

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None  # If tqdm isn't installed, we'll fall back to print statements.

def fetch_ads_dataframe_page_by_page(q, telescope_name='telescope_name', program_name='program_name',
                                       rows=200, retries=3, delay=5, max_pages=None):
    """
    Fetch ADS data one page at a time to help avoid connection timeouts, with progress tracking.
    
    Parameters:
        q (str): The ADS search query.
        telescope_name (str): Name to populate the 'Telescope' column.
        program_name (str): Name to populate the 'Program' column.
        rows (int): Number of results per page.
        retries (int): Number of times to retry a page if a connection error occurs.
        delay (int): Delay (in seconds) between retries.
        max_pages (int, optional): Maximum number of pages to fetch. If None, fetch until no more results.
        
    Returns:
        pd.DataFrame: A DataFrame with columns:
                      ['Bibcode', 'Title', 'ID', 
                       'Pubdate', 'Telescope', 'Program']
    """
    all_results = []
    page = 0

    # Set up progress tracking if max_pages is provided and tqdm is available.
    if max_pages is not None and tqdm is not None:
        progress_bar = tqdm(total=max_pages, desc="Fetching pages")
    else:
        progress_bar = None

    while True:
        if max_pages is not None and page >= max_pages:
            print(f"Reached the maximum page limit: {max_pages}")
            break
        
        print(f"Fetching page {page + 1}...")

        success = False
        for attempt in range(retries):
            try:
                # Create a query for one page.
                query = ads.SearchQuery(
                    q=q,
                    fl=['id', 'bibcode', 'title', 'pubdate', 'citation_count'],
                    rows=rows,
                    start=page * rows,
                    max_pages=1  # Force this instance to fetch one page.
                )
                papers = list(query)  # Fetch the current page.
                success = True
                break  # Exit the retry loop on success.
            except ads.exceptions.APIResponseError as e:
                print(f"Page {page + 1}, attempt {attempt + 1}/{retries} failed with APIResponseError: {e}")
                time.sleep(delay)
            except Exception as e:
                print(f"Unexpected error on page {page + 1}, attempt {attempt + 1}/{retries}: {e}")
                time.sleep(delay)
        
        if not success:
            print(f"Failed to fetch page {page + 1} after {retries} attempts. Exiting loop.")
            break
        
        if not papers:
            print(f"No more records found on page {page + 1}.")
            break
        
        # Process current page results.
        for paper in papers:
            record = {
                'Bibcode': paper.bibcode,
                'Title': paper.title,
                'ID': paper.id,
                'Pubdate': paper.pubdate,
                'Telescope': telescope_name,
                'Program': program_name
            }
            all_results.append(record)
        
        num_records = len(papers)
        print(f"Fetched {num_records} records from page {page + 1}.")
        
        if progress_bar is not None:
            progress_bar.update(1)
        
        page += 1

    if progress_bar is not None:
        progress_bar.close()
    
    print(f"Total records fetched: {len(all_results)}")
    df = pd.DataFrame.from_records(all_results)
    return df

# Example usage:
# new_library_df = fetch_ads_dataframe_page_by_page("docs(library/N_BP0kgwTeGEU4TgaecDNw)",
#                                                     telescope_name="New Telescope",
#                                                     program_name="New Program",
#                                                     rows=200,
#                                                     max_pages=100)

def fetch_ads_bibgroup_dataframe_page_by_page(bibgroup, telescope_name='telescope_name', program_name='program_name',
                                       rows=200, retries=3, delay=5, max_pages=None):
    """
    Fetch ADS data one page at a time to help avoid connection timeouts, with progress tracking.
    
    Parameters:
        q (str): The ADS search query.
        telescope_name (str): Name to populate the 'Telescope' column.
        program_name (str): Name to populate the 'Program' column.
        rows (int): Number of results per page.
        retries (int): Number of times to retry a page if a connection error occurs.
        delay (int): Delay (in seconds) between retries.
        max_pages (int, optional): Maximum number of pages to fetch. If None, fetch until no more results.
        
    Returns:
        pd.DataFrame: A DataFrame with columns:
                      ['Bibcode', 'Title', 'ID', 
                       'Pubdate', 'Telescope', 'Program']
    """
    all_results = []
    page = 0

    # Set up progress tracking if max_pages is provided and tqdm is available.
    if max_pages is not None and tqdm is not None:
        progress_bar = tqdm(total=max_pages, desc="Fetching pages")
    else:
        progress_bar = None

    while True:
        if max_pages is not None and page >= max_pages:
            print(f"Reached the maximum page limit: {max_pages}")
            break
        
        print(f"Fetching page {page + 1}...")

        success = False
        for attempt in range(retries):
            try:
                # Create a query for one page.
                query = ads.SearchQuery(
                    bibgroup = bibgroup,
                    fl=['id', 'bibcode', 'title', 'pubdate', 'citation_count'],
                    rows=rows,
                    start=page * rows,
                    max_pages=1  # Force this instance to fetch one page.
                )
                papers = list(query)  # Fetch the current page.
                success = True
                break  # Exit the retry loop on success.
            except ads.exceptions.APIResponseError as e:
                print(f"Page {page + 1}, attempt {attempt + 1}/{retries} failed with APIResponseError: {e}")
                time.sleep(delay)
            except Exception as e:
                print(f"Unexpected error on page {page + 1}, attempt {attempt + 1}/{retries}: {e}")
                time.sleep(delay)
        
        if not success:
            print(f"Failed to fetch page {page + 1} after {retries} attempts. Exiting loop.")
            break
        
        if not papers:
            print(f"No more records found on page {page + 1}.")
            break
        
        # Process current page results.
        for paper in papers:
            record = {
                'Bibcode': paper.bibcode,
                'Title': paper.title,
                'ID': paper.id,
                'Pubdate': paper.pubdate,
                'Telescope': telescope_name,
                'Program': program_name
            }
            all_results.append(record)
        
        num_records = len(papers)
        print(f"Fetched {num_records} records from page {page + 1}.")
        
        if progress_bar is not None:
            progress_bar.update(1)
        
        page += 1

    if progress_bar is not None:
        progress_bar.close()
    
    print(f"Total records fetched: {len(all_results)}")
    df = pd.DataFrame.from_records(all_results)
    return df

# Example usage:
# new_library_df = fetch_ads_bib_group_dataframe_page_by_page("docs(library/N_BP0kgwTeGEU4TgaecDNw)",
#                                                     telescope_name="New Telescope",
#                                                     program_name="New Program",
#                                                     rows=200,
#                                                     max_pages=100)