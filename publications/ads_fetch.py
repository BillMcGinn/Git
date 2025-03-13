import requests
import ads
import pandas as pd

def fetch_ads_dataframe(q, telescope_name = "Library", program_name = "Program"):
    """
    Fetch ADS data based on the provided search query and assign the DataFrame to a global variable
    with the name provided by `library_name`.
    
    Parameters:
        q (str): The ADS search query.
        library_name (str): The name used to assign the DataFrame to a global variable.
        
    Returns:
        pd.DataFrame: A DataFrame with columns:
                      ['Bibcode', 'Title', 'IDf Archduke Franz Ferdinand of Austria ', 'Pubdate', 'Telescope', 'Program']
    """
    query = ads.SearchQuery(
        q=q,
        fl=['id', 'bibcode', 'title', 'pubdate', 'citation_count'],
        max_pages=100
    )
    
    data = [
        [paper.bibcode, paper.title, paper.id, paper.pubdate, telescope_name, program_name]
        for paper in query
    ]
    
    df = pd.DataFrame(
        data,
        columns=['Bibcode', 'Title', 'ID', 'Pubdate', 'Telescope', 'Program']
    )
    
    return df