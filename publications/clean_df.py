import pandas as pd
from pandas.tseries.offsets import DateOffset

def clean_publication_dates(df):
    """
    Clean the publication date data in the DataFrame.

    Steps:
      - Convert 'Pubdate' to a datetime.
      - Split the datetime into 'Year', 'Month', 'Day' columns.
      - Adjust the day (e.g., adding 1 day if needed).
      - Remove rows with invalid publication dates (e.g., month == 0).
      - Create a 'CleanDate' column.
      - Create a 'FiscalDate' by adding a 3-month offset.
      - Extract 'FiscalYear' from 'FiscalDate'.
      - Reset the index.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing a 'Pubdate' column.
    
    Returns:
        pd.DataFrame: A new DataFrame with clean date columns.
    """
    df_clean = df.copy()
    
    # Convert the 'Pubdate' column to datetime.
    # Errors='coerce' converts malformed dates to NaT.
    df_clean['CleanDate'] = pd.to_datetime(df_clean['Pubdate'], errors='coerce')
    
    # Drop rows with invalid dates (NaT values)
    df_clean = df_clean.dropna(subset=['CleanDate'])
    
    # Optionally adjust the day by adding one day if needed.
    # (This avoids manual splitting and integer conversion.)
    df_clean['CleanDate'] = df_clean['CleanDate'] + pd.Timedelta(days=1)
    
    # Extract Year, Month, and Day as separate columns.
    df_clean['Year'] = df_clean['CleanDate'].dt.year
    df_clean['Month'] = df_clean['CleanDate'].dt.month
    df_clean['Day'] = df_clean['CleanDate'].dt.day
    
    # Remove rows with invalid publication dates (for example, where Month == 0).
    # (Normally, pd.to_datetime would have handled this, but you can include additional logic if needed.)
    df_clean = df_clean[df_clean['Month'] != 0]
    
    # Create FiscalDate by adding a 3-month offset.
    df_clean['FiscalDate'] = df_clean['CleanDate'] + DateOffset(months=3)
    
    # Extract FiscalYear from FiscalDate.
    df_clean['FiscalYear'] = df_clean['FiscalDate'].dt.year
    
    # Reset index
    df_clean.reset_index(drop=True, inplace=True)
    
    return df_clean