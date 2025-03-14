import pandas as pd
from pandas.tseries.offsets import DateOffset

def clean_publication_dates(df):
    """
    Clean the publication date data in the DataFrame.

    This function performs the following steps:
      - Splits the 'Pubdate' column (assumed to be in "YYYY-MM-DD" string format)
        into separate 'Year', 'Month', and 'Day' columns.
      - Converts the 'Day' column to integer and adds 1.
      - Removes rows where the Month is zero (invalid publication dates).
      - Combines the 'Year', 'Month', and 'Day' columns into a proper datetime
        column called 'CleanDate'.
      - Creates a 'FiscalDate' column by adding a 3-month offset to 'CleanDate'.
      - Extracts the fiscal year into a new 'FiscalYear' column.
      - Resets the DataFrame index.

    Parameters:
        df (pd.DataFrame): The input DataFrame containing at least a 'Pubdate' column.

    Returns:
        pd.DataFrame: A new DataFrame with clean date columns.
    """
    print("Starting to clean publication dates...")
    # Work on a copy so the original DataFrame remains unchanged.
    df_clean = df.copy()
    print("Initial DataFrame shape:", df_clean.shape)

    # Check if 'Pubdate' column exists.
    if 'Pubdate' not in df_clean.columns:
        print("Error: 'Pubdate' column not found in DataFrame.")
        return df_clean

    # Split the 'Pubdate' column into Year, Month, and Day columns.
    print("Splitting 'Pubdate' column into 'Year', 'Month', 'Day'...")
    try:
        df_clean[['Year', 'Month', 'Day']] = df_clean['Pubdate'].str.split('-', expand=True)
    except Exception as e:
        print("Error while splitting 'Pubdate':", e)
        return df_clean

    print("After splitting, sample data:")
    print(df_clean[['Year', 'Month', 'Day']].head())

    # Convert the 'Day' column to integer and add 1.
    print("Converting 'Day' column to int and adding 1...")
    try:
        df_clean['Day'] = df_clean['Day'].astype(int) + 1
    except Exception as e:
        print("Error converting 'Day' column:", e)

    # Remove rows with invalid publication dates (where Month == 0).
    print("Removing rows with invalid 'Month' (Month == 0)...")
    try:
        initial_rows = df_clean.shape[0]
        df_clean = df_clean[df_clean['Month'].astype(int) != 0]
        removed_rows = initial_rows - df_clean.shape[0]
        print(f"Removed {removed_rows} invalid rows. New shape: {df_clean.shape}")
    except Exception as e:
        print("Error filtering invalid 'Month':", e)

    # Combine Year, Month, and Day into a proper datetime column.
    print("Creating 'CleanDate' column from 'Year', 'Month', and 'Day'...")
    try:
        df_clean['CleanDate'] = pd.to_datetime(
            df_clean['Year'] + '-' + df_clean['Month'].astype(str) + '-' + df_clean['Day'].astype(str),
            format='%Y-%m-%d',
            errors='coerce'
        )
        print("Created 'CleanDate'. Sample dates:")
        print(df_clean['CleanDate'].head())
    except Exception as e:
        print("Error creating 'CleanDate':", e)

    # Create FiscalDate by adding a 3-month offset to CleanDate.
    print("Creating 'FiscalDate' by adding 3 months to 'CleanDate'...")
    try:
        df_clean['FiscalDate'] = df_clean['CleanDate'] + DateOffset(months=3)
        print("Created 'FiscalDate'. Sample dates:")
        print(df_clean['FiscalDate'].head())
    except Exception as e:
        print("Error creating 'FiscalDate':", e)

    # Extract FiscalYear from FiscalDate.
    print("Extracting 'FiscalYear' from 'FiscalDate'...")
    try:
        df_clean['FiscalYear'] = df_clean['FiscalDate'].dt.year
        print("Created 'FiscalYear'. Sample years:")
        print(df_clean['FiscalYear'].head())
    except Exception as e:
        print("Error extracting 'FiscalYear':", e)

    # Reset the index of the DataFrame.
    print("Resetting the DataFrame index...")
    df_clean.reset_index(drop=True, inplace=True)
    print("Final DataFrame shape:", df_clean.shape)
    print("Finished cleaning publication dates.")

    return df_clean