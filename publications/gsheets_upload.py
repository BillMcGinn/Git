def upload_dataframes_to_gsheets(dataframes_dict, sheet_key, credentials_file='gsheetskey.json'):
    """
    Upload multiple DataFrames to a Google Sheets document.

    This function uses the df2gspread package to upload each DataFrame in the provided dictionary
    to a separate worksheet (tab) within the same Google Sheets document. The keys of the dictionary
    should be the desired worksheet names and the values should be the corresponding DataFrames.

    Prerequisites:
      - The JSON credentials file (e.g., 'gsheetskey.json') must be saved in the working directory.
      - The service account (e.g., noirlabpublicationsdashboard@publications-366923.iam.gserviceaccount.com)
        must have editor access to the target Google Sheets document.
      - Required packages: gspread, df2gspread, oauth2client.

    Parameters:
        dataframes_dict (dict): A dictionary where keys are worksheet names (strings) and values are DataFrame objects.
        sheet_key (str): The key (ID) of the Google Sheets document.
        credentials_file (str): The filename of the JSON credentials file.

    Returns:
        None
    """
    from oauth2client.service_account import ServiceAccountCredentials
    from df2gspread import df2gspread as d2g
    import gspread

    # Define the scope of the application.
    scope_app = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']

    # Load the service account credentials from the JSON key file.
    try:
        cred = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope_app)
    except Exception as e:
        print(f"Error loading credentials from {credentials_file}: {e}")
        return

    # Authorize the gspread client.
    try:
        gc = gspread.authorize(cred)
    except Exception as e:
        print(f"Error authorizing gspread client: {e}")
        return

    # Iterate over the dictionary and upload each DataFrame.
    for worksheet_name, df in dataframes_dict.items():
        print(f"Uploading DataFrame to worksheet '{worksheet_name}'...")
        try:
            d2g.upload(df, sheet_key, worksheet_name, credentials=cred, row_names=False)
            print(f"Worksheet '{worksheet_name}' uploaded successfully.")
        except Exception as e:
            print(f"Error uploading worksheet '{worksheet_name}': {e}")