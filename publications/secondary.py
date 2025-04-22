#Import libraries
#Must install all these libraries via command line for script to run
import requests #necessary library for ADS access
import ads #necessary library for ADS access
#The Python API for ADS that is used in this script is documented at https://ads.readthedocs.io/en/latest/
import numpy as np
import pandas as pd
from datetime import datetime
from pandas.tseries.offsets import DateOffset
import gspread
from df2gspread import df2gspread as d2g
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
from ads_fetch import fetch_ads_dataframe_page_by_page
from ads_fetch import fetch_ads_bibgroup_dataframe_page_by_page
from clean_df import clean_publication_dates
from gsheets_upload import upload_dataframes_to_gsheets

#Must use your own personal ADS key
#Bill McGinn's ADS key is: nTigmrHHUwONTbRHW2ceHSC5wiUDst4GRUHGevRM
#Your ADS key can be found at https://ui.adsabs.harvard.edu/user/settings/token after making an ADS account
#your account is limited to searching for 5000 pages of library results per day, which is beyond adequate for this script
#The Python API for ADS that is used in this script is documented at https://ads.readthedocs.io/en/latest/
ads.config.token = "nTigmrHHUwONTbRHW2ceHSC5wiUDst4GRUHGevRM"

#Search paramaters related to the MSO and CSDC libraries, these libraries are not maintaned in a "bibgroup"

#WIYN Charts
WIYNKey = 'docs(library/s_JxIgbaRiOv2O3J9HljuQ)' #WIYN Library
WIYN35TechKey = 'docs(library/QCHeHCQqTHGSK_BgCCWezw)' #WIYN 3.5m Technical Library
WIYN35DissertationKey = 'docs(library/pcmTuHNgSoO1kp66l_WIsQ)' #WIYN 3.5m Dissertation Library
WIYN35HydraKey  = 'docs(library/oyMY1sxjRluRh1pYZRkhtw)' #WIYN 3.5m Hydra Library
WIYN35NessiKey = 'docs(library/1SKfgJP-RZOnLwgJ6yDTUQ)' #WIYN 3.5m NESSI Library
WIYN35NEIDKey = 'docs(library/x9dVuW5lTJ-REEGGJyofcw)' #WIYN 3.5m NEID Library
WIYN35pDOIKey = 'docs(library/9sM83_28SWaco09iwby90g)' #WIYN 3.5m pDOI Library (example key, replace with actual)
WIYN9RefereedKey = 'docs(library/V4vvstqjTL6Lc5u_D_N0ug)' #WIYN 0.9m Refereed Library
WIYN9DissertationKey = 'docs(library/WYYXzNeaR1yFy8oLon6wow)' #WIYN 0.9m Dissertation Library

#WIYN / NEID Charts
NEIDSPIEKey = 'docs(library/THVLJ_yaRqya5HXj_oSPHQ)' #NEID SPIE Library
NEIDSolarKey = 'docs(library/mPeLhCsLQqaOuEsqb_nYMw)' #NEID Solar Library
    #NEID Refereed charts are in WIYN35NEIDKey above
NEIDTechnicalKey = 'docs(library/XtJy_LT9Qb2TEeKjYL6MWQ)' #NEID Technical Library
NEIDDissertationKey = 'docs(library/EUBsNnEeQXikJlABpp5QXg)' #NEID Dissertation Library

#Mayall Instruments Charts
MayallDesiKey = 'docs(library/GuY5b8LWQrKUvBpBeKMCTA)' #Mayall DESI Library
MayallMosaic1Key = 'docs(library/1b2d9g0uQx6c4a3k5e8f7w)' #Mayall Mosaic 1 Library
MayallMosaic3Key = 'docs(library/HrKPDtdFSLChzyBhY63ErQ)' #Mayall Mosaic 3 Library
MayallNewfirmKey = 'docs(library/qrX4BDq6TA6jkKiytrzwog)' #Mayall Newfirm Library
MayallRCSpectrographKey = 'docs(library/jFSmx3E-T-2eqmX1UyXbPA)' #Mayall RC Spectrograph Library

#Blanco Instrument Charts
BlancoDECamKey = 'docs(library/yL0w6HBoT2G6kJfSrYHqlQ">)' #Blanco DECam Library
BlancoMosaic2Key =  'docs(library/1itEjDVAS0WhCHqVr55FIw)' #Blanco Mosaic 2 Library
BlancoNewfirmKey = 'docs(library/l0pArcIkSBmrxgWwyAGkZA)' #Blanco Newfirm Library

#SOAR Instrument Charts
SOARGoodmanKey = 'docs(library/gDbace28QLCFgdxLJGkUEg)' #SOAR Goodman Library
SOARHRCamKey = 'docs(library/waE16PeKTJa0bQZ-ok_lOA)' #SOAR HRCam Library
SOARSAMKey = 'docs(library/y0nbFBXeT4iEv4vh8kR7bQ)' #SOAR SAM Library
SOARSOIKey = 'docs(library/qLFXoQucRB6JAUUhVqQQMw)' #SOAR SOI Library
SOARSIFSKey = 'docs(library/TlmyExqCS4qaqz8LbBF9JQ)' #SOAR SIFS Library
SOARSpartanKey =  'docs(library/LytbfbKtR3uDUoice2B4tQ)' #SOAR Spartan Library
SOARTripleSPec41Key = 'docs(library/7U-qRx2aT5Syd8U8US-AGA)' #SOAR TripleSpec 4.1 Library

#WIYN Charts Queries

#WIYN library data
print("Fetching WIYN library data...")
#Fetch the WIYN library data
MSO = fetch_ads_dataframe_page_by_page(WIYNKey, telescope_name = 'WIYN', program_name = 'WIYN')
print("WIYN library data fetched successfully.")

#WIYN35m Technical library data
print("Fetching WIYN 3.5m Technical library data...")
#Fetch the WIYN 3.5m Technical library data
WIYN35Tech = fetch_ads_dataframe_page_by_page(WIYN35TechKey, telescope_name = 'WIYN 3.5 Technical', program_name = 'WIYN')
print("WIYN 3.5m Technical library data fetched successfully.")

#WIYN35m Dissertation library data
print("Fetching WIYN 3.5m Dissertation library data...")
#Fetch the WIYN 3.5m Dissertation library data
WIYN35Dissertation = fetch_ads_dataframe_page_by_page(WIYN35DissertationKey, telescope_name = 'WIYN 3.5m Dissertation', program_name = 'WIYN')
print("WIYN 3.5m Dissertation library data fetched successfully.")

#WIYN35m Hydra library data
print("Fetching WIYN 3.5m Hydra library data...")
#Fetch the WIYN 3.5m Hydra library data
WIYN35Hydra = fetch_ads_dataframe_page_by_page(WIYN35HydraKey, telescope_name = 'WIYN 3.5m Hydra', program_name = 'WIYN')
print("WIYN 3.5m Hydra library data fetched successfully.")

#WIYN35m NESSI library data
print("Fetching WIYN 3.5m NESSI library data...")
#Fetch the WIYN 3.5m NESSI library data
WIYN35Nessi = fetch_ads_dataframe_page_by_page(WIYN35NessiKey, telescope_name = 'WIYN 3.5m NESSI', program_name = 'WIYN')
print("WIYN 3.5m NESSI library data fetched successfully.")

#WIYN35m NEID library data
print("Fetching WIYN 3.5m NEID library data...")
#Fetch the WIYN 3.5m NEID library data
WIYN35NEID = fetch_ads_dataframe_page_by_page(WIYN35NEIDKey, telescope_name = 'WIYN 3.5m NEID', program_name = 'WIYN')
print("WIYN 3.5m NEID library data fetched successfully.")

#WIYN35m pDOI library data
print("Fetching WIYN 3.5m pDOI library data...")
#Fetch the WIYN 3.5m pDOI library data
WIYN35pDOI = fetch_ads_dataframe_page_by_page(WIYN35pDOIKey, telescope_name = 'WIYN 3.5m pDOI', program_name = 'WIYN')
print("WIYN 3.5m pDOI library data fetched successfully.")

#WIYN 0.9m Refereed library data
print("Fetching WIYN 0.9m Refereed library data...")
#Fetch the WIYN 0.9m Refereed library data
WIYN9Refereed = fetch_ads_dataframe_page_by_page(WIYN9RefereedKey, telescope_name = 'WIYN 0.9m Refereed', program_name = 'WIYN')
print("WIYN 0.9m Refereed library data fetched successfully.")

#WIYN 0.9m Dissertation library data
print("Fetching WIYN 0.9m Dissertation library data...")
#Fetch the WIYN 0.9m Dissertation library data
WIYN9DissertationKey = fetch_ads_dataframe_page_by_page(WIYN9DissertationKey, telescope_name = 'WIYN 0.9m Dissertation', program_name = 'WIYN')
print("WIYN 0.9m Dissertation library data fetched successfully.")

#WIYN / NEID Charts Queries

#NEID SPIE library data
print("Fetching NEID SPIE library data...")
#Fetch the NEID SPIE library data
NEIDSPIE = fetch_ads_dataframe_page_by_page(NEIDSPIEKey, telescope_name = 'NEID SPIE', program_name = 'WIYN NEID')
print("NEID SPIE library data fetched successfully.")

#NEID Solar library data
print("Fetching NEID Solar library data...")
#Fetch the NEID Solar library data
NEIDSolar = fetch_ads_dataframe_page_by_page(NEIDSolarKey, telescope_name = 'NEID Solar', program_name = 'WIYN NEID')
print("NEID Solar library data fetched successfully.")

#NEID Refereed charts are in WIYN35NEIDKey above

#NEID Technical library data
print("Fetching NEID Technical library data...")
#Fetch the NEID Technical library data
NEIDTechnical = fetch_ads_dataframe_page_by_page(NEIDTechnicalKey, telescope_name = 'NEID Technical', program_name = 'WIYN NEID')
print("NEID Technical library data fetched successfully.")

#NEID Dissertation library data
print("Fetching NEID Dissertation library data...")
#Fetch the NEID Dissertation library data
NEIDDissertation = fetch_ads_dataframe_page_by_page(NEIDDissertationKey, telescope_name = 'NEID Dissertation', program_name = 'WIYN NEID')
print("NEID Dissertation library data fetched successfully.")

#Mayall Instruments Charts Queries

#Mayall DESI library data
print("Fetching Mayall DESI library data...")
#Fetch the Mayall DESI library data
MayallDesi = fetch_ads_dataframe_page_by_page(MayallDesiKey, telescope_name = 'Mayall DESI', program_name = 'Mayall')
print("Mayall DESI library data fetched successfully.")

#Mayall Mosaic 1 library data
print("Fetching Mayall Mosaic 1 library data...")
#Fetch the Mayall Mosaic 1 library data
MayallMosaic1 = fetch_ads_dataframe_page_by_page(MayallMosaic1Key, telescope_name = 'Mayall Mosaic 1', program_name = 'Mayall')
print("Mayall Mosaic 1 library data fetched successfully.")

#Mayall Mosaic 3 library data
print("Fetching Mayall Mosaic 3 library data...")
#Fetch the Mayall Mosaic 3 library data
MayallMosaic3 = fetch_ads_dataframe_page_by_page(MayallMosaic3Key, telescope_name = 'Mayall Mosaic 3', program_name = 'Mayall')
print("Mayall Mosaic 3 library data fetched successfully.")

#Mayall Newfirm library data
print("Fetching Mayall Newfirm library data...")
#Fetch the Mayall Newfirm library data
MayallNewfirm = fetch_ads_dataframe_page_by_page(MayallNewfirmKey, telescope_name = 'Mayall Newfirm', program_name = 'Mayall')
print("Mayall Newfirm library data fetched successfully.")

#Mayall RC Spectrograph library data
print("Fetching Mayall RC Spectrograph library data...")
#Fetch the Mayall RC Spectrograph library data
MayallRCSpectrograph = fetch_ads_dataframe_page_by_page(MayallRCSpectrographKey, telescope_name = 'Mayall RC Spectrograph', program_name = 'Mayall')
print("Mayall RC Spectrograph library data fetched successfully.")

#Blanco Instruments Charts Queries

#Blanco DECam library data
print("Fetching Blanco DECam library data...")
#Fetch the Blanco DECam library data
BlancoDECam = fetch_ads_dataframe_page_by_page(BlancoDECamKey, telescope_name = 'Blanco DECam', program_name = 'Blanco')
print("Blanco DECam library data fetched successfully.")

#Blanco Mosaic 2 library data
print("Fetching Blanco Mosaic 2 library data...")
#Fetch the Blanco Mosaic 2 library data
BlancoMosaic2 = fetch_ads_dataframe_page_by_page(BlancoMosaic2Key, telescope_name = 'Blanco Mosaic 2', program_name = 'Blanco')
print("Blanco Mosaic 2 library data fetched successfully.")

# Blanco Newfirm library data
print("Fetching Blanco Newfirm library data...")
#Fetch the Blanco Newfirm library data
BlancoNewfirm = fetch_ads_dataframe_page_by_page(BlancoNewfirmKey, telescope_name = 'Blanco Newfirm', program_name = 'Blanco')
print("Blanco Newfirm library data fetched successfully.")

#SOAR Instruments Charts Queries

#SOAR Goodman library data
print("Fetching SOAR Goodman library data...")
#Fetch the SOAR Goodman library data
SOARGoodman = fetch_ads_dataframe_page_by_page(SOARGoodmanKey, telescope_name = 'SOAR Goodman', program_name = 'SOAR')
print("SOAR Goodman library data fetched successfully.")

#SOAR HRCam library data
print("Fetching SOAR HRCam library data...")
#Fetch the SOAR HRCam library data
SOARHRCam = fetch_ads_dataframe_page_by_page(SOARHRCamKey, telescope_name = 'SOAR HRCam', program_name = 'SOAR')
print("SOAR HRCam library data fetched successfully.")

#SOAR SAM library data
print("Fetching SOAR SAM library data...")
#Fetch the SOAR SAM library data
SOARSAM = fetch_ads_dataframe_page_by_page(SOARSAMKey, telescope_name = 'SOAR SAM', program_name = 'SOAR')
print("SOAR SAM library data fetched successfully.")

#SOAR SOI library data
print("Fetching SOAR SOI library data...")
#Fetch the SOAR SOI library data
SOARSOI = fetch_ads_dataframe_page_by_page(SOARSOIKey, telescope_name = 'SOAR SOI', program_name = 'SOAR')
print("SOAR SOI library data fetched successfully.")

#SOAR SIFS library data
print("Fetching SOAR SIFS library data...")
#Fetch the SOAR SIFS library data
SOARSIFSKey = fetch_ads_dataframe_page_by_page(SOARSIFSKey, telescope_name = 'SOAR SIFS', program_name = 'SOAR')
print("SOAR SIFS library data fetched successfully.")

#SOAR Spartan library data
print("Fetching SOAR Spartan library data...")
#Fetch the SOAR Spartan library data
SOARSpartan = fetch_ads_dataframe_page_by_page(SOARSpartanKey, telescope_name = 'SOAR Spartan', program_name = 'SOAR')
print("SOAR Spartan library data fetched successfully.")

#SOAR TripleSpec 4.1 library data
print("Fetching SOAR TripleSpec 4.1 library data...")
#Fetch the SOAR TripleSpec 4.1 library data
SOARTripleSPec41 = fetch_ads_dataframe_page_by_page(SOARTripleSPec41Key, telescope_name = 'SOAR TripleSpec 4.1', program_name = 'SOAR')
print("SOAR TripleSpec 4.1 library data fetched successfully.")

#Concatenate all the WIYN program dataframes into a single dataframe
WIYNProgram = pd.concat([WIYN, WIYN35Tech, WIYN35Dissertation, WIYN35Hydra, WIYN35Nessi, WIYN35NEID, WIYN35pDOI, WIYN9Refereed, WIYN9Dissertation], ignore_index=True)
#Concatenate all the NEID program dataframes into a single dataframe
NEIDProgram = pd.concat([NEIDSPIE, NEIDSolar, WIYN35NEID, NEIDTechnical, NEIDDissertation], ignore_index=True)
#Concatenate all the Mayall Instrument dataframes into a single dataframe
MayallInstruments = pd.concat([MayallDesi, MayallMosaic1, MayallMosaic3, MayallNewfirm, MayallRCSpectrograph], ignore_index=True)
#Concatenate all the WIYN Instrument dataframes into a single dataframe
WIYNInstruments = pd.concat([WIYN35Hydra,WIYN35Nessi, WIYN35NEID, WIYN35pDOI], ignore_index=True)
#Concatenate all the Blanco Instrument dataframes into a single dataframe
BlancoInstruments = pd.concat([BlancoDECam, BlancoMosaic2, BlancoNewfirm], ignore_index=True)
#Concatenate all the SOAR Instrument dataframes into a single dataframe
SOARInstruments = pd.concat([SOARGoodman, SOARHRCam, SOARSAM, SOARSOI, SOARSIFSKey, SOARSpartan, SOARTripleSPec41], ignore_index=True)

#Clean the dataframes
clean_WIYNProgram = clean_publication_dates(WIYNProgram)
clean_NEIDProgram = clean_publication_dates(NEIDProgram)
clean_MayallInstruments = clean_publication_dates(MayallInstruments)
clean_WIYNInstruments = clean_publication_dates(WIYNInstruments)
clean_BlancoInstruments = clean_publication_dates(BlancoInstruments)
clean_SOARInstruments = clean_publication_dates(SOARInstruments)
print("Dataframes cleaned successfully.")

#Export the dataframes to csv
clean_WIYNProgram.to_csv('WIYNProgram.csv', index=False)
clean_NEIDProgram.to_csv('NEIDProgram.csv', index=False)
clean_MayallInstruments.to_csv('MayallInstruments.csv', index=False)
clean_WIYNInstruments.to_csv('WIYNInstruments.csv', index=False)
clean_BlancoInstruments.to_csv('BlancoInstruments.csv', index=False)
clean_SOARInstruments.to_csv('SOARInstruments.csv', index=False)
print("Dataframes exported to csv successfully.")

#Create dataframe dictionary
dataframes = {
    'WIYNProgram': clean_WIYNProgram,
    'NEIDProgram': clean_NEIDProgram,
    'MayallInstruments': clean_MayallInstruments,
    'WIYNInstruments': clean_WIYNInstruments,
    'BlancoInstruments': clean_BlancoInstruments,
    'SOARInstruments': clean_SOARInstruments
}

#Publications data sheet key for https://docs.google.com/spreadsheets/d/1Kl9ao9gtcU5VcEMSN0QLittihgBqckx6vxKiYg6Z78A/
sheet_key = '1Kl9ao9gtcU5VcEMSN0QLittihgBqckx6vxKiYg6Z78A'

#Call helper function to upload dataframes to Google Sheets
print("Initiating helper function to upload dataframes to Google Sheets...")
upload_dataframes_to_gsheets(dataframes, sheet_key)
print("Upload finished")