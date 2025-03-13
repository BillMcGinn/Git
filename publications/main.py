#Import libraries
#Must install all these libraries via command line for script to run
import requests #necessary library for ADS access
import ads #necessary library for ADS access
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

#Must use your own personal ADS key
#Bill McGinn's ADS key is: nTigmrHHUwONTbRHW2ceHSC5wiUDst4GRUHGevRM
#Your ADS key can be found at https://ui.adsabs.harvard.edu/user/settings/token after making an ADS account
#your account is limited to searching for 5000 pages of library results per day, which is beyond adequate for this script
ads.config.token = "nTigmrHHUwONTbRHW2ceHSC5wiUDst4GRUHGevRM"

#Search paramaters related to the MSO and CSDC libraries, these libraries are not maintaned in a "bibgroup"
MSOKey = 'docs(library/N_BP0kgwTeGEU4TgaecDNw)' #MSO LIbrary
MayallKey = 'docs(library/eJh5qR3GSp28FDUP0qenQg)' #Mayall Library
WIYNKey = 'docs(library/s_JxIgbaRiOv2O3J9HljuQ)' #WIYN Library
BlancoKey = 'docs(library/bl0RXk6TT2-WzZEYr6mBCw)' #Blanco Library
SOARKey = 'docs(library/-vb-OttsRwuGfAc9nBUMiw)' # SOAR Library
GeminiNorthKey ='docs(library/_vJwt8lESOGcOGJJzmFuxA)' #Gemini North Library
GeminiSouthKey = 'docs(library/X1ad6z5_TOmP28LXwuP4eA)' #Gemini South Library
CSDCKey = 'docs(library/25P0rj8HRtOhxtrkjr1v5w)' #CSDC Library
AstroKey = 'docs(library/yW43ynzZRuatTILqn9LuAA)' #Astro Data Lab Library
NOIRLabSourceKey = 'docs(library/jW-NkLN3Sc-yMmIuFYZD1w)' #NOIRLab Source Catalog Key
ANTARESKey = 'docs(library/dAulqOR8RmyZmMV8tbLwcg)' #Antares Key
DECamKey = 'docs(library/mWwQSbmuRheit_4CF9ahWA)' #DECam Community Key
SMARTSKey = 'docs(library/AK0BVB-aSCeFHOFc_2fm-g)' #SMARTS Library
WIYN9Key = 'docs(library/V4vvstqjTL6Lc5u_D_N0ug)' #WIYN .9m Library
NOIRLabStaffRefereedKey = 'docs(library/RrGZ7UpxRSaRQXn8ZRtL2g)+property:refereed' #Library of all refereed NOIRLab staff publications
NOIRLabStaffNotRefereedKey = 'docs(library/RrGZ7UpxRSaRQXn8ZRtL2g)+property:notrefereed' #Library of all non refereed NOIRLab staff publications

#NOIRlab bibgroup data
print("Fetching NOIRLab bibgroup data... ")
#Fetch the NOIRLab bibgroup data
NOIRLab = fetch_ads_bibgroup_dataframe_page_by_page(NOIRLabStaffRefereedKey, telescope_name = 'NOIRLab', program_name = 'NOIRLab')
print("NOIRLab bibgroup data fetched successfully.")

#MSO library data
print("Fetching MSO library data...")
#Fetch the MSO library data
MSO = fetch_ads_dataframe_page_by_page(MSOKey, telescope_name = 'MSO', program_name = 'MSO')
print("MSO library data fetched successfully.")

#Mayall library data
print("Fetching Mayall library data...")
#Fetch the Mayall library data
Mayall = fetch_ads_dataframe_page_by_page(MayallKey, telescope_name = 'Mayall', program_name = 'MSO')
print("Mayall library data fetched successfully.")

#WIYN library data
print("Fetching WIYN library data...")
#Fetch the WIYN library data
WIYN = fetch_ads_dataframe_page_by_page(WIYNKey, telescope_name = 'WIYN 3.5', program_name = 'MSO')
print("WIYN library data fetched successfully.")

#Blanco library data
print("Fetching Blanco library data...")
#Fetch the Blanco library data
Blanco = fetch_ads_dataframe_page_by_page(BlancoKey, telescope_name = 'Blanco', program_name = 'MSO')
print("Blanco library data fetched successfully.")

#SOAR library data
print("Fetching SOAR library data...")
#Fetch the SOAR library data
SOAR = fetch_ads_dataframe_page_by_page(SOARKey, telescope_name = 'SOAR', program_name = 'MSO')
print("SOAR library data fetched successfully.")

#Gemini North library data
print("Fetching Gemini North library data...")
#Fetch the Gemini North library data
GeminiNorth = fetch_ads_dataframe_page_by_page(GeminiNorthKey, telescope_name = 'Gemini North', program_name = 'Gemini')
print("Gemini North library data fetched successfully.")

#Gemini South library data
print("Fetching Gemini South library data...")
#Fetch the Gemini South library data
GeminiSouth = fetch_ads_dataframe_page_by_page(GeminiSouthKey, telescope_name = 'Gemini South', program_name = 'Gemini')
print("Gemini South library data fetched successfully.")

#Gemini bibgroup data
print("Fetching Gemini bibgroup data...")
#Fetch the Gemini bibgroup data\
Gemini = fetch_ads_bibgroup_dataframe_page_by_page('docs(library/2Wz3q6hRrGmI1Q8nQ3w4kA)', telescope_name = 'Gemini', program_name = 'Gemini')
print("Gemini bibgroup data fetched successfully.")

#CSDC library data
print("Fetching CSDC library data...")
#Fetch the CSDC library data
CSDC = fetch_ads_dataframe_page_by_page(CSDCKey, telescope_name = 'CSDC', program_name = 'CSDC')
print("CSDC library data fetched successfully.")

#Astro Data Lab library data
print("Fetching Astro Data Lab library data...")
#Fetch the Astro Data Lab library data
AstroDataLab = fetch_ads_dataframe_page_by_page(AstroKey, telescope_name = 'Astro Data Lab', program_name = 'CSDC')
print("Astro Data Lab library data fetched successfully.")

#NOIRLab Source Catalog library data
print("Fetching NOIRLab Source Catalog library data...")
#Fetch the NOIRLab Source Catalog library data
NOIRLabSource = fetch_ads_dataframe_page_by_page(NOIRLabSourceKey, telescope_name = 'NOIRLab Source Catalog', program_name = 'CSDC')
print("NOIRLab Source Catalog library data fetched successfully.")

#ANTARES library data
print("Fetching ANTARES library data...")
#Fetch the ANTARES library data
ANTARES = fetch_ads_dataframe_page_by_page(ANTARESKey, telescope_name = 'ANTARES', program_name = 'CSDC')
print("ANTARES library data fetched successfully.")

#DECam library data
print("Fetching DECam library data...")
#Fetch the DECam library data
DECam = fetch_ads_dataframe_page_by_page(DECamKey, telescope_name = 'DECam', program_name = 'CSDC')
print("DECam library data fetched successfully.")

#SMARTS library data
print("Fetching SMARTS library data...")
#Fetch the SMARTS library data
SMARTS = fetch_ads_dataframe_page_by_page(SMARTSKey, telescope_name = 'SMARTS', program_name = 'MSO')
print("SMARTS library data fetched successfully.")

#NOIRLab Refereed library data
print("Fetching NOIRLab Refereed library data...")
#Fetch the NOIRLab Refereed library data
NOIRLabRefereed = fetch_ads_dataframe_page_by_page(NOIRLabStaffRefereedKey, telescope_name = 'NOIRLab Refereed', program_name = 'Staff')
print("NOIRLab Refereed library data fetched successfully.")

#NOIRLab Not Refereed library data
print("Fetching NOIRLab Not Refereed library data...")
#Fetch the NOIRLab Not Refereed library data
NOIRLabNotRefereed = fetch_ads_dataframe_page_by_page(NOIRLabStaffNotRefereedKey, telescope_name = 'NOIRLab Not Refereed', program_name = 'Staff')
print("NOIRLab Not Refereed library data fetched successfully.")

