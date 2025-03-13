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
#from ads_query import fetch_ads_dataframe
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

print("Fetching MSO library data...")
#Fetch the MSO library data
MSO = fetch_ads_dataframe_page_by_page(MSOKey, telescope_name = 'MSO', program_name = 'MSO')
print("MSO library data fetched successfully.")
print(MSO)