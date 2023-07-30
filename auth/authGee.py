
import ee
import sys
import os
service_account = 'YOURSERVICEACCOUNT'

# The private key associated with your service account in JSON format.
EE_PRIVATE_KEY_FILE = 'auth/your.json'

if sys.platform in ['win32', 'darwin']:
    cwd = os.getcwd().replace('\\', '/')
    EE_PRIVATE_KEY_FILE = cwd + "/" + EE_PRIVATE_KEY_FILE

credentials = ee.ServiceAccountCredentials(service_account, EE_PRIVATE_KEY_FILE)

ee.Initialize(credentials)

