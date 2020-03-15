# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 10:49:24 2020

@author: pamelatsui
"""


# Import the library.
from googleads import ad_manager
from googleads import errors
import tempfile
import pandas as pd
import pygsheets
import pprint
import _locale

_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])

################# Google Ad Manager API Authentication ####################
# Initialize a client object, by default uses the credentials in ~/googleads.yaml.
client = ad_manager.AdManagerClient.LoadFromStorage()

# Initialize services.
network_service = client.GetService('NetworkService', version='v201911')
report_service = client.GetService('ReportService', version= 'v201911')

# Initialize a report downloader.
report_downloader = client.GetDataDownloader(version='v201911')
print('Done getting credentials for dfp')

# Make a request.
current_network = network_service.getCurrentNetwork()

print ('Found network %s (%s)!' % (current_network['displayName'],
                                  current_network['networkCode']))

############### Google Sheet API Authentication ###################
pp = pprint.PrettyPrinter()

# Authorise pygsheets
key = pygsheets.authorize(service_file= 'creds.json')
print('Done getting credentials for google sheets')

# Open Google Sheet API - Revenue report Raw data
sheet_revenue = key.open_by_url('INSERT URL')
RawData = sheet_revenue.worksheet_by_title('RawData')
CampaignData = sheet_revenue.worksheet_by_title('Campaign data')

#Fetch the order list to be filtered
df = CampaignData.get_as_df(start='C7', end=None)
list = df.loc[df['*This Month'] == 'Y'].iloc[:, 0]
print(list)


################## Fetching Raw Data #####################
# State report job query
report_job = {
    'reportQuery': {
    'dimensions':['ORDER_NAME','LINE_ITEM_NAME','AD_UNIT_NAME'],
    'columns':[ 'AD_SERVER_CLICKS','AD_SERVER_IMPRESSIONS'],
    'dateRangeType': 'LAST_3_MONTHS'
    }
}

try:
    # Run the report and wait for it to finish.
    report_job_data = report_downloader.WaitForReport(report_job)
except errors.AdManagerReportError as e:
    print('Failed to generate report. Error was: %s' % e)

with tempfile.NamedTemporaryFile(
    suffix='.csv.gz', mode='wb', delete=False) as line_item_report:

    # Downloads the response from PQL select statement to the specified file
    report_downloader.DownloadReportToFile(report_job_data, 'CSV_DUMP', line_item_report)

print('Saved line items to... %s' % (line_item_report.name))

# Use pandas to join the two csv files into a match table
line_item_report2 = pd.read_csv(line_item_report.name)
print('Done reading report')

# Filter the result according to list
result_line_item = line_item_report2.loc[line_item_report2['Dimension.ORDER_NAME'].isin(list)]
pp.pprint(result_line_item)


########################## Update all necessary data ############################
# Update data
RawData.set_dataframe(result_line_item, 'A1')

print('Done updating all data')
