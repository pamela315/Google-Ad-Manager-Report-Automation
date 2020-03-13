# -*- coding: utf-8 -*-
"""
Created on Wed Feb 12 10:49:24 2020

@author: pamelatsui
"""


# Import the library.
import datetime
from googleads import ad_manager
from googleads import errors
from datetime import date, timedelta
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
key = pygsheets.authorize(service_file='creds.json')
print('Done getting credentials for google sheets')

# Open Google Sheet API - TM Performance Adjustment Note
sheet_note = key.open_by_url('INSERT URL')
worksheet_raw_key = sheet_note.worksheet_by_title('Raw_ATS')

# Open Google Sheet API - Target Marketing - Knowledge Library
sheet_library = key.open_by_url('INSERT URL')
worksheet_library = sheet_library.worksheet_by_title('Raw_ATS')

# Open Google Sheet API and fetch order list - TM Campaign Weekly Summary Report
sheet_report = key.open_by_url('INSERT URL')
worksheet_report1 = sheet_report.worksheet_by_title('Raw_Line item')
worksheet_report2 = sheet_report.worksheet_by_title('Raw_Key value')
worksheet_report3 = sheet_report.worksheet_by_title('Raw_Platform')
worksheet_report4 = sheet_report.worksheet_by_title('Campaign')

#Fetch the order list to be filtered
list_report = worksheet_report4.get_col(2)
list = pd.DataFrame(list_report, columns=['Order'])
print(list)

############## Fetching all Orders Name & Orders Id ####################
# Initialize appropriate service.
order_service = client.GetService('OrderService', version='v201911')
print('Done to get service')

# Create a statement to select orders.
statement_builder = ad_manager.StatementBuilder(version='v201911')

# Retrieve a small amount of orders at a time, paging
# through until all orders have been retrieved.
while True:
    response = order_service.getOrdersByStatement(statement_builder.ToStatement())
    if 'results' in response and len(response['results']):
        for order in response['results']:
            # Print out some information for each order.
            print('Order with ID "%d" and name "%s" was found.' % (order['id'],
                                                                     order['name']))
        statement_builder.offset += statement_builder.limit
    else:
        break
print('\nNumber of results found: %s' % response['totalResultSetSize'])

################## Fetching key-value report #####################
# State report job query - Key Value
report_job_key = {
    'reportQuery': {
    'dimensions':['ORDER_NAME','CUSTOM_CRITERIA','ORDER_ID'],
    'columns':['AD_SERVER_IMPRESSIONS', 'AD_SERVER_CLICKS'],
    'dateRangeType': 'CUSTOM_DATE',
    'startDate': datetime.date(2019,10,28),
    'endDate': date.today()
    }
}

try:
    # Run the report and wait for it to finish.
    report_job_id = report_downloader.WaitForReport(report_job_key)
except errors.AdManagerReportError as e:
    print('Failed to generate report. Error was: %s' % e)

with tempfile.NamedTemporaryFile(
    suffix='.csv.gz', mode='wb', delete=False) as report_keyvalue:

    # Downloads the response from PQL select statement to the specified file
    report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_keyvalue)

print('Saved line items to... %s' % (report_keyvalue.name))

# Use pandas to join the two csv files into a match table
report_keyvalue2 = pd.read_csv(report_keyvalue.name)
print('Done reading report')
print(report_keyvalue2['Dimension.CUSTOM_CRITERIA'].head())

# Filter the result according to list
result_key = report_keyvalue2.loc[report_keyvalue2['Dimension.ORDER_NAME'].isin(list_report)]
pp.pprint(result_key)
# Export file
# result.to_csv(r'/Users/pamelatsui/Desktop/result.csv', index=False)

################## Fetching line item report #####################
# State report job query - line item
report_job_lineitem = {
    'reportQuery': {
    'dimensions':['ORDER_NAME','LINE_ITEM_NAME','DATE','ORDER_ID','LINE_ITEM_ID'],
    'columns':['AD_SERVER_IMPRESSIONS', 'AD_SERVER_CLICKS'],
    'dateRangeType': 'CUSTOM_DATE',
    'startDate': datetime.date(2019,10,28),
    'endDate': date.today()
    }
}

try:
    # Run the report and wait for it to finish.
    report_job_id2 = report_downloader.WaitForReport(report_job_lineitem)
except errors.AdManagerReportError as e:
    print('Failed to generate report. Error was: %s' % e)

with tempfile.NamedTemporaryFile(
    suffix='.csv.gz', mode='wb', delete=False) as line_item_report:

    # Downloads the response from PQL select statement to the specified file
    report_downloader.DownloadReportToFile(report_job_id2, 'CSV_DUMP', line_item_report)

print('Saved line items to... %s' % (line_item_report.name))

# Use pandas to join the two csv files into a match table
line_item_report2 = pd.read_csv(line_item_report.name)
print('Done reading report')
print(line_item_report2['Dimension.LINE_ITEM_NAME'].head())

# Filter the result according to list
result_line_item = line_item_report2.loc[line_item_report2['Dimension.ORDER_NAME'].isin(list_report)]
pp.pprint(result_line_item)

################## Fetching channel report #####################
# State report job query - channel
report_job_channel = {
    'reportQuery': {
    'dimensions':['ORDER_NAME','AD_UNIT_NAME','ORDER_ID','AD_UNIT_ID'],
    'columns':['AD_SERVER_IMPRESSIONS', 'AD_SERVER_CLICKS'],
    'dateRangeType': 'CUSTOM_DATE',
    'startDate': datetime.date(2019,10,28),
    'endDate': date.today()
    }
}

try:
    # Run the report and wait for it to finish.
    report_job_id3 = report_downloader.WaitForReport(report_job_channel)
except errors.AdManagerReportError as e:
    print('Failed to generate report. Error was: %s' % e)

with tempfile.NamedTemporaryFile(
    suffix='.csv.gz', mode='wb', delete=False) as channel_report:

    # Downloads the response from PQL select statement to the specified file
    report_downloader.DownloadReportToFile(report_job_id3, 'CSV_DUMP', channel_report)

print('Saved line items to... %s' % (channel_report.name))

# Use pandas to join the two csv files into a match table
channel_report2 = pd.read_csv(channel_report.name)
print('Done reading report')
print(channel_report2['Dimension.AD_UNIT_NAME'].head())

# Filter the result according to list
result_channel = channel_report2.loc[channel_report2['Dimension.ORDER_NAME'].isin(list_report)]
pp.pprint(result_channel)

########################## Update all necessary data ############################
# Update line item data for TM summary report
worksheet_report1.set_dataframe(result_line_item, 'A1')

# Update key value data for three reports
worksheet_raw_key.set_dataframe(result_key, 'A1')
worksheet_library.set_dataframe(result_key, 'A1')
worksheet_report2.set_dataframe(result_key, 'A1')

# Update channel data for TM summary report
worksheet_report3.set_dataframe(result_channel, 'A1')

print('Done updating all data')