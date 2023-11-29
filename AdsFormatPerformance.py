# Import the library.
import datetime
from googleads import ad_manager
from googleads import errors
from datetime import date, timedelta, datetime
import tempfile
import pandas as pd
import pprint
import _locale
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import csv
import os
from oauth2client.service_account import ServiceAccountCredentials

_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])

def main():
    channel = []
    platform = []
    ad_unit = []
    ad_exchange_impression = []
    ad_exchange_impression_all = []
    ad_exchange_revenue = []
    ad_exchange_revenue_all = []
    ad_exchange_cpm = []
    ad_exchange_cpm_all = []
    adsense_impression = []
    adsense_revenue = []
    adsense_cpm = []
    date_final = []
    hour = []
    pricing_rule = []
    Count = 0
    WriteCount = 0
    WriteCount_ALL = 0
    WriteCount_backup = 0
    pricing_rule_id = []
    ad_unit_id = []
    today = datetime.now().strftime("%Y-%m-%d")
    hour_text = (datetime.now() + timedelta(hours=8)).strftime("%H")
    ad_exchange_last = []



    ################# Delete all csv in Report Folder ####################
    filelist = [ f for f in os.listdir("/home/pamelatsui/Report/Ads_Format_Report/Temp") if f.endswith(".csv") ]
    for f in filelist:
        os.remove(os.path.join("/home/pamelatsui/Report/Ads_Format_Report/Temp", f))

    ################# Google Ad Manager API Authentication ####################
    # Initialize a client object, by default uses the credentials in ~/googleads.yaml.
    client = ad_manager.AdManagerClient.LoadFromStorage()

    # Initialize services.
    network_service = client.GetService('NetworkService', version='v202005')
    report_service = client.GetService('ReportService', version= 'v202005')

    # Initialize a report downloader.
    report_downloader = client.GetDataDownloader(version='v202005')
    print('Done getting credentials for dfp')

    # Make a request.
    current_network = network_service.getCurrentNetwork()

    print ('Found network %s (%s)!' % (current_network['displayName'],
                                      current_network['networkCode']))


    report_job_ad_exchange = {
        'reportQuery': {
        'dimensions': ['AD_EXCHANGE_DATE','AD_EXCHANGE_INVENTORY_SIZE','AD_EXCHANGE_CREATIVE_SIZES'],
        'columns': ['AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_ESTIMATED_REVENUE', 'AD_EXCHANGE_AD_ECPM','AD_EXCHANGE_CLICKS'],
        'dateRangeType': 'CUSTOM_DATE',
        'timeZoneType': 'AD_EXCHANGE',
        'startDate': date.today() - timedelta(days=1),
        'endDate': date.today()- timedelta(days=1)
        }
    }

    try:
        # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(report_job_ad_exchange)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    with tempfile.NamedTemporaryFile(
        suffix='.csv', mode='wb', delete=False, dir='/home/pamelatsui/Report/Ads_Format_Report/Temp') as report_ad_exchange:
        filewriter = csv.writer(report_ad_exchange, delimiter=',')

        # Downloads the response from PQL select statement to the specified file
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_ad_exchange, use_gzip_compression=False)

    print('Saved line items to... %s' % (report_ad_exchange.name))


    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']


    creds = ServiceAccountCredentials.from_json_keyfile_name("/home/pamelatsui/Report/creds2.json",scope)
    client = gspread.authorize(creds)

    print('Done getting credentials for google sheets')



    sheet = client.open('Ads Format Performance Report').worksheet('ALL')
    d = pd.read_csv(report_ad_exchange.name, skiprows=1)

    all = sheet.get_all_values()
    end_row = len(all) + 1

    print(end_row)


    df = pd.DataFrame(data=d)
    new_df= pd.DataFrame()
    set_with_dataframe(sheet, df,row=end_row, col=1, include_index=False, include_column_header=True,
                       resize=False, allow_formulas=True)



if __name__=="__main__":
    main()

