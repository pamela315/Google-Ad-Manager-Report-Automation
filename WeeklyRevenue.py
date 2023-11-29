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

    dateFinal = []
    adUnit = []
    advertiser = []
    demandChannel = []
    impressions = []
    adsenseImps = []
    adsenseRev = []
    adxImps = []
    adxRev = []
    advertiserType =[]
    platform = []
    channel = []
    totalRev =[]
    Count  = 0
    WriteCount = 0


    ################# Delete all csv in Report Folder ####################
    filelist = [ f for f in os.listdir("/home/pamelatsui/Report/WeeklyRevenue/Temp") if f.endswith(".csv") ]
    for f in filelist:
        os.remove(os.path.join("/home/pamelatsui/Report/WeeklyRevenue/Temp", f))

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

    ################# Raw Date ####################
    report_job = {
        'reportQuery': {
        'dimensions': ['DATE','AD_UNIT_NAME','ADVERTISER_NAME','DEMAND_CHANNEL_NAME'],
        'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_REVENUE','AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS','AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE'],
        ###'dateRangeType': 'LAST_WEEK'
        'dateRangeType': 'CUSTOM_DATE',
        'startDate': date.today() - timedelta(days=1),
        'endDate': date.today()- timedelta(days=1)
        }
    }

    try:
        # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(report_job)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    with tempfile.NamedTemporaryFile(
        suffix='.csv', mode='wb', delete=False, dir='/home/pamelatsui/Report/WeeklyRevenue/Temp') as report:
        filewriter = csv.writer(report, delimiter=',')

        # Downloads the response from PQL select statement to the specified file
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report, use_gzip_compression=False)

    print('Saved line items to... %s' % (report.name))


          # read csv data
    with open(report.name , newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            dateFinal.append(row[0])
            adUnit.append (row[1])
            advertiser.append (row[2])
            demandChannel.append (row[3])

            impressions.append (row[7])
            adsenseImps.append (row[8])
            adxImps.append (row[10])

            if Count == 0 or row[9] == 0:
                adsenseRev.append (row[9])
            else:
                adsenseRev.append (float(row[9])/1000000)

            if Count == 0 or row[11] == 0:
                adxRev.append (row[11])
            else:
                adxRev.append (float(row[11])/1000000)

            if adUnit[Count][0:3].upper() == 'UHK':
                channel.append('UHK')
            elif adUnit[Count][0:7].upper() == 'UBEAUTY':
                channel.append('UBeauty')
            elif adUnit[Count][0:5].upper() == 'UFOOD':
                channel.append('UFood')
            elif adUnit[Count][0:5].upper() == 'EZONE':
                channel.append('e-zone')
            elif adUnit[Count][0:2].upper() == 'UL':
                channel.append('UL')
            elif adUnit[Count][0:5].upper() == 'UBLOG':
                channel.append('UBlog')
            elif adUnit[Count][0:7].upper() == 'UTRAVEL':
                channel.append('UTravel')
            elif 'INVEST' in adUnit[Count].upper():
                channel.append('Invest')
            elif 'INVESTMENT' in adUnit[Count].upper():
                channel.append('Invest')
            elif 'SME' in adUnit[Count].upper():
                channel.append('SME')
            elif 'PAPER' in adUnit[Count].upper():
                channel.append('paper')
            elif 'PROPERTY' in adUnit[Count].upper():
                channel.append('PS')
            elif 'INEWS' in adUnit[Count].upper():
                channel.append('iNews')
            elif adUnit[Count][0:4].upper() == 'HKET':
                channel.append('HKET')
            elif adUnit[Count][0:3].upper() == 'IET':
                channel.append('HKET')
            elif adUnit[Count][0:6].upper() == 'TOPICK':
                channel.append('TOPick')
            elif adUnit[Count][0:6].upper() == 'iMONEY':
                channel.append('iMoney')
            elif adUnit[Count][0:2].upper() == 'PS':
                channel.append('PS')
            elif adUnit[Count][0:3].upper() == 'SKY':
                channel.append('SkyPost')
            elif adUnit[Count][0:6].upper() == 'IMONEY':
                channel.append('iMoney')
            else:
                channel.append('Others')

            if "DEFAULT" in (row[1]).upper():
                platform.append('Others')
            elif "TEST" in (row[1]).upper():
                platform.append('Others')
            elif "ZENO" in (row[1]).upper():
                platform.append('Others')
            elif "PS2_" in (row[1]).upper():
                platform.append('iET')
            elif "IET" in (row[1]).upper():
                platform.append('iET')
            elif "HKET" in (row[1]).upper():
                platform.append('iET')
            elif "IMONEY" in (row[1]).upper():
                platform.append('iET')
            elif "TOPICK" in (row[1]).upper():
                platform.append('iET')
            elif "SKYPOST" in (row[1]).upper():
                platform.append('SkyPost')
            elif "EZONE" in (row[1]).upper():
                platform.append('e-zone')
            elif "UBEAUTY" in (row[1]).upper():
                platform.append('U Lifestyle')
            elif "UFOOD" in (row[1]).upper():
                platform.append('U Lifestyle')
            elif "UHK" in (row[1]).upper():
                platform.append('U Lifestyle')
            elif "UTRAVEL" in (row[1]).upper():
                platform.append('U Lifestyle')
            elif "UBLOG" in (row[1]).upper():
                platform.append('U Lifestyle')
            elif "UL" in (row[1]).upper():
                platform.append('U Lifestyle')
            else:
                platform.append('Others')

            if Count == 0 or (row[11]+row[9]) == 0:
                totalRev.append('Total Revenue')
            else:
                totalRev.append(float(row[9])/1000000+float(row[11])/1000000)


            if "ADSENSE" in (row[2]).upper():
                advertiserType.append('Google AdSense')
            elif 'EXCHANGE' in (row[2]).upper():
                advertiserType.append('Google Ad Exchange')
            elif 'GOOGLE ADX' in (row[2]).upper():
                advertiserType.append('Google Ad Exchange')
            elif 'INNITY' in (row[2]).upper():
                advertiserType.append('Ad Network')
            elif 'TEADS' in advertiser[Count].upper():
                advertiserType.append('Ad Network')
            elif 'VIDOOMY' in (row[2]).upper():
                advertiserType.append('Ad Network')
            elif 'ANDBEYOND' in (row[2]).upper():
                advertiserType.append('Ad Network')
            elif 'UCFUNNEL' in (row[2]).upper():
                advertiserType.append('Ad Network')
            elif 'GROUPM' in (row[2]).upper():
                advertiserType.append('Demand Partner')
            elif 'PIXELS' in (row[2]).upper():
                advertiserType.append('Demand Partner')
            elif 'TEST' in (row[2]).upper():
                advertiserType.append('Others')
            elif 'TM' in (row[2]).upper():
                advertiserType.append('Others')
            elif 'CAR' in (row[2]).upper():
                advertiserType.append('Others')
            elif 'UAT' in (row[2]).upper():
                advertiserType.append('Others')
            elif 'SPECIAL' in (row[2]).upper():
                advertiserType.append('Others')
            elif 'SKY POST' in (row[2]).upper():
                advertiserType.append('House Ad')
            elif 'U BLOG' in (row[2]).upper():
                advertiserType.append('House Ad')
            elif 'HKET' in (row[2]).upper():
                advertiserType.append('House Ad')
            elif 'U LIFESTYLE' in (row[2]).upper():
                advertiserType.append('House Ad')
            elif 'E-ZONE' in (row[2]).upper():
                advertiserType.append('House Ad')
            elif '-' in (row[2]).upper() and 'EXCHANGE' in (row[3]).upper():
                advertiserType.append('Google Ad Exchange')
            elif '-' in (row[2]).upper() and 'ADSENSE' in (row[3]).upper():
                advertiserType.append('Google AdSense')
            else:
                advertiserType.append('Direct')



            Count = Count + 1


    dateFinal[0] = 'Date'
    adUnit[0] = 'Ad Unit'
    advertiser[0] = 'Advertiser'
    demandChannel[0] = 'demand Channel'
    impressions[0] = 'Total Impressions'
    adsenseImps[0] = 'AdSense Impressinos'
    adsenseRev[0] = 'AdSense Revenue'
    adxImps[0] = 'Ad Exchnage Impressions'
    adxRev[0] = 'Ad Exchange Revenue'
    advertiserType[0] = 'Advertiser Type'
    channel[0] = 'Channel'
    platform[0] = 'Platform'
    totalRev[0] = 'Total Revenue'


    print(Count)

    with open('/home/pamelatsui/Report/WeeklyRevenue/WeeklyRevenue_final.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvfile.truncate()
        for WriteCount in range(0,Count):
            writer.writerow([dateFinal[WriteCount], adUnit[WriteCount], advertiser[WriteCount], demandChannel[WriteCount], impressions[WriteCount],
						 adsenseImps[WriteCount],adsenseRev[WriteCount],adxImps[WriteCount],adxRev[WriteCount],advertiserType[WriteCount],platform[WriteCount],channel[WriteCount],totalRev[WriteCount]])
            WriteCount = WriteCount + 1



    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']


    creds = ServiceAccountCredentials.from_json_keyfile_name("/home/pamelatsui/Report/creds2.json",scope)
    client = gspread.authorize(creds)

    print('Done getting credentials for google sheets')



    sheet = client.open('Weekly Programmatic Revenue Overview').worksheet('Raw')
    d = pd.read_csv('/home/pamelatsui/Report/WeeklyRevenue/WeeklyRevenue_final.csv', skiprows=1)

    all = sheet.get_all_values()
    end_row = len(all) + 1

    print(end_row)


    df = pd.DataFrame(data=d)
    set_with_dataframe(sheet, df,row=end_row, col=1, include_index=False, include_column_header=True,
                       resize=False, allow_formulas=True)


    ################# Unfilled  ####################

    report_job_unfilled = {
        'reportQuery': {
        'dimensions': ['DATE','AD_UNIT_NAME','DEVICE_CATEGORY_NAME'],
        'columns': ['TOTAL_AD_REQUESTS','TOTAL_FILL_RATE','TOTAL_RESPONSES_SERVED','TOTAL_UNMATCHED_AD_REQUESTS','TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS','ADSENSE_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS','AD_EXCHANGE_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS','AD_EXCHANGE_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS','AD_SERVER_ACTIVE_VIEW_VIEWABLE_IMPRESSIONS','AD_SERVER_ACTIVE_VIEW_MEASURABLE_IMPRESSIONS','TOTAL_INVENTORY_LEVEL_UNFILLED_IMPRESSIONS'],
        ###'dateRangeType': 'LAST_WEEK'
        'dateRangeType': 'CUSTOM_DATE',
        'startDate': date.today() - timedelta(days=1),
        'endDate': date.today()- timedelta(days=1)
        }
    }

    try:
        # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(report_job_unfilled)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    with tempfile.NamedTemporaryFile(
        suffix='.csv', mode='wb', delete=False, dir='/home/pamelatsui/Report/WeeklyRevenue/Temp') as report_unfilled:
        filewriter = csv.writer(report_unfilled, delimiter=',')

        # Downloads the response from PQL select statement to the specified file
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_unfilled, use_gzip_compression=False)

    print('Saved line items to... %s' % (report_unfilled.name))



    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']


    creds = ServiceAccountCredentials.from_json_keyfile_name("/home/pamelatsui/Report/creds2.json",scope)
    client = gspread.authorize(creds)

    print('Done getting credentials for google sheets')



    sheet_unfilled = client.open('Fill Rate and Viewability').worksheet('Raw')
    d_unfilled = pd.read_csv(report_unfilled.name, skiprows=1)
    all_unfilled = sheet_unfilled.get_all_values()
    end_row_unfilled = len(all_unfilled) + 1

    print(end_row_unfilled)


    df_unfilled = pd.DataFrame(data=d_unfilled)
    set_with_dataframe(sheet_unfilled, df_unfilled,row=end_row_unfilled, col=1, include_index=False, include_column_header=True,
                       resize=False, allow_formulas=True)



if __name__=="__main__":
    main()

