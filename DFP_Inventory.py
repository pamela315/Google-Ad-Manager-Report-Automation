# Import the library.
import datetime
from googleads import ad_manager
from googleads import errors
from datetime import date, timedelta
from datetime import datetime
import tempfile
import pandas as pd
import pygsheets
import pprint
import _locale
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import csv
import os
from oauth2client.service_account import ServiceAccountCredentials

_locale._getdefaultlocale = (lambda *args: ['en_US', 'UTF-8'])

def main():
    platform = []
    platform_unfilled = []
    channel = []
    channel_unfilled = []
    adUnit = []
    adUnit_unfilled = []
    device = []
    device_unfilled = []
    advertiser = []
    monthYear = []
    monthYear_unfilled = []
    adRequest_unfilled = []
    adType = []
    totalImp = []
    totalImp_unfilled = []
    unfilledImp_unfilled = []
    adServerImp_unfilled = []
    adSenseImp_unfilled = []
    adExchangeImp_unfilled = []
    adExchangeRevenue= []
    adSenseRevenue = []
    appWeb = []
    appWeb_unfilled = []
    count = 0
    count_unfilled = 0
    writeCount = 0
    writeCount_unfilled = 0
    impType = []
    impType_unfilled = []
    today = datetime.now().strftime("%Y-%m-%d")


    ################# Delete all csv in Report Folder ####################
    filelist = [ f for f in os.listdir("/home/pamelatsui/Report/DFP_Inventory/Temp/") if f.endswith(".csv") ]
    for f in filelist:
        os.remove(os.path.join("/home/pamelatsui/Report/DFP_Inventory/Temp/", f))


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


    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=1)


################################# Inventory Distribution #################################

    report_job = {
        'reportQuery': {
        'dimensions': ['MONTH_AND_YEAR','AD_UNIT_NAME','DEVICE_CATEGORY_NAME','ADVERTISER_NAME'],
        'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_REVENUE','AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE'],
        'dateRangeType': 'LAST_3_MONTHS',
        }
    }

    try:
        # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(report_job)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    with tempfile.NamedTemporaryFile(
        suffix='.csv', mode='wb', delete=False, dir='/home/pamelatsui/Report/DFP_Inventory/Temp/') as report:
        filewriter = csv.writer(report, delimiter=',')

        # Downloads the response from PQL select statement to the specified file
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report, use_gzip_compression=False)

    print('Saved line items to... %s' % (report.name))


    with open(report.name , newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            monthYear.append(row[0])
            adUnit.append(row[1])
            advertiser.append(row[3])
            totalImp.append(row[7])
            if count == 0:
                adSenseRevenue.append(row[8])
                adExchangeRevenue.append(row[9])
            else:
                adSenseRevenue.append(float(row[8])/1000000)
                adExchangeRevenue.append(float(row[9])/1000000)


            if "DESKTOP" in (row[2]).upper():
                device.append('Desktop')
            else:
                device.append('Mobile')

            if "LIGHTBOX" in (row[1]).upper():
                impType.append('Album')
            else:
                impType.append('General')

            if "DEFAULT" in (row[1]).upper():
                channel.append('exclude')
            elif "TEST" in (row[1]).upper():
                channel.append('exclude')
            elif "PAPER" in (row[1]).upper():
                channel.append('Paper')
            elif "PS2_" in (row[1]).upper():
                channel.append('Paper')
            elif "ZENO" in (row[1]).upper():
                channel.append('exclude')
            elif "SME" in (row[1]).upper():
                channel.append('SME')
            elif "PROPERTY" in (row[1]).upper():
                channel.append('PS')
            elif "KNOWLEDGE" in (row[1]).upper():
                channel.append('Knowledge')
            elif "WEALTH" in (row[1]).upper():
                channel.append('Wealth')
            elif "IMONEY" in (row[1]).upper():
                channel.append('iMoney')
            elif "TOPICK" in (row[1]).upper():
                channel.append('TOPick')
            elif "INVEST" in (row[1]).upper():
                channel.append('Invest')
            elif "INEWS" in (row[1]).upper():
                channel.append('iNews')
            elif "CHINA" in (row[1]).upper():
                channel.append('China')
            elif "HKET" in (row[1]).upper():
                channel.append('iET')
            elif "IET" in (row[1]).upper():
                channel.append('iET')
            elif "SKYPOST" in (row[1]).upper():
                channel.append('SkyPost')
            elif "EZONE" in (row[1]).upper():
                channel.append('e-zone')
            elif "UBEAUTY" in (row[1]).upper():
                channel.append('UBeauty')
            elif "UFOOD" in (row[1]).upper():
                channel.append('UFood')
            elif "UHK" in (row[1]).upper():
                channel.append('UHK')
            elif "UTRAVEL" in (row[1]).upper():
                channel.append('UTravel')
            elif "UBLOG" in (row[1]).upper():
                channel.append('UBlog')
            elif "MALLKING" in (row[1]).upper():
                channel.append('Mall King')
            elif "UL" in (row[1]).upper():
                channel.append('U Lifestyle')
            elif "CT_" in (row[1]).upper():
                channel.append('CT')
            else:
                channel.append('Others')

            if "DEFAULT" in (row[1]).upper():
                platform.append('exclude')
            elif "TEST" in (row[1]).upper():
                platform.append('exclude')
            elif "PS2_" in (row[1]).upper():
                platform.append('iET')
            elif "ZENO" in (row[1]).upper():
                platform.append('exclude')
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
                platform.append('U Lifestyle')
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
            elif "MALLKING" in (row[1]).upper():
                platform.append('U Lifestyle')
            elif "UL" in (row[1]).upper():
                platform.append('U Lifestyle')
            elif "CT_" in (row[1]).upper():
                platform.append('CT')
            else:
                platform.append('Others')


            if "APP" in (row[1]).upper() and "PS2_" in (row[1]).upper():
                appWeb.append('PS App')
            elif "APP" in (row[1]).upper() and "IET" in (row[1]).upper():
                appWeb.append('ET App')
            elif "APP" in (row[1]).upper() and "IMONEY" in (row[1]).upper():
                appWeb.append('iMoney App')
            elif "APP" in (row[1]).upper() and "UL_" in (row[1]).upper():
                appWeb.append('UL App')
            elif "APP" in (row[1]).upper() and "SKYPOST" in (row[1]).upper():
                appWeb.append('SkyPost App')
            elif "WEB" in (row[1]).upper() :
                appWeb.append('Web')
            elif "DEFAULT" in (row[1]).upper():
                appWeb.append('exclude')
            elif "TEST" in (row[1]).upper():
                appWeb.append('exclude')
            elif "ZENO" in (row[1]).upper():
                appWeb.append('exclude')
            else:
                appWeb.append('Others')

            if "TEST" in (row[3]).upper():
                adType.append('exclude')
            elif "AD EXCHANGE" in (row[3]).upper():
                adType.append('AdX')
            elif "ADX" in (row[3]).upper():
                adType.append('AdX')
            elif "ADSENSE" in (row[3]).upper():
                adType.append('AdSense')
            elif "IMONEY" in (row[3]).upper():
                adType.append('House Ad')
            elif "SKY POST" in (row[3]).upper():
                adType.append('House Ad')
            elif "HKET" in (row[3]).upper():
                adType.append('House Ad')
            elif "U LIFESTYLE" in (row[3]).upper():
                adType.append('House Ad')
            elif "TOPICK" in (row[3]).upper():
                adType.append('House Ad')
            elif "E-ZONE" in (row[3]).upper():
                adType.append('House Ad')
            elif "UCFUNNEL" in (row[3]).upper():
                adType.append('Ad Network')
            elif "APPIER" in (row[3]).upper():
                adType.append('Ad Network')
            elif "ANDBEYOND MEDIA" in (row[3]).upper():
                adType.append('Ad Network')
            elif "INNITY" in (row[3]).upper():
                adType.append('Ad Network')
            elif "Vidoomy" in (row[3]).upper():
                adType.append('Ad Network')
            elif row[3] == '-':
                adType.append('AdSense')
            else:
                adType.append('Direct')


            count = count + 1

    platform[0] = 'Platform'
    channel[0] = 'channel'
    adType[0] = 'adType'
    appWeb[0] = 'appWeb'
    device[0] = 'Device Cat'
    impType[0] = 'Impression Type'


    with open('/home/pamelatsui/Report/DFP_Inventory/DFP_Inventory_Final.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvfile.truncate()
        for writeCount in range(0,count):
            writer.writerow([monthYear[writeCount],adUnit[writeCount],device[writeCount],advertiser[writeCount],totalImp[writeCount],channel[writeCount],platform[writeCount],appWeb[writeCount],adType[writeCount],impType[writeCount],adSenseRevenue[writeCount],adExchangeRevenue[writeCount]])
            writeCount = writeCount + 1


    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']


    creds = ServiceAccountCredentials.from_json_keyfile_name("/home/pamelatsui/Report/creds2.json",scope)
    client = gspread.authorize(creds)

    print('Done getting credentials for google sheets')



    sheet = client.open('DFP Inventory').worksheet('Raw')
    d = pd.read_csv('/home/pamelatsui/Report/DFP_Inventory/DFP_Inventory_Final.csv',skiprows=1)

    all = sheet.get_all_values()
    end_row = len(all) + 1

    print(end_row)


    df = pd.DataFrame(data=d)
    set_with_dataframe(sheet, df,row=end_row, col=1, include_index=False, include_column_header=True,
                       resize=False, allow_formulas=True)

################################# Unfilled #################################

    unfilled_report_job = {
        'reportQuery': {
        'dimensions': ['MONTH_AND_YEAR','AD_UNIT_NAME','DEVICE_CATEGORY_NAME'],
        'columns': ['TOTAL_AD_REQUESTS','TOTAL_INVENTORY_LEVEL_UNFILLED_IMPRESSIONS','TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS','AD_SERVER_IMPRESSIONS','AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS'],
        'dateRangeType': 'LAST_3_MONTHS',
        }
    }

    try:
        # Run the report and wait for it to finish.
        report_job_id_unfilled = report_downloader.WaitForReport(unfilled_report_job)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    with tempfile.NamedTemporaryFile(
        suffix='.csv', mode='wb', delete=False, dir='/home/pamelatsui/Report/DFP_Inventory/Temp/') as unfilled_report:
        filewriter = csv.writer(unfilled_report, delimiter=',')

        # Downloads the response from PQL select statement to the specified file
        report_downloader.DownloadReportToFile(report_job_id_unfilled, 'CSV_DUMP', unfilled_report, use_gzip_compression=False)

    print('Saved line items to... %s' % (unfilled_report.name))


    with open(unfilled_report.name , newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            monthYear_unfilled.append (row[0])
            adUnit_unfilled.append (row[1])
            adRequest_unfilled.append(row[5])
            unfilledImp_unfilled.append(row[6])
            totalImp_unfilled.append(row[7])
            adServerImp_unfilled.append(row[8])
            adExchangeImp_unfilled.append(row[9])
            adSenseImp_unfilled.append(row[10])


            if "DESKTOP" in (row[2]).upper():
                device_unfilled.append('Desktop')
            else:
                device_unfilled.append('Mobile')

            if "LIGHTBOX" in (row[1]).upper():
                impType_unfilled.append('Album')
            else:
                impType_unfilled.append('General')

            if "DEFAULT" in (row[1]).upper():
                channel_unfilled.append('exclude')
            elif "TEST" in (row[1]).upper():
                channel_unfilled.append('exclude')
            elif "PAPER" in (row[1]).upper():
                channel_unfilled.append('Paper')
            elif "PS2_" in (row[1]).upper():
                channel_unfilled.append('Paper')
            elif "ZENO" in (row[1]).upper():
                channel_unfilled.append('exclude')
            elif "SME" in (row[1]).upper():
                channel_unfilled.append('SME')
            elif "PROPERTY" in (row[1]).upper():
                channel_unfilled.append('PS')
            elif "KNOWLEDGE" in (row[1]).upper():
                channel_unfilled.append('Knowledge')
            elif "WEALTH" in (row[1]).upper():
                channel_unfilled.append('Wealth')
            elif "IMONEY" in (row[1]).upper():
                channel_unfilled.append('iMoney')
            elif "TOPICK" in (row[1]).upper():
                channel_unfilled.append('TOPick')
            elif "INVEST" in (row[1]).upper():
                channel_unfilled.append('Invest')
            elif "INEWS" in (row[1]).upper():
                channel_unfilled.append('iNews')
            elif "CHINA" in (row[1]).upper():
                channel_unfilled.append('China')
            elif "HKET" in (row[1]).upper():
                channel_unfilled.append('iET')
            elif "IET" in (row[1]).upper():
                channel_unfilled.append('iET')
            elif "SKYPOST" in (row[1]).upper():
                channel_unfilled.append('SkyPost')
            elif "EZONE" in (row[1]).upper():
                channel_unfilled.append('e-zone')
            elif "UBEAUTY" in (row[1]).upper():
                channel_unfilled.append('UBeauty')
            elif "UFOOD" in (row[1]).upper():
                channel_unfilled.append('UFood')
            elif "UHK" in (row[1]).upper():
                channel_unfilled.append('UHK')
            elif "UTRAVEL" in (row[1]).upper():
                channel_unfilled.append('UTravel')
            elif "UBLOG" in (row[1]).upper():
                channel_unfilled.append('UBlog')
            elif "MALLKING" in (row[1]).upper():
                channel_unfilled.append('Mall King')
            elif "UL" in (row[1]).upper():
                channel_unfilled.append('U Lifestyle')
            elif "CT_" in (row[1]).upper():
                channel_unfilled.append('CT')
            else:
                channel_unfilled.append('Others')

            if "DEFAULT" in (row[1]).upper():
                platform_unfilled.append('exclude')
            elif "TEST" in (row[1]).upper():
                platform_unfilled.append('exclude')
            elif "PS2_" in (row[1]).upper():
                platform_unfilled.append('iET')
            elif "ZENO" in (row[1]).upper():
                platform_unfilled.append('exclude')
            elif "IET" in (row[1]).upper():
                platform_unfilled.append('iET')
            elif "HKET" in (row[1]).upper():
                platform_unfilled.append('iET')
            elif "IMONEY" in (row[1]).upper():
                platform_unfilled.append('iET')
            elif "TOPICK" in (row[1]).upper():
                platform_unfilled.append('iET')
            elif "SKYPOST" in (row[1]).upper():
                platform_unfilled.append('SkyPost')
            elif "EZONE" in (row[1]).upper():
                platform_unfilled.append('U Lifestyle')
            elif "UBEAUTY" in (row[1]).upper():
                platform_unfilled.append('U Lifestyle')
            elif "UFOOD" in (row[1]).upper():
                platform_unfilled.append('U Lifestyle')
            elif "UHK" in (row[1]).upper():
                platform_unfilled.append('U Lifestyle')
            elif "UTRAVEL" in (row[1]).upper():
                platform_unfilled.append('U Lifestyle')
            elif "UBLOG" in (row[1]).upper():
                platform_unfilled.append('U Lifestyle')
            elif "MALLKING" in (row[1]).upper():
                platform_unfilled.append('U Lifestyle')
            elif "UL" in (row[1]).upper():
                platform_unfilled.append('U Lifestyle')
            elif "CT_" in (row[1]).upper():
                platform_unfilled.append('CT')
            else:
                platform_unfilled.append('Others')


            if "APP" in (row[1]).upper() and "PS2_" in (row[1]).upper():
                appWeb_unfilled.append('PS App')
            elif "APP" in (row[1]).upper() and "IET" in (row[1]).upper():
                appWeb_unfilled.append('ET App')
            elif "APP" in (row[1]).upper() and "IMONEY" in (row[1]).upper():
                appWeb_unfilled.append('iMoney App')
            elif "APP" in (row[1]).upper() and "UL_" in (row[1]).upper():
                appWeb_unfilled.append('UL App')
            elif "APP" in (row[1]).upper() and "SKYPOST" in (row[1]).upper():
                appWeb_unfilled.append('SkyPost App')
            elif "WEB" in (row[1]).upper() :
                appWeb_unfilled.append('Web')
            elif "DEFAULT" in (row[1]).upper():
                appWeb_unfilled.append('exclude')
            elif "TEST" in (row[1]).upper():
                appWeb_unfilled.append('exclude')
            elif "ZENO" in (row[1]).upper():
                appWeb_unfilled.append('exclude')
            else:
                appWeb_unfilled.append('Others')


            count_unfilled = count_unfilled + 1

    platform_unfilled[0] = 'Platform'
    channel_unfilled[0] = 'channel'
    appWeb_unfilled[0] = 'appWeb'
    device_unfilled[0] = 'Device Cat'
    impType_unfilled[0] = 'Impression Type'



    with open('/home/pamelatsui/Report/DFP_Inventory/DFP_Inventory_Unfilled_Final.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvfile.truncate()
        for writeCount_unfilled in range(0,count_unfilled):
            writer.writerow([monthYear_unfilled[writeCount_unfilled],adUnit_unfilled[writeCount_unfilled],device_unfilled[writeCount_unfilled],adRequest_unfilled[writeCount_unfilled],unfilledImp_unfilled[writeCount_unfilled],totalImp_unfilled[writeCount_unfilled],adServerImp_unfilled[writeCount_unfilled],adExchangeImp_unfilled[writeCount_unfilled],adSenseImp_unfilled[writeCount_unfilled],channel_unfilled[writeCount_unfilled],platform_unfilled[writeCount_unfilled],appWeb_unfilled[writeCount_unfilled],impType_unfilled[writeCount_unfilled]])
            writeCount_unfilled = writeCount_unfilled + 1


    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']


    creds = ServiceAccountCredentials.from_json_keyfile_name("/home/pamelatsui/Report/creds2.json",scope)
    client = gspread.authorize(creds)

    print('Done getting credentials for google sheets')



    sheet = client.open('DFP Inventory').worksheet('Unfilled')
    d2 = pd.read_csv('/home/pamelatsui/Report/DFP_Inventory/DFP_Inventory_Unfilled_Final.csv',skiprows=1)

    all2 = sheet.get_all_values()
    end_row2 = len(all2) + 1

    print(end_row)


    df2 = pd.DataFrame(data=d2)
    set_with_dataframe(sheet, df2,row=end_row2, col=1, include_index=False, include_column_header=True,
                       resize=False, allow_formulas=True)



if __name__=="__main__":
    main()

