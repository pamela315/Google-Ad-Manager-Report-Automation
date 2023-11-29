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
    adUnit = []
    dateFinal = []
    adRequest = []
    matchRequest = []
    impressions = []
    revenue = []
    eCPM = []
    count  = 0
    writeCount = 0


    ################# Delete all csv in Report Folder ####################
    filelist = [ f for f in os.listdir("/home/pamelatsui/Report/DFP_Daily_Performance/Temp") if f.endswith(".csv") ]
    for f in filelist:
        os.remove(os.path.join("/home/pamelatsui/Report/DFP_Daily_Performance/Temp", f))

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
        'dimensions': ['AD_EXCHANGE_DATE','AD_EXCHANGE_DFP_AD_UNIT'],
        'columns': ['AD_EXCHANGE_AD_REQUESTS','AD_EXCHANGE_MATCHED_REQUESTS', 'AD_EXCHANGE_IMPRESSIONS', 'AD_EXCHANGE_ESTIMATED_REVENUE', 'AD_EXCHANGE_AD_ECPM'],
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
        suffix='.csv', mode='wb', delete=False, dir='/home/pamelatsui/Report/DFP_Daily_Performance/Temp') as report_ad_exchange:
        filewriter = csv.writer(report_ad_exchange, delimiter=',')

        # Downloads the response from PQL select statement to the specified file
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_ad_exchange, use_gzip_compression=False)

    print('Saved line items to... %s' % (report_ad_exchange.name))


    with open(report_ad_exchange.name , newline='', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            dateFinal.append (row[0])
            adUnit.append (row[1].replace('183518426 » ',''))
            adRequest.append(row[2])
            matchRequest.append(row[3])
            impressions.append(row[4])

            if count == 0:
                revenue.append(row[5])
            else:
                revenue.append((float(row[5]))/1000000)

            if count == 0:
                eCPM.append(row[6])
            else:
                eCPM.append((float(row[6]))/1000000)


            if "PAPER" in (row[1].replace('183518426 » ','')).upper():
                channel.append('Paper')
            elif "SME" in (row[1].replace('183518426 » ','')).upper():
                channel.append('SME')
            elif "PROPERTY" in (row[1].replace('183518426 » ','')).upper():
                channel.append('PS')
            elif "KNOWLEDGE" in (row[1].replace('183518426 » ','')).upper():
                channel.append('Knowledge')
            elif "WEALTH" in (row[1].replace('183518426 » ','')).upper():
                channel.append('Wealth')
            elif "IMONEY" in (row[1].replace('183518426 » ','')).upper():
                channel.append('iMoney')
            elif "TOPICK" in (row[1].replace('183518426 » ','')).upper():
                channel.append('TOPick')
            elif "INVEST" in (row[1].replace('183518426 » ','')).upper():
                channel.append('Invest')
            elif "INEWS" in (row[1].replace('183518426 » ','')).upper():
                channel.append('iNews')
            elif "CHINA" in (row[1].replace('183518426 » ','')).upper():
                channel.append('China')
            elif "HKET" in (row[1].replace('183518426 » ','')).upper():
                channel.append('iET')
            elif "IET" in (row[1].replace('183518426 » ','')).upper():
                channel.append('iET')
            elif "SKYPOST" in (row[1].replace('183518426 » ','')).upper():
                channel.append('SkyPost')
            elif "EZONE" in (row[1].replace('183518426 » ','')).upper():
                channel.append('e-zone')
            elif "UBEAUTY" in (row[1].replace('183518426 » ','')).upper():
                channel.append('UBeauty')
            elif "UFOOD" in (row[1].replace('183518426 » ','')).upper():
                channel.append('UFood')
            elif "UHK" in (row[1].replace('183518426 » ','')).upper():
                channel.append('UHK')
            elif "UTRAVEL" in (row[1].replace('183518426 » ','')).upper():
                channel.append('UTravel')
            elif "UBLOG" in (row[1].replace('183518426 » ','')).upper():
                channel.append('UBlog')
            elif "MALLKING" in (row[1].replace('183518426 » ','')).upper():
                channel.append('Mall King')
            elif "UL" in (row[1].replace('183518426 » ','')).upper():
                channel.append('U Lifestyle')
            elif "CT_" in (row[1].replace('183518426 » ','')).upper():
                channel.append('CT')
            else:
                channel.append('Others')


            if "IET" in (row[1].replace('183518426 » ','')).upper():
                platform.append('iET')
            elif "HKET" in (row[1].replace('183518426 » ','')).upper():
                platform.append('iET')
            elif "IMONEY" in (row[1].replace('183518426 » ','')).upper():
                platform.append('iET')
            elif "TOPICK" in (row[1].replace('183518426 » ','')).upper():
                platform.append('TOPick')
            elif "SKYPOST" in (row[1].replace('183518426 » ','')).upper():
                platform.append('SkyPost')
            elif "EZONE" in (row[1].replace('183518426 » ','')).upper():
                platform.append('e-zone')
            elif "UBEAUTY" in (row[1].replace('183518426 » ','')).upper():
                platform.append('U Lifestyle')
            elif "UFOOD" in (row[1].replace('183518426 » ','')).upper():
                platform.append('U Lifestyle')
            elif "UHK" in (row[1].replace('183518426 » ','')).upper():
                platform.append('U Lifestyle')
            elif "UTRAVEL" in (row[1].replace('183518426 » ','')).upper():
                platform.append('U Lifestyle')
            elif "UBLOG" in (row[1].replace('183518426 » ','')).upper():
                platform.append('U Lifestyle')
            elif "MALLKING" in (row[1].replace('183518426 » ','')).upper():
                platform.append('U Lifestyle')
            elif "UL" in (row[1].replace('183518426 » ','')).upper():
                platform.append('U Lifestyle')
            elif "CT_" in (row[1].replace('183518426 » ','')).upper():
                platform.append('CT')
            else:
                platform.append('Others')

            count = count + 1


    with open('/home/pamelatsui/Report/DFP_Daily_Performance/AdExchangeFinal.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csvfile.truncate()
        for writeCount in range(0,count):
            writer.writerow([dateFinal[writeCount],adUnit[writeCount],adRequest[writeCount],matchRequest[writeCount],impressions[writeCount],revenue[writeCount],eCPM[writeCount],channel[writeCount],platform[writeCount]])
            writeCount = writeCount + 1



    scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']


    creds = ServiceAccountCredentials.from_json_keyfile_name("/home/pamelatsui/Report/creds2.json",scope)
    client = gspread.authorize(creds)

    print('Done getting credentials for google sheets')



    sheet = client.open('DFP Daily Performance').worksheet('Ad Exchange')
    d = pd.read_csv('/home/pamelatsui/Report/DFP_Daily_Performance/AdExchangeFinal.csv', skiprows=1)

    all = sheet.get_all_values()
    end_row = len(all) + 1

    print(end_row)


    df = pd.DataFrame(data=d)
    set_with_dataframe(sheet, df,row=end_row, col=1, include_index=False, include_column_header=True,
                       resize=False, allow_formulas=True)




############################################## SkyPost ##############################################


    ################## Fetching Raw Data #####################
    # Create statement object to filter for an order.
    statement_sp = (ad_manager.StatementBuilder(version='v201911')
                   .Where('AD_UNIT_NAME LIKE: name')
                   .WithBindVariable('name', '%SkyPost%')
                   .Limit(None)  # No limit or offset for reports
                   .Offset(None)
                   )

    SP_job = {
        'reportQuery': {
        'dimensions':['DATE','ADVERTISER_NAME'],
        'statement': statement_sp.ToStatement(),
        'columns':[ 'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_REVENUE','ADSENSE_LINE_ITEM_LEVEL_CLICKS','AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS','AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE','AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS'],
        'startDate': date.today() - timedelta(days=1),
        'endDate': date.today()- timedelta(days=1)
        }
    }

    try:
        # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(SP_job)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    with tempfile.NamedTemporaryFile(
        suffix='.csv', mode='wb', delete=False, dir='/home/pamelatsui/Report/DFP_Daily_Performance/Temp') as SP_job:
        filewriter = csv.writer(SP_job, delimiter=',')

        # Downloads the response from PQL select statement to the specified file
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', SP_job, use_gzip_compression=False)

    print('Saved line items to... %s' % (SP_job.name))


    sheet_sp = client.open('DFP Daily Performance').worksheet('SkyPost')
    d_sp = pd.read_csv(SP_job.name, skiprows=1)

    all_sp = sheet_sp.get_all_values()
    end_row_sp = len(all_sp) + 1

    print(end_row_sp)

    df_sp = pd.DataFrame(data=d_sp)
    set_with_dataframe(sheet_sp, df_sp , row=end_row_sp, col=1, include_index=False, include_column_header=True,
                       resize=False, allow_formulas=True)

############################################## HKET ##############################################


    ################## Fetching Raw Data #####################
    # Create statement object to filter for an order.
    statement_et = (ad_manager.StatementBuilder(version='v201911')
                   .Where('AD_UNIT_NAME LIKE: name')
                   .WithBindVariable('name', '%HKET%')
                   .Limit(None)  # No limit or offset for reports
                   .Offset(None)
                   )

    ET_job = {
        'reportQuery': {
        'dimensions':['DATE','ADVERTISER_NAME','AD_UNIT_NAME'],
        'statement': statement_et.ToStatement(),
        'columns':[ 'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_REVENUE','ADSENSE_LINE_ITEM_LEVEL_CLICKS','AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS','AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE','AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS'],
        'startDate': date.today() - timedelta(days=1),
        'endDate': date.today()- timedelta(days=1)
        }
    }

    try:
        # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(ET_job)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    with tempfile.NamedTemporaryFile(
        suffix='.csv', mode='wb', delete=False, dir='/home/pamelatsui/Report/DFP_Daily_Performance/Temp') as ET_job:
        filewriter = csv.writer(ET_job, delimiter=',')

        # Downloads the response from PQL select statement to the specified file
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', ET_job, use_gzip_compression=False)

    print('Saved line items to... %s' % (ET_job.name))


    sheet_et = client.open('DFP Daily Performance').worksheet('iET')
    d_et = pd.read_csv(ET_job.name, skiprows=1)

    all_et = sheet_et.get_all_values()
    end_row_et = len(all_et) + 1

    print(end_row_et)

    df_et = pd.DataFrame(data=d_et)
    set_with_dataframe(sheet_et, df_et , row=end_row_et, col=1, include_index=False, include_column_header=True,
                       resize=False, allow_formulas=True)


    ################## iET #####################
    # Create statement object to filter for an order.
    statement_et = (ad_manager.StatementBuilder(version='v201911')
                   .Where('AD_UNIT_NAME LIKE: name')
                   .WithBindVariable('name', '%iET%')
                   .Limit(None)  # No limit or offset for reports
                   .Offset(None)
                   )

    ET_job = {
        'reportQuery': {
        'dimensions':['DATE','ADVERTISER_NAME','AD_UNIT_NAME'],
        'statement': statement_et.ToStatement(),
        'columns':[ 'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_REVENUE','ADSENSE_LINE_ITEM_LEVEL_CLICKS','AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS','AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE','AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS'],
        'startDate': date.today() - timedelta(days=1),
        'endDate': date.today()- timedelta(days=1)
        }
    }

    try:
        # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(ET_job)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    with tempfile.NamedTemporaryFile(
        suffix='.csv', mode='wb', delete=False, dir='/home/pamelatsui/Report/DFP_Daily_Performance/Temp') as ET_job:
        filewriter = csv.writer(ET_job, delimiter=',')

        # Downloads the response from PQL select statement to the specified file
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', ET_job, use_gzip_compression=False)

    print('Saved line items to... %s' % (ET_job.name))


    sheet_et = client.open('DFP Daily Performance').worksheet('iET')
    d_et = pd.read_csv(ET_job.name, skiprows=1)

    all_et = sheet_et.get_all_values()
    end_row_et = len(all_et) + 1

    print(end_row_et)

    df_et = pd.DataFrame(data=d_et)
    set_with_dataframe(sheet_et, df_et , row=end_row_et, col=1, include_index=False, include_column_header=True,
                       resize=False, allow_formulas=True)

    ################## iMoney #####################
    # Create statement object to filter for an order.
    statement_et = (ad_manager.StatementBuilder(version='v201911')
                   .Where('AD_UNIT_NAME LIKE: name')
                   .WithBindVariable('name', '%iMoney%')
                   .Limit(None)  # No limit or offset for reports
                   .Offset(None)
                   )

    ET_job = {
        'reportQuery': {
        'dimensions':['DATE','ADVERTISER_NAME','AD_UNIT_NAME'],
        'statement': statement_et.ToStatement(),
        'columns':[ 'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS','ADSENSE_LINE_ITEM_LEVEL_REVENUE','ADSENSE_LINE_ITEM_LEVEL_CLICKS','AD_EXCHANGE_LINE_ITEM_LEVEL_IMPRESSIONS','AD_EXCHANGE_LINE_ITEM_LEVEL_REVENUE','AD_EXCHANGE_LINE_ITEM_LEVEL_CLICKS'],
        'startDate': date.today() - timedelta(days=1),
        'endDate': date.today()- timedelta(days=1)
        }
    }

    try:
        # Run the report and wait for it to finish.
        report_job_id = report_downloader.WaitForReport(ET_job)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    with tempfile.NamedTemporaryFile(
        suffix='.csv', mode='wb', delete=False, dir='/home/pamelatsui/Report/DFP_Daily_Performance/Temp') as ET_job:
        filewriter = csv.writer(ET_job, delimiter=',')

        # Downloads the response from PQL select statement to the specified file
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', ET_job, use_gzip_compression=False)

    print('Saved line items to... %s' % (ET_job.name))


    sheet_et = client.open('DFP Daily Performance').worksheet('iET')
    d_et = pd.read_csv(ET_job.name, skiprows=1)

    all_et = sheet_et.get_all_values()
    end_row_et = len(all_et) + 1

    print(end_row_et)

    df_et = pd.DataFrame(data=d_et)
    set_with_dataframe(sheet_et, df_et , row=end_row_et, col=1, include_index=False, include_column_header=True,
                       resize=False, allow_formulas=True)

if __name__=="__main__":
    main()

