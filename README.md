# Google Ad Manager Revuenue Report Automation
Duration: Oct 2019 to Feb 2020

DFP Version: v201911 

The "Google Ad Manager Revenue Report Automation" project is a software tool initiated by a client company. It is designed to automate the generation and retrieval of cost and impression data from Google Ad Manager and raw cost data from various ad network. Google Ad Manager is a comprehensive ad buying platform that enables publishers to manage and optimize their advertising inventory.

This project aims to streamline the revenue reporting process by automating the extraction of revenue data from Google Ad Manager and generating insightful reports in a user-friendly format. By leveraging the Google Ad Manager API, the tool eliminates the need for manual data extraction and manipulation, saving time and effort for publishers and ad operations teams.

The ETL script is to call the Google Ad Manager API data, transform the result and finally push them to Google Sheets workbook. The function I use in Google Ad Manager API is the report service and line item service, under which impressions, clicks and CTR data are pulled out.

## Key Features:

- **Automated Data Retrieval:** It pulls the necessary metrics and dimensions to generate revenue reports. 

- **Customizable Reporting:** The tool provides flexibility in generating revenue reports by allowing users to customize the metrics, dimensions, and time periods they want to include in Looker Studio. 

- **Scheduled Reporting:** The tool project supports scheduled reporting, allowing users to set up automated report generation at specific intervals (daily, weekly, monthly, etc.) by . 

- **Exporting Options:** Generated revenue reports can be exported in multiple formats, such as PDF, Excel, or CSV, making it easy to share and distribute the reports across teams or stakeholders.

## Content
1. [Automation Trial](dfp_automation.py): Trial report query
   ```
   'reportQuery': {
    'dimensions':['ORDER_NAME','CUSTOM_CRITERIA','ORDER_ID'],
    'columns':['AD_SERVER_IMPRESSIONS', 'AD_SERVER_CLICKS'],
    'dateRangeType': 'CUSTOM_DATE',
    'startDate': datetime.date(2019,10,28),
    'endDate': date.today()
    }

3. [Ads Format Performance](/AdsFormatPerformance.py)
4. [DFP daily Performance](/DFP_Daily_Performance.py)
5. [DFP Weekly Performance](/WeeklyRevenue.py)
6. [Ad Network Performance](/ad_network_performance.py)

---
License: [CC-BY](https://creativecommons.org/licenses/by/3.0/)
