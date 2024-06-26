# CS02516_FINALPROJECT - New York City Crime Data Loading and Analysis
# Louis Lagonik

New York City Crime Data Analysis

For this project I obtained a New York Police Department complaint dataset that contains 555,117 complaint records
with 34 columns providing a variety of data include complain dates, complain descriptive information, police response
information as well as geographic information.  The data source was obtained from.
https://data.cityofnewyork.us/Public-Safety/NYPD-Complaint-Data-Current-Year-To-Date-/5uac-w243/about_data

The data source included both an API which significantly limits the number of records that can be queried as well
as a CSV file containing the full data set.  The dataset was downloaded to local storage and was over 200 MB.

A data dictionary was also provided for the data set:
https://data.cityofnewyork.us/api/views/5uac-w243/files/e5a74f73-983a-473b-a4f2-0f5ef6fbee08?download=true&filename=NYPD_Complaint_YTD_DataDictionary.xlsx

NewYorkCityCrimeDataLoader.py is used to create a connection to the Elasticsearch installation and creates an index for the
complaint data.  It procedes to create a mapping data structure for all of the fields to be loaded into the index.
The NYPD complaint dataset is loaded from the downloaded CSV and rows are read one by one and loaded into the index.
The majority of fields are loaded into elastic search as text with the exception of the complaint date and report date
which were loaded as dates and the geographic location which was loaded as geo_point.

main.py is used for creating the NewYorkCityCrimeDataLoader class and clearing the elastic search index, creating a new one
and calling teh function to load the data.

Analysis of the data was performed using Kibana's web interface.