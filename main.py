from NewYorkCityCrimeDataLoader import NewYorkCityCrimeDataLoader

def main():
    print("Running CS02516 FINAL PROJECT - New York City Crime Data Loader")

    newYorkCityCrimeDataLoader = NewYorkCityCrimeDataLoader()
    newYorkCityCrimeDataLoader.deleteNYPDCrimeDataIndex()
    newYorkCityCrimeDataLoader.createNYPDCrimeDataIndex()
    newYorkCityCrimeDataLoader.loadNYPDCrimeDataIntoElasticSearch("NYPD_Complaint_Data_Current__Year_To_Date__20240421.csv")

    print("New York City Crime Data has been loaded, analysis to be performed in Kibana")

if __name__ == "__main__":
    main()

