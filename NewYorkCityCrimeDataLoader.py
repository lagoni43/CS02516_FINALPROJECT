from elasticsearch import Elasticsearch
import csv
import yaml

class NewYorkCityCrimeDataLoader:
    '''
    NewYorkCityCrimeDataLoader.py is used to create a connection to the Elasticsearch installation and creates an index for the
    complaint data.  It procedes to create a mapping data structure for all of the fields to be loaded into the index.
    The NYPD complaint dataset is loaded from the downloaded CSV and rows are read one by one and loaded into the index.
    The majority of fields are loaded into elastic search as text with the exception of the complaint date and report date
    which were loaded as dates and the geographic location which was loaded as geo_point.
    '''

    def __init__(self):
        '''
        Constructor that initializes the data loader by reading in elastic search
        connectivity parameters from config.yaml and creating the connection to be 
        used for loading the data into elastic search.
        '''

        config = None

        with open("config.yaml", "r") as file:
            config = yaml.safe_load(file)


        # Create the client instance
        self.es = Elasticsearch(
            config["elastic"]["host"]+":"+str(config["elastic"]["port"]),
            ca_certs=config["elastic"]["ca_certs"],
            basic_auth=(config["elastic"]["user"], config["elastic"]["password"])
        )

        self.esindex = config["elastic"]["index"]

        print(self.es.info())

    def createNYPDCrimeDataIndex(self):
        '''
        This uses the connection to elastic search to create the index
        for storing the crime data.  The mappings are defined and used
        for index creation.
        '''

        mappings = {
        "properties": {
            "CMPLNT_NUM": {"type": "text"},
            "ADDR_PCT_CD": {"type": "text"},
            "BORO_NM": {"type": "text"},
            "CMPLNT_FR_DT": {"type": "date","format": "MM/dd/yyyy"},
            "CMPLNT_FR_TM": {"type": "text"},
            "CRM_ATPT_CPTD_CD": {"type": "text"},
            "HADEVELOPT": {"type": "text"},
            "HOUSING_PSA": {"type": "text"},
            "JURISDICTION_CODE": {"type": "text"},
            "JURIS_DESC": {"type": "text"},
            "KY_CD": {"type": "text"},
            "LAW_CAT_CD": {"type": "text"},
            "LOC_OF_OCCUR_DESC": {"type": "text"},
            "OFNS_DESC": {"type": "text"},
            "PARKS_NM": {"type": "text"},
            "PATROL_BORO": {"type": "text"},
            "PD_CD": {"type": "text"},
            "PD_DESC": {"type": "text"},
            "PREM_TYP_DESC": {"type": "text"},
            "RPT_DT": {"type": "date","format": "MM/dd/yyyy"},
            "STATION_NAME": {"type": "text"},
            "SUSP_AGE_GROUP": {"type": "text"},
            "SUSP_RACE": {"type": "text"},
            "SUSP_SEX": {"type": "text"},
            "TRANSIT_DISTRICT": {"type": "text"},
            "VIC_AGE_GROUP": {"type": "text"},
            "VIC_RACE": {"type": "text"},
            "VIC_SEX": {"type": "text"},
            "X_COORD_CD": {"type": "float"},
            "Y_COORD_CD": {"type": "float"},
            "Latitude": {"type": "float"},
            "Longitude": {"type": "float"},
            "Lat_Lon": {"type": "text"},
            "New Georeferenced Column": {"type": "geo_point"}

            }
        }

        self.es.indices.create(index=self.esindex, mappings=mappings)


    def deleteNYPDCrimeDataIndex(self):
        '''
        This uses the connection to elastic search to delete the index
        for storign the crime data.  The mappings are defined and used
        for index creation.
        '''

        self.es.indices.delete(index=self.esindex)


    def loadNYPDCrimeDataIntoElasticSearch(self, crimeDataFile):
        '''
        Function for reading the crime data from a specified file.  This assumes
        the index has been created in elastic search and proceeds to load the data
        into the index using the connection to elastic search.
        '''

        idval = 0

        # Open file  
        with open(crimeDataFile) as file_obj: 
            

            heading = next(file_obj) 

            # Create reader object by passing the file  
            # object to reader method 
            reader_obj = csv.reader(file_obj) 

            # Iterate over each row in the csv  
            # file using reader object 
            for row in reader_obj: 

                geopoint = None

                if row[35] != "":
                    geopoint = row[35]

                doc = {
                    "CMPLNT_NUM": row[0],
                    "ADDR_PCT_CD": row[1],
                    "BORO_NM": row[2],
                    "CMPLNT_FR_DT": row[3],
                    "CMPLNT_FR_TM": row[4],
                    "CRM_ATPT_CPTD_CD": row[7],
                    "HADEVELOPT": row[8],
                    "HOUSING_PSA": row[9],
                    "JURISDICTION_CODE": row[10],
                    "JURIS_DESC": row[11],
                    "KY_CD": row[12],
                    "LAW_CAT_CD": row[13],
                    "LOC_OF_OCCUR_DESC": row[14],
                    "OFNS_DESC": row[15],
                    "PARKS_NM": row[16],
                    "PATROL_BORO": row[17],
                    "PD_CD": row[18],
                    "PD_DESC": row[19],
                    "PREM_TYP_DESC": row[20],
                    "RPT_DT": row[21],
                    "STATION_NAME": row[22],
                    "SUSP_AGE_GROUP": row[23],
                    "SUSP_RACE": row[24],
                    "SUSP_SEX": row[25],
                    "TRANSIT_DISTRICT": row[26],
                    "VIC_AGE_GROUP": row[27],
                    "VIC_RACE": row[28],
                    "VIC_SEX": row[29],
                    "X_COORD_CD": row[30],
                    "Y_COORD_CD": row[31],
                    "Latitude": row[32],
                    "Longitude": row[33],
                    "Lat_Lon": row[34],
                    "New Georeferenced Column": geopoint
                }
                        
                self.es.index(index=self.esindex, id=idval, document=doc)
                #print(str(idval)+" "+str(doc))
                idval += 1

                if idval % 100 == 0:
                    print("Loading record: "+str(idval))
                



        print(self.es.cat.count(index=self.esindex, format="json"))


