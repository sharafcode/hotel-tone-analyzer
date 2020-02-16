import pandas as pd
import logging
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from tqdm import tqdm as tq
from ToneAnalyzer import ToneAnalyzer

tq.pandas()


class ESIndexer(object):
    def __init__(self,
                 api_key:str,
                 host:str = 'localhost',
                 port:int = 9200,
                 data_path:str='hotel-reviews/7282_1.csv'):
        
        """
        Initialize the Elasticsearch engine with the data to import in it.
        
        Args:
            host: the exact Elasticsearch cluster host. (Default : 'localhost') 
            port: the exact Elasticsearch cluster port. (Default : 9200)
            data_path: data_path of the CSV file to be imported in Elasticsearch indes 
                       (Default : the tone-analyzed data of hotels) 
            
        Returns:
            None
        """
        
        # Dataset loading
        self.DATA_PATH = data_path
        self.data , self.data_groups = self.__load_data()
        self.TA = ToneAnalyzer(api_key)
        # ElasticSearch connection
        self.HOST = host
        self.PORT = port
        self.es = self.__connect_es()
    
    def __load_data(self):
        
        """
        Loading the data passed to this class constructor, then grouping it based on hotels' name
        
        Returns:
            data: the selected/cleaned hotels data read from the external CSV file
            data_groups: grouped hotels data based on hotels' names
        """
        
        data = pd.read_csv(self.DATA_PATH)
        ## Gets only "Hotels" value in column "categories"
        data= data[data.categories=='Hotels']
        data.reset_index(inplace=True)
        data.drop('categories', axis=1, inplace=True) # Useless column (non-unique)
        del data['index']
        data.name = data.name.apply(lambda x: x.lower()) # Lowercase to prevent unmatched strings of the same hotels
        data_groups = data.groupby('name') # Apply groupby on hotel name for easier indexing and getting data faster.
        return (data, data_groups)
        
        
    def __connect_es(self):
        
        """
        Connects to the Elasticsearch server with the passed parameters to the constructor (HOST , PORT)
        
        Returns:
            _es: the elasticsearch connection object
        
        """
        
        _es = None
        _es = Elasticsearch([{'host': self.HOST , 'port': self.PORT }])
        if _es.ping():
            print('ElasticSearch server Connected!')
        else:
            print('ElasticSearch server failed to connect!')
        return _es
        
    def __group_hotel_reviews(self,
                              hotel_name: str):
        
        """
        Creating new hotels object to group all reviews in just ONE object for each hotel.
        Args:
            hotel_name: the hotel name passed to group its all reviews in just ONE object
        Returns:
            hotel_object: {'metadata' => contains all the hotel meta-data (name, address , city ...etc.), 
                           'reviews' => contains an array of objects for all reviews,
                           'tones' => watson-ibm tones analysis normalized for all reviews}
        """
        
        HOTEL_META = ['name', 'address', 'city' , 'country', 'latitude', 'longitude' , 'postalCode', 'province'] # Hotel Metadata fields to group in one field
        review_tones = self.TA.hotel_review_tones(self.data_groups, hotel_name)
        tones_normalized = self.TA.normalize_tones(review_tones)
        hotel_group = self.data_groups.get_group(hotel_name.lower())
               
        hotel_metadata = json.loads(hotel_group[HOTEL_META].iloc[0].to_json())
        hotel_group.drop(HOTEL_META, axis=1, inplace=True)

        COLS_RENAME = {col: col.split('reviews.')[1] for col in hotel_group.columns.to_list()} # Group all the reviews data in one object including the watson_data
        hotel_group.rename(columns=COLS_RENAME, errors="raise", inplace=True)

        reviews = list(json.loads(hotel_group.T.to_json()).values())

        hotel_object = {'metadata': hotel_metadata, 'reviews': reviews, 'tones':tones_normalized} 
        return hotel_object 

        
    def create_index(self,
                     mapping_settings: dict ,
                     index_name:str ='hotels'):
        
        """
        Creating a new/existinig index in Elasticsearch with mapping_settings.
        Args:
            mapping_settings: mapping settings in creation of index in ES
            index_name: the index name created in ES
            
        Returns:
            created: (bool) true => if created successfully , false => if not created (error happened in ES)
        """
        
        created = False
        # index settings
        settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": mapping_settings
        }
        try:
            if not self.es.indices.exists(index_name):
                # Ignore 400 means to ignore "Index Already Exist" error.
                self.es.indices.create(index=index_name, ignore=400, body=settings)
                print('Created Index')
            created = True
        except Exception as ex:
            print(str(ex))
        finally:
            return created           
            
                 
    def store_records(self,
                      index_name:str,
                      type_name: str):
        
        """
        Store records in the created ES index in specific type.
        Args:
            index_name: the created index in ES to path the data for it
            type_name: the specific type for the data passed to this index
            
        Returns:
            None
        """
        
        for hotel in self.data_groups.groups:
            hotel_data = self.__group_hotel_reviews(hotel.lower())
            try:
                outcome = self.es.index(index=index_name, doc_type=type_name, body=hotel_data)
            except Exception as ex:
                print('Error in indexing data')
                print(str(ex))
                
                
