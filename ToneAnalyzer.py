import pandas as pd
from tqdm import tqdm as tq
from ibm_watson import ToneAnalyzerV3
from ibm_watson import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator

tq.pandas()


class ToneAnalyzer(object):
    def __init__(self,
                 api_key:str ,
                 data_path:str ='hotel-reviews/7282_1.csv')->None:
        
        """
        Initialize the Watson-IBM Tone Analyzer with the API-KEY and the load the data to be toned.
        
        Args:
            api_key: the APIKEY to be passed for Watson-IBM to initialize the tone analyzer 
            data_path: the data path that needs to be toned
            
        Returns:
            None
        """
        
        self.APIKEY = api_key
        self.DATA_PATH = data_path
        self.tone_analyzer = self.load_tone_analyzer()
    
    def load_tone_analyzer(self
                           ,URL:str ='https://gateway-lon.watsonplatform.net/tone-analyzer/api'
                           ,VERSION:str ='2016-05-19'):
        
        """
        Loading the Tone Analyzer API endpoint from the IBM services.
        
        Args:
            URL: the tone_analyzer API url in IBM (Default: 'https://gateway-lon.watsonplatform.net/tone-analyzer/api')
            Version: the tone_analyzer version (Default: '2016-05-19')
            
        Returns:
            if successfully created => tone_analyzer object
            not successfully created => False
        """
        
        try:
            authenticator = IAMAuthenticator(self.APIKEY)
            tone_analyzer = ToneAnalyzerV3(
                version=VERSION,
                authenticator=authenticator
            )
            tone_analyzer.set_service_url(URL)
            return tone_analyzer
        except ApiException as ex:
            print ("Method failed with status code " + str(ex.code) + ": " + ex.message)
            return False




    def load_data(self):
        
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
        del data['index']
        data.name = data.name.apply(lambda x: x.lower()) # Lowercase to prevent unmatched strings of the same hotels
        data_groups = data.groupby('name') # Apply groupby on hotel name for easier indexing and getting data faster.
        return (data, data_groups)




    def _get_tones(self,
                   text:str):
        
        """
        Uses the initialized tone_analyzer to get whole document tones.
        NOTE:: making the (sentences=False) to accelerate the API response time (no need for sentences analysis)
        
        Args:
            text: whole document text to be analyzed
        
        Returns:
            the API tone_analyzer response of all tones with category and scores
        """
        
        return self.tone_analyzer.tone({'text': text}
                                  ,content_type='application/json'
                                  ,sentences=False).get_result()



    def hotel_review_tones(self,
                           groups: pd.core.groupby.generic.DataFrameGroupBy,
                           hotel_name: str):
        
        """
        Get all reviews tones for specific hotel.
        
        Args:
            groups: all hotel groups for each one with all its reviews
            hotel_name: specific hotel name to get its group reviews
        
        Returns:
            all reviews' tones analysis for specific hotel
        
        """
        
        return groups.get_group(hotel_name.lower())['reviews.text'].progress_apply(self._get_tones)



    def normalize_tones(self,
                        review_tones: pd.core.series.Series):
        
        """
        Normalize all the tones for specific hotel reviews.
        
        Args:
            review_tones: a pandas series of all reviews tones analysis
            
        Returns:
            dictionary of all tones with normalized value  
        """
        
        all_tones = {}
        for review in review_tones:
            for category in review['document_tone']['tone_categories']:
                for tone in category['tones']:
                    if tone['score'] != 0.0:
                        if tone['tone_id'] in all_tones:
                            all_tones[tone['tone_id']].append(tone['score'])
                        else:
                            all_tones[tone['tone_id']]= []
                            all_tones[tone['tone_id']].append(tone['score'])

        ## Normalize on tones
        for tone, score in all_tones.items():
            all_tones[tone] = sum(all_tones[tone])/float(len(all_tones[tone]))

        return all_tones
        
