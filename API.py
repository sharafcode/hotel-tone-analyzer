import json
from ToneAnalyzer import ToneAnalyzer
from ESIndexer import ESIndexer
from flask import Flask, request, render_template
from flask_cors import CORS, cross_origin



def tones_endpoint(api_key: str,
                   hotel_name: str,
                   data_path: str):
    TA = ToneAnalyzer(api_key)
    data, data_groups = TA.load_data()
    try:
        review_tones = TA.hotel_review_tones(data_groups, hotel_name)
        tones_normalized = TA.normalize_tones(review_tones)
        status='ok'
    except e:
        print ("API failed with status code " + str(ex.code) + ": " + ex.message)
        status='fail'
        
    return json.dumps({'status':status, 'tones':tones_normalized}, default=json_default)


def elasticsearch_index(host: str,
                        port: int,
                        api_key: str,
                        data_path: str,
                        index_name: str,
                        type_name: str,
                        mapping_settings: dict):
    
    ESI = ESIndexer(host , port , api_key , data_path)
    ESI.create_index(mapping_settings , index_name)
    ESI.store_records(index_name , type_name)
    status='ok'
    return json.dumps({'status':status}, default=json_default)


app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

# Routes
@app.route('/')
def hello_world():
    return 'TONES/ElasticSearch API is Working!'
    

@app.route('/normalized_tones', methods = ['POST'])
@cross_origin()
def api_normalized_tones():
    data= ast.literal_eval(list(request.form.to_dict().keys())[0])
    api_key = data['api_key'] if 'api_key' in data and isinstance(data['api_key'],str) else ''
    hotel_name = data['hotel_name'] if 'hotel_name' in data and isinstance(data['hotel_name'],str) else ''
    data_path = data['data_path'] if 'data_path' in data and isinstance(data['data_path'],str) else 'hotel-reviews/7282_1.csv'
    
    return tones_endpoint(api_key, hotel_name,data_path)


@app.route('/index_data', methods = ['POST'])
@cross_origin()
def index_data():
    host = data['es_host'] if 'es_host' in data and isinstance(data['es_host'],str) else 'localhost'
    port = data['es_port'] if 'es_port' in data and isinstance(data['es_port'],str) else 9200
    data_path = data['data_path'] if 'data_path' in data and isinstance(data['data_path'],str) else 'hotel-reviews/7282_1.csv'
    api_key = data['api_key'] if 'api_key' in data and isinstance(data['api_key'],str) else ''
    index_name =  data['index_name'] if 'index_name' in data and isinstance(data['index_name'],str) else 'hotels'
    type_name = data['type_name'] if 'type_name' in data and isinstance(data['type_name'],str) else 'reviews'
    mapping_settings =   data['es_mapping'] if 'es_mapping' in data and isinstance(data['es_mapping'], dict) else {}
    return elasticsearch_index(host , port , api_key, data_path , index_name , type_name , mapping_settings)

