# Hotel Tone Analyzer & Indexer
First trial using IBM-watson tone analyzer &amp; Elasticsearch with Hotels data

## Hotel Tone Analyzer:
- A service with one endpoint to get the total emotional tones for a hotel, as described in the next step
- Normalized total tones for the hotel using (Watson python lib). This lib will give you a normalized score for the detected tones, aggregate them all and get a final score.
  - For example, if review #1, for a specific hotel scored 0.25 angry, and 0.80 sad
  - Review #2 scored 0.7 happy, and 0.65 sad
  - Review #3 scored 0.2 happy, 0.7 angry, and 0.4 sad
  - So the total normalized tones for this hotel is 0.47 angry, 0.45 happy, and 0.62 sad
  - To have an insight on the tones, try this (web interface), but don’t use this interface in the task

## Elasticsearch Indexer
- Have a quick look first on what is ElasticSearch, and how it works
- When I hit this endpoint it should use ElasticSearch to index all data found in dataset for each hotel
- Each hotel has only one document with all its data, including data obtained from Watson lib



### Elasticsearch installtion
- Prerequsities
  * Java version>6
  * Ubuntu OS version>14
```
>> sudo apt-get install apt-transport-https
>> wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
>> add-apt-repository "deb https://artifacts.elastic.co/packages/7.x/apt stable main"
>> sudo apt-get update
>> sudo apt-get install elasticsearch
```

### Elasticsearch Configuration
```
>> sudo nano /etc/elasticsearch/elasticsearch.yml

## Change here in any configs you want
 network.host: the host IP to be deployed on
 cluster.name: the cluster name
 node.name: node name
```
