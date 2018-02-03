
import csv
from google.cloud import bigquery

fName = 'A:/SentimentAnalysisDataset.csv'

bigquery_client = bigquery.Client.from_service_account_json(
        'A:/Google Cloud Platform/My Project-9d7a4af36792.json')

f = open(fName, 'r', encoding="utf8")
filereader = csv.reader(f)

for row in filereader:
    label = row[1]
    text = row[3]
    query = 'INSERT SentimentAnalysis.trainingdataset (text, label) VALUES({},{})'
    query = query.format("'" + text + "'",label)
    query_job = bigquery_client.query(query)

f.close()
