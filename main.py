import apache_beam as beam
from typing import NamedTuple
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
import json
import numpy as np
from apache_beam.options.pipeline_options import GoogleCloudOptions
import time
from apache_beam.options.pipeline_options import StandardOptions
from google.cloud import language_v1

class AnalyzeSentiment(beam.DoFn):

    def process(self, text):
        client = language_v1.LanguageServiceClient()

        document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
        sentiment = client.analyze_sentiment(document=document).document_sentiment
        score = sentiment.score
        sentiment_label = self.get_sentiment_label(score)
        
        yield {"sentiment": sentiment_label}
        
    def get_sentiment_label(self, score):
        if score >= 0.2:
            return 'positive'
        elif score <= -0.2:
            return 'negative'
        else:
            return 'neutral'
        
        
class Tweet(NamedTuple):
    
    id : str
    text : str

beam.coders.registry.register_coder(Tweet, beam.coders.RowCoder)



def parse_json(element):

    line = json.loads(element)
    return Tweet(**line)





def to_dict(element):
    return element._asdict()


full_data_schema = {
    "fields": [
        {
            "name": "id",
            "type": "INTEGER"
        },
        {
            "name": "text",
            "type": "STRING"
        }    
    ]
}

sentiment_schema = {
    "fields": [

        {
            "name":"sentiment",
            "type":"STRING"
        }
    ]
}

    


#Creación del pipeline
def run():
    
    
    #parametros    
    parser = argparse.ArgumentParser(description= "Pipeline para análisis de sentimientos")
    parser.add_argument("--input", help = "ruta del archivo input")
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--region', required=True, help='Specify Google Cloud region')
    parser.add_argument('--staging_location', required=True, help='Specify Cloud Storage bucket for staging')
    parser.add_argument('--temp_location', required=True, help='Specify Cloud Storage bucket for temp')
    parser.add_argument('--runner', required=True, help='Specify Apache Beam Runner')
    parser.add_argument('--table_name1', required=True, help='BigQuery table name')
    parser.add_argument('--table_name2', required=True, help='BigQuery table name')

    options = PipelineOptions(save_main_session=True)
    opts = parser.parse_args()
    options.view_as(GoogleCloudOptions).project = opts.project
    options.view_as(GoogleCloudOptions).region = opts.region
    options.view_as(GoogleCloudOptions).staging_location = opts.staging_location
    options.view_as(GoogleCloudOptions).temp_location = opts.temp_location
    options.view_as(GoogleCloudOptions).job_name = '{0}{1}'.format('sentimentanalysis',time.time_ns())
    options.view_as(StandardOptions).runner = opts.runner
    
    
    
    
    input_path = opts.input
    nombre_tabla1 = opts.table_name1
    nombre_tabla2 = opts.table_name2

    with beam.Pipeline(options=options) as p:

        lineas = p | "Leer" >> beam.io.ReadFromText(input_path)
        parse = lineas | "Parse JSON" >> beam.Map(parse_json)

        #devolver todos los datos        
        (parse | "Pasar a diccionario" >> beam.Map(to_dict)
               | "Escribir en tabla1" >> beam.io.WriteToBigQuery(
                   nombre_tabla1,
                   schema=full_data_schema,
                   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                   write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
               )
        )

        #devolver las predicciones
        (parse| "Elegir solo tweet" >> beam.Map(lambda x : x.text)
              | "Analizar" >> beam.ParDo(AnalyzeSentiment())
              | "Escribir en tabla2" >> beam.io.WriteToBigQuery(
                nombre_tabla2,
                schema=sentiment_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
             )
        )     


if __name__ == "__main__":
    run()