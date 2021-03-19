import argparse
import logging
import apache_beam as beam
import json
import xmltodict
from google.cloud import storage


def parse_into_dict(xmlfile):
    import xmltodict
    from google.cloud import storage
    # create storage client
    storage_client = storage.Client()#.from_service_account_json('/path/to/SA_key.json')
    # get bucket with name
    bucket = storage_client.get_bucket('projet_smart_gcp')
    # get bucket data as blob
    blob = bucket.get_blob('File/'+xmlfile)
    blob.download_to_filename(xmlfile)
    #root = ET.fromstring(contents)
    with open(xmlfile) as ifp:
        doc = xmltodict.parse(ifp.read())
        return doc

def get_orders(doc):
    for order in doc['Data']['Document']['Record']:
        yield order

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
      '--output',
      required=True,
      help=(
          'Specify text file orders.txt or BigQuery table project:dataset.table '))
    parser.add_argument(
      '--input',
      required=True,
      help=(
          'Specify text file orders.txt or BigQuery table project:dataset.table '))

    
    known_args, pipeline_args = parser.parse_known_args(argv)    
    with beam.Pipeline(argv=pipeline_args) as p:
        orders = (p 
             | 'files' >> beam.Create([known_args.input])
             | 'parse' >> beam.Map(parse_into_dict)
             | 'orders' >> beam.FlatMap(get_orders))
           #  | 'back to json' >> beam.Map(lambda x: json.dumps(x)))

        if '.json' in known_args.output:
             orders | 'totxt' >> beam.io.WriteToText(known_args.output)
        else:
             orders | 'tobq' >> beam.io.WriteToBigQuery(known_args.output,
                                    #   schema=table_schema,
                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, #WRITE_TRUNCATE
                                                       )#  create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

    
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
