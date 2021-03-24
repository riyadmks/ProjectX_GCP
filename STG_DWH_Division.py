import apache_beam as beam
import argparse
import json
import datetime

from apache_beam.io import ReadFromText
from apache_beam.io import BigQuerySource
from apache_beam.io import BigQuerySink
from apache_beam.io import WriteToText
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = "studied-client-307710"
google_cloud_options.region = "europe-west1"#"europe-west1"
google_cloud_options.job_name = "dataflowdwdivision"
google_cloud_options.staging_location = "gs://projet_smart_gcp/staging"
google_cloud_options.temp_location = "gs://projet_smart_gcp/temp"
#options.view_as(StandardOptions).runner = "DirectRunner"  # use this for debugging
options.view_as(StandardOptions).runner = "DataFlowRunner"
options.view_as(SetupOptions).setup_file = "./setup.py" 

# see here for bigquery docs https://beam.apache.org/documentation/io/built-in/google-bigquery/
source_table_spec = bigquery.TableReference(
    projectId="studied-client-307710", datasetId="SMT_DWH", tableId="ThirdAxis"
)
sink_table_spec = bigquery.TableReference(
    projectId="studied-client-307710", datasetId="SMT_DWH", tableId="SMT_REF_Division"
)


source = BigQuerySource(query="SELECT ROW_NUMBER() over(order by Code)  AS CORE_ID_DivisionId, Parent AS CORE_LB_DivisionParentCode, Hierarchy AS CORE_LB_DivisionHierarchyCode, level AS CORE_LB_DivisionLevel, Code AS CORE_LB_DivisionCode, EntityEN AS CORE_LB_DivisionEntityEN, EntityFR AS CORE_LB_DivisionEntityFR, EntityStatus AS CORE_LB_DivisionEntityStatusCode, `Order` AS CORE_LB_Order, Id AS CORE_ID_DivisionSourceId, LastMod AS CORA_DT_LastMod,  current_date() AS CORE_DT_RecordCreationDate, current_date() AS CORE_DT_RecordModificationDate, 0 AS CORE_FL_IsDeleted FROM `studied-client-307710.SMT_STG.ThirdAxis`", use_standard_sql=True)  # you can also use SQL queries
#source = BigQuerySource(source_table_spec)
#target = BigQuerySink(sink_table_spec, schema=table_schema)
#target = beam.io.WriteToText("output.txt")

def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    with beam.Pipeline(argv=pipeline_args) as p:
        raw_values = (
            p 
            | "ReadTable" >> beam.io.Read(source) 
            | "cleanup" >> beam.ParDo(ElementCleanup())
            | "writeTable" >> beam.io.WriteToBigQuery(sink_table_spec,
                                    #   schema=table_schema,
                                       write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE, #WRITE_TRUNCATE
                                                       )
            
            #beam.io.Write(target)
            )
        # pipeline
        # parDo for all values in PCollection: process
        # each element: define a target datatype and a set of cleanup functions for each


class ElementCleanup(beam.DoFn):
    """
    tasker uses the %VAR_NAME syntax to construct JSON. Sometimes, values aren't replaced. In these cases, the string starts with a "%". If this is the case, simply replace it with a None
    """
    
    def __init__(self):
        self.transforms = self.make_transform_map()
    def make_transform_map(self):
        return {
            "CORE_ID_DivisionId": [self.passe],
            "CORE_LB_DivisionParentCode":    [self.trim],
            "CORE_LB_DivisionHierarchyCode":     [self.trim],
            "CORE_LB_DivisionLevel":  [self.trim, self.percent_cleaner, self.to_int],
            "CORE_LB_DivisionCode":     [self.trim],
            "CORE_LB_DivisionEntityEN":        [self.trim],
            "CORE_LB_DivisionEntityFR":          [self.trim],
            "CORE_LB_DivisionEntityStatusCode":      [self.trim], 
            "CORE_LB_Order":      [self.trim, self.percent_cleaner, self.to_int], 
            "CORE_ID_DivisionSourceId": [self.trim, self.percent_cleaner, self.to_int],
            "CORA_DT_LastMod": [self.to_datetime],
            "CORE_DT_RecordCreationDate": [self.passe],
            "CORE_DT_RecordModificationDate": [self.passe],
            "CORE_FL_IsDeleted": [self.passe]
        }

    def process(self, row):
        #process receives the object and (must) return an iterable (in case of breaking objects up into several)
        return [self.handle_row(row, self.transforms)]


    def handle_row(self, row, transforms):
        fixed = {}
        for key in row.keys():
            val = row[key]
            for func in transforms[key]:
                val = func(val)
            fixed[key] = val
        return fixed

    def percent_cleaner(self, value: str):
        if isinstance(value, str) and value.startswith("%"):
            return None
        else:
            return value

    def trim(self, val:str):
        return val #val.strip()

    def to_int(self, val: str):
        return (int(val) if val != None else None)

    def to_float(self, val: str):
        return (float(val) if val != None else None)

    def to_datetime(self, val: str):
        import datetime
        return (datetime.datetime.strptime(val, '%d/%m/%Y %H:%M:%S') if val != None else None)

    def passe(self, val):
        return val
    
if __name__ == "__main__":
    run()