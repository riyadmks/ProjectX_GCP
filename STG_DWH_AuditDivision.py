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
google_cloud_options.job_name = "dataflowdwhauditdivision"
google_cloud_options.staging_location = "gs://projet_smart_gcp/staging"
google_cloud_options.temp_location = "gs://projet_smart_gcp/temp"
#options.view_as(StandardOptions).runner = "DirectRunner"  # use this for debugging
options.view_as(StandardOptions).runner = "DataFlowRunner"
options.view_as(SetupOptions).setup_file = "./setup.py" 

# see here for bigquery docs https://beam.apache.org/documentation/io/built-in/google-bigquery/
source_table_spec = bigquery.TableReference(
    projectId="studied-client-307710", datasetId="SMT_STG", tableId="referentiel"
)
sink_table_spec = bigquery.TableReference(
    projectId="studied-client-307710", datasetId="SMT_DWH", tableId="SMT_REF_WorkProgramAudit"
)


source = BigQuerySource(query="SELECT distinct ROW_NUMBER() over(order by Parent)as CORE_ID_WorkProgramAuditId, Parent as CORE_LB_WorkProgramParentCode, Level as CORE_LB_WorkProgramLevel, Code as CORE_LB_WorkProgramCode, CORE_ID_WorkProgramId as CORE_ID_WorkProgramId, null as CORE_ID_AuditId, CS_Progress as CORE_LB_CS_Progress, CS_AssessControl as CORE_LB_AssessControl, CS_SubProcessAssessment as CORE_LB_SubProcessAssessment, CreatedBy as CORE_LB_CreatedBy, CreatedOn as CORE_DT_CreatedOn, ModifiedBy as CORE_LB_ModifiedBy, ModifiedOn as CORE_DT_ModifiedOn, null as CORE_ID_WorkProgramSourceId,  current_date() as CORE_DT_RecordCreationDate, current_date() as CORE_DT_RecordModificationDate, 0 as CORE_FL_IsDeleted  FROM `studied-client-307710.SMT_STG.Referentiel` ref left outer join `studied-client-307710.SMT_DWH.SMT_REF_WorkProgram` wrk on ref.Code = wrk.CORE_LB_WorkProgramCode", use_standard_sql=True)  # you can also use SQL queries
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
            "CORE_ID_WorkProgramAuditId": [self.passe],
            "CORE_LB_WorkProgramParentCode":    [self.trim],
            "CORE_LB_WorkProgramLevel":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_LB_WorkProgramCode":    [self.trim],
            "CORE_ID_WorkProgramId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_ID_AuditId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_LB_CS_Progress":    [self.trim],
            "CORE_LB_AssessControl":    [self.trim],
            "CORE_LB_SubProcessAssessment":    [self.trim],
            "CORE_LB_CreatedBy":    [self.trim],
            "CORE_DT_CreatedOn":    [self.to_datetime],
            "CORE_LB_ModifiedBy":    [self.trim],
            "CORE_DT_ModifiedOn":    [self.to_datetime],
            "CORE_ID_WorkProgramSourceId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
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

    def to_datetimeWithoutSec(self, val: str):
        import datetime
        return (datetime.datetime.strptime(val, '%d/%m/%Y %H:%M') if val != None else None)

    def to_date(self, val: str):
        import datetime
        return (datetime.datetime.strptime(val, '%d/%m/%Y') if val != None else None)

    def passe(self, val):
        return val

    def NulltoNA(self, val):
        return ('NA' if val == None else val )

    def NulltoNUM(self, val):
        return (-1 if val == None else val )
    
if __name__ == "__main__":
    run()