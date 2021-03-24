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
google_cloud_options.job_name = "dataflowdwhrecommendation"
google_cloud_options.staging_location = "gs://projet_smart_gcp/staging"
google_cloud_options.temp_location = "gs://projet_smart_gcp/temp"
#options.view_as(StandardOptions).runner = "DirectRunner"  # use this for debugging
options.view_as(StandardOptions).runner = "DataFlowRunner"
options.view_as(SetupOptions).setup_file = "./setup.py" 

# see here for bigquery docs https://beam.apache.org/documentation/io/built-in/google-bigquery/
source_table_spec = bigquery.TableReference(
    projectId="studied-client-307710", datasetId="SMT_STG", tableId="Recommendations"
)
sink_table_spec = bigquery.TableReference(
    projectId="studied-client-307710", datasetId="SMT_DWH", tableId="SMT_REF_Recommendation"
)


source = BigQuerySource(query="SELECT  distinct ROW_NUMBER() over(order by CS_MainRecommandation) as CORE_ID_RecommendationId, CS_LinkToAnExistingRecommandation as CORE_LB_LinkToAnExistingRecommandation, CS_MainRecommandation as CORE_LB_MainRecommandation, -1 as CORE_ID_AuditId, Template as CORE_LB_Template, Reference as CORE_LB_RecommendationReference, null as CORE_LB_RecommendationTitle, AllowFollowUp as CORE_LB_AllowFollowUp, -1 as CORE_ID_WorkProgramAuditId, pro.CORE_ID_ProcessId as CORE_ID_ProcessId, sub.CORE_ID_ProcessId as CORE_ID_SubProcessId, AuditedSite as CORE_LB_AuditedSiteCode, Archived as CORE_LB_Archived, ArchivingDate as CORE_LB_ArchivingDate, ArchivingResponsibleFor as CORE_LB_ArchivingResponsibleFor, Reason as CORE_LB_Reason, Services as CORE_LB_Services, CS_Levelofrisk as CORE_LB_LevelOfRisk, CS_OtherSuggestedRisks as CORE_LB_OtherSuggestedRisks, null as CORE_LB_OtherSuggestedRisksAll, null as CORE_LB_CopyOfDescription, CS_ZoneCommentsIA as CORE_LB_ZoneCommentsIA, CS_RecommendationAnswer as CORE_LB_RecommendationAnswer, CS_AcceptanceDate as CORE_DT_AcceptanceDate, null as CORE_LB_EntityComment, null as CORE_LB_AuditTeamAuditeesReason, CS_Status as CORE_LB_RecommendationStatus, CS_ImplemRatPercent as CORE_FL_ImplemRatPercent, CS_ImplemRatAudit as CORE_FL_ImplemRatAudit, Calendar as CORE_DT_Calendar, null as CORE_LB_Fonction, null as CORE_LB_InChargeOfRecoText, FUCreation as CORE_DT_FUCreation, CreatedBy as CORE_LB_CreatedBy, CreatedOn as CORE_DT_CreatedOn, ModifiedBy as CORE_LB_ModifiedBy, ModifiedOn as CORE_DT_ModifiedOn, Id as CORE_ID_RecommendationSourceId, LastModif as CORE_DT_LastModif, Source as CORE_LB_Source, AuditTypology as CORE_LB_AuditTypology, CS_FollowupCreation as CORE_DT_FollowUpCreation, current_date() as CORE_DT_RecordCreationDate, current_date() as CORE_DT_RecordModificationDate, 0 as CORE_FL_IsDeleted FROM `studied-client-307710.SMT_STG.Recommendations` rec left outer join `studied-client-307710.SMT_DWH.SMT_REF_Process` pro on cast(pro.CORE_ID_ProcessSourceId as STRING) = rec.CS_ProcessLevel left outer join `studied-client-307710.SMT_DWH.SMT_REF_Process` sub on cast(sub.CORE_ID_ProcessSourceId as STRING) = rec.CS_SubprocessLevel ", use_standard_sql=True)  # you can also use SQL queries
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
            "CORE_ID_RecommendationId": [self.passe],
            "CORE_LB_LinkToAnExistingRecommandation":    [self.trim],
            "CORE_LB_MainRecommandation":    [self.trim],
            "CORE_ID_AuditId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_LB_Template":    [self.trim],
            "CORE_LB_RecommendationReference":    [self.trim, self.NulltoNA],
            "CORE_LB_RecommendationTitle":    [self.trim],
            "CORE_LB_AllowFollowUp":    [self.trim],
            "CORE_ID_WorkProgramAuditId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_ID_ProcessId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_ID_SubProcessId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_LB_AuditedSiteCode":    [self.trim],
            "CORE_LB_Archived":    [self.trim],
            "CORE_LB_ArchivingDate":    [self.trim],
            "CORE_LB_ArchivingResponsibleFor":    [self.trim],
            "CORE_LB_Reason":    [self.trim],
            "CORE_LB_Services":    [self.trim],
            "CORE_LB_LevelOfRisk":    [self.trim],
            "CORE_LB_OtherSuggestedRisks":    [self.trim],
            "CORE_LB_OtherSuggestedRisksAll":    [self.trim],
            "CORE_LB_CopyOfDescription":    [self.trim],
            "CORE_LB_ZoneCommentsIA":    [self.trim],
            "CORE_LB_RecommendationAnswer":    [self.trim],
            "CORE_DT_AcceptanceDate":    [self.to_date],
            "CORE_LB_EntityComment":    [self.trim],
            "CORE_LB_AuditTeamAuditeesReason":    [self.trim],
            "CORE_LB_RecommendationStatus":    [self.trim],
            "CORE_FL_ImplemRatPercent":    [self.trim, self.percent_cleaner, self.to_float],
            "CORE_FL_ImplemRatAudit":    [self.trim, self.percent_cleaner, self.to_float],
            "CORE_DT_Calendar":    [self.to_date],
            "CORE_LB_Fonction":    [self.trim],
            "CORE_LB_InChargeOfRecoText":    [self.trim],
            "CORE_DT_FUCreation":    [self.to_datetime],
            "CORE_LB_CreatedBy":    [self.trim],
            "CORE_DT_CreatedOn":    [self.to_datetimeWithoutSec],
            "CORE_LB_ModifiedBy":    [self.trim],
            "CORE_DT_ModifiedOn":    [self.to_datetimeWithoutSec],
            "CORE_ID_RecommendationSourceId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_DT_LastModif":    [self.to_datetime],
            "CORE_LB_Source":    [self.trim],
            "CORE_LB_AuditTypology":    [self.trim],
            "CORE_DT_FollowUpCreation":    [self.to_datetime],
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
        return (int(val) if val != None and val != 'NULL' else None)

    def to_float(self, val: str):
        return (float(val) if val != None and val != 'NULL' else None)

    def to_datetime(self, val: str):
        import datetime
        return (datetime.datetime.strptime(val, '%d/%m/%Y %H:%M:%S') if val != None and val != 'NULL' else None)

    def to_datetimeWithoutSec(self, val: str):
        import datetime
        return (datetime.datetime.strptime(val, '%d/%m/%Y %H:%M') if val != None else None)

    def to_date(self, val: str):
        import datetime
        return (datetime.datetime.strptime(val, '%d/%m/%Y') if val != None and val != 'NULL' else None)

    def passe(self, val):
        return val

    def NulltoNA(self, val):
        return ('NA' if val == None else val )

    def NulltoNUM(self, val):
        return (-1 if val == None else val )
    
if __name__ == "__main__":
    run()