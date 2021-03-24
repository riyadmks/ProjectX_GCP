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
google_cloud_options.job_name = "dataflowdwhaudit"
google_cloud_options.staging_location = "gs://projet_smart_gcp/staging"
google_cloud_options.temp_location = "gs://projet_smart_gcp/temp"
#options.view_as(StandardOptions).runner = "DirectRunner"  # use this for debugging
options.view_as(StandardOptions).runner = "DataFlowRunner"
options.view_as(SetupOptions).setup_file = "./setup.py" 

# see here for bigquery docs https://beam.apache.org/documentation/io/built-in/google-bigquery/
source_table_spec = bigquery.TableReference(
    projectId="studied-client-307710", datasetId="SMT_STG", tableId="Missions"
)
sink_table_spec = bigquery.TableReference(
    projectId="studied-client-307710", datasetId="SMT_DWH", tableId="SMT_REF_Audit"
)


source = BigQuerySource(query="SELECT ROW_NUMBER() over(order by CS_Ref) as CORE_ID_AuditId, CS_Ref as CORE_LB_AuditReference, Title as CORE_LB_AuditTitle, CS_CopyOfStatus as CORE_LB_AuditStatus, plan.CORE_ID_AuditPlanId as CORE_ID_AuditPlanId, CS_PreviousAudit as CORE_LB_PreviousAudit, CS_ReferenceAudits as CORE_LB_ReferenceAudits, LinkTypology as CORE_LB_LinkTypolygy, MissionTypology as CORE_LB_MissionTypolygy, CS_Cycle as CORE_LB_Cycle, Language as CORE_LB_Language, Archived as CORE_LB_Archived, ArchivingDate as CORE_DT_ArchivingDate, ArchivingResponsibleFor as CORE_LB_ArchivingResponsibleFor, Reason as CORN_LB_Reason, Services as CORE_LB_Services, zone.CORE_Id_ZoneId as CORE_ID_ZoneId, CS_CAnet as CORE_FL_CANet, CS_UnitsSold as CORE_FL_UnitsSold, CS_UnitsProduced as CORE_FL_UnitsProduced, CS_REXResultatDexploitation as CORE_FL_REXResultatDexploitation, CS_BAI as CORE_FL_BAI, CS_StatutoryEmployees as CORE_FL_StatutoryEmployees, CS_TotalEmployeesFTE as CORE_FL_TotalEmployeesFTE, CS_OverdueValue as CORE_FL_OverdueValue, CS_BadDebtProvisionValue as CORE_FL_BadDebtProvisionValue, CS_ReturnsValue as CORE_FL_ReturnsValue, CS_ServiceRateDivPerc as CORE_FL_ServiceRateDivPerc, CS_StockValue as CORE_FL_StockValue, CS_DestructionValue as CORE_FL_DestructionValue, CS_InfluencersValue as CORE_FL_InfluencersValue, CS_NbNonStatutoryEmployees as CORE_FL_NbNonStatutoryEmployees, Planned as CORE_LB_Planned, Criticity as CORE_LB_Criticity, typ.CORE_ID_AuditTypeId as CORE_ID_AuditTypeId, CS_CurrMissPha as CORE_LB_CurrMissPha, Initiator as CORE_LB_Initiator, Objective as CORT_LB_Objective, AuditContext as CORE_LB_AuditContext, ExNihilo as CORE_LB_ExNihilo, ToDuplicate as CORE_LB_ToDuplicate, RefCreate as CORE_LB_RefCreate, ActualStartDate as CORE_DT_ActualStartDate, ActualEndDate as CORE_DT_ActualEndDate, Agenda as CORE_LB_Agenda, null as CORE_FL_EntityDAF, CS_EntityICM as CORE_LB_EntityICMCode, InternalRN as CORE_FL_INT64ernalRN, ExternalRN as CORE_FL_ExternalRN, null as CORE_LB_CheckAvailabilityTA, CS_RecoTransDate as CORE_DT_RecoTransDate, CS_RecoAccepDate as CORE_DT_RecoAccepDate, CS_approvReco as CORE_FL_ApprovReco, CS_APValidated as CORE_LB_APValidated, CS_APValDate as CORE_DT_APValDate, CS_RepIssDate as CORE_DT_RepissDate, CS_ActionPlanClosed as CORE_LB_ActionPlanClosed, MissionManager as CORE_LB_MissionManager, GlobalMark as CORE_LB_GlobalMark, LabelMark as CORE_LB_LabelMark, null as CORE_LB_MarkDescription, null as CORE_LB_Conclusion, null as CORE_LB_Improve, null as CORE_LB_CopyOfSynthesis, null as CORE_LB_RATitle, null as CORE_LB_RASubTitle, null as CORE_DT_ARDate, null as CORE_LB_FinalAuditReportCode, null as CORE_DT_FinalAuditEmi, null as CORE_ID_MissionSourceId, null as CORE_FL_DurationReal, null as CORE_FL_DurationPlan, null as CORE_DT_LastUpdateReceived, null as CORE_DT_LastModif, null as CORE_LB_Source, current_date() as CORE_DT_RecordCreationDate, current_date() as CORE_DT_RecordModificationDate, 0 as CORE_FL_IsDeleted FROM `studied-client-307710.SMT_STG.Missions` miss left outer join `studied-client-307710.SMT_DWH.SMT_REF_AuditPlan` plan on cast(miss.PlanCode as INT64) = plan.CORE_ID_AuditPlanSourceId left outer join `studied-client-307710.SMT_DWH.SMT_REF_AuditType` typ on cast(miss.Type as INT64) = typ.CORE_ID_AuditTypeSourceId left outer join `studied-client-307710.SMT_DWH.SMT_REF_Zone` zone on cast(miss.CS_ZoneScope as INT64) = zone.CORE_ID_ZoneSourceId", use_standard_sql=True)  # you can also use SQL queries
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
            "CORE_ID_AuditId":    [self.passe],
            "CORE_LB_AuditReference":    [self.trim],
            "CORE_LB_AuditTitle":    [self.trim],
            "CORE_LB_AuditStatus":    [self.trim],
            "CORE_ID_AuditPlanId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_LB_PreviousAudit":    [self.trim],
            "CORE_LB_ReferenceAudits":    [self.trim],
            "CORE_LB_LinkTypolygy":    [self.trim],
            "CORE_LB_MissionTypolygy":    [self.trim],
            "CORE_LB_Cycle":    [self.trim],
            "CORE_LB_Language":    [self.trim],
            "CORE_LB_Archived":    [self.trim],	
            "CORE_DT_ArchivingDate":    [self.to_datetimeWithoutSec],
            "CORE_LB_ArchivingResponsibleFor":    [self.trim],
            "CORN_LB_Reason":    [self.trim],
            "CORE_LB_Services":    [self.trim],
            "CORE_ID_ZoneId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_FL_CANet":    [self.to_float],
            "CORE_FL_UnitsSold":    [self.to_float],
            "CORE_FL_UnitsProduced":    [self.to_float],
            "CORE_FL_REXResultatDexploitation":    [self.to_float],
            "CORE_FL_BAI":    [self.to_float],
            "CORE_FL_StatutoryEmployees":    [self.to_float],
            "CORE_FL_TotalEmployeesFTE":    [self.to_float],
            "CORE_FL_OverdueValue":    [self.to_float],
            "CORE_FL_BadDebtProvisionValue":    [self.to_float],
            "CORE_FL_ReturnsValue":    [self.to_float],
            "CORE_FL_ServiceRateDivPerc":    [self.to_float],
            "CORE_FL_StockValue":    [self.to_float],
            "CORE_FL_DestructionValue":    [self.to_float],
            "CORE_FL_InfluencersValue":    [self.to_float],
            "CORE_FL_NbNonStatutoryEmployees":    [self.to_float],
            "CORE_LB_Planned":    [self.trim],
            "CORE_LB_Criticity":    [self.trim],
            "CORE_ID_AuditTypeId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_LB_CurrMissPha":    [self.trim],
            "CORE_LB_Initiator":    [self.trim],
            "CORT_LB_Objective":    [self.trim],
            "CORE_LB_AuditContext":    [self.trim],
            "CORE_LB_ExNihilo":    [self.trim],
            "CORE_LB_ToDuplicate":    [self.trim],
            "CORE_LB_RefCreate":    [self.trim],
            "CORE_DT_ActualStartDate":    [self.to_date],
            "CORE_DT_ActualEndDate":    [self.to_date],
            "CORE_LB_Agenda":    [self.trim],
            "CORE_FL_EntityDAF":    [self.to_float],
            "CORE_LB_EntityICMCode":    [self.trim],
            "CORE_FL_INT64ernalRN":    [self.to_float],
            "CORE_FL_ExternalRN":    [self.to_float],
            "CORE_LB_CheckAvailabilityTA":    [self.trim],
            "CORE_DT_RecoTransDate":    [self.to_date],
            "CORE_DT_RecoAccepDate":    [self.to_date],
            "CORE_FL_ApprovReco":    [self.to_float],
            "CORE_LB_APValidated":    [self.trim],
            "CORE_DT_APValDate":    [self.to_date],
            "CORE_DT_RepissDate":    [self.to_date],
            "CORE_LB_ActionPlanClosed":    [self.trim],
            "CORE_LB_MissionManager":    [self.trim],
            "CORE_LB_GlobalMark":    [self.trim],
            "CORE_LB_LabelMark":    [self.trim],
            "CORE_LB_MarkDescription":    [self.trim],
            "CORE_LB_Conclusion":    [self.trim],
            "CORE_LB_Improve":    [self.trim],
            "CORE_LB_CopyOfSynthesis":    [self.trim],
            "CORE_LB_RATitle":    [self.trim],
            "CORE_LB_RASubTitle":    [self.trim],
            "CORE_DT_ARDate":    [self.passe],
            "CORE_LB_FinalAuditReportCode":    [self.trim],
            "CORE_DT_FinalAuditEmi":    [self.passe],
            "CORE_ID_MissionSourceId":    [self.trim, self.percent_cleaner, self.to_int, self.NulltoNUM],
            "CORE_FL_DurationReal":    [self.to_float],
            "CORE_FL_DurationPlan":    [self.to_float],
            "CORE_DT_LastUpdateReceived":    [self.passe],
            "CORE_DT_LastModif":    [self.passe],
            "CORE_LB_Source":    [self.trim],
            "CORE_DT_RecordCreationDate":    [self.passe],
            "CORE_DT_RecordModificationDate":    [self.passe],
            "CORE_FL_IsDeleted":    [self.passe]
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
        return (datetime.datetime.strptime(val, '%d/%m/%Y %H:%M') if val != None and val != 'NULL' else None)

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