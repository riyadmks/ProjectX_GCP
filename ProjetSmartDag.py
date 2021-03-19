# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An example DAG demonstrating simple Apache Airflow operators."""

# [START composer_simple]
from __future__ import print_function

# [START composer_simple_define_dag]
import datetime

from airflow import models
# [END composer_simple_define_dag]
# [START composer_simple_operators]
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.contrib.operators import gcs_to_bq
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
# [END composer_simple_operators]


# [START composer_simple_define_dag]
default_dag_args = {
    # The start_date describes when a DAG is valid / can be run. Set this to a
    # fixed point in time rather than dynamically, since it is evaluated every
    # time a DAG is parsed. See:
    # https://airflow.apache.org/faq.html#what-s-the-deal-with-start-date
    'start_date': datetime.datetime(2021, 3, 2),
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'Projet_SMART_GCP',
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
        
    dataflow_Plans = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/xmlload.py',
        task_id='dataflow_plans',
        options={
            'input': 'Plans.xml',
            'output': 'studied-client-307710:SMT_STG.Plans',
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'studied-client-307710',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_ConcernedFunctions = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/xmlload.py',
        task_id='dataflow_concernedfunctions',
        options={
            'input': 'CS_ConcernedFunctions.xml',
            'output': 'studied-client-307710:SMT_STG.CS_ConcernedFunctions',
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'studied-client-307710',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)
		
    dataflow_Top5Subjects = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/xmlload.py',
        task_id='dataflow_top5subjects',
        options={
            'input': 'CS_Top5Subjects.xml',
            'output': 'studied-client-307710:SMT_STG.CS_Top5Subjects',
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'studied-client-307710',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)
		
    dataflow_MissionTypes = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/xmlload.py',
        task_id='dataflow_missiontypes',
        options={
            'input': 'MissionTypes.xml',
            'output': 'studied-client-307710:SMT_STG.MissionTypes',
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'studied-client-307710',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)
		
    dataflow_Processes = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/xmlload.py',
        task_id='dataflow_processes',
        options={
            'input': 'Processes.xml',
            'output': 'studied-client-307710:SMT_STG.Processes',
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'studied-client-307710',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)
		
    dataflow_RecommendationCriticity = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/xmlload.py',
        task_id='dataflow_recommendationcriticity',
        options={
            'input': 'RecommendationCriticity.xml',
            'output': 'studied-client-307710:SMT_STG.RecommendationCriticity',
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'studied-client-307710',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)
		
    dataflow_Risks = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/xmlload.py',
        task_id='dataflow_risks',
        options={
            'input': 'Risks.xml',
            'output': 'studied-client-307710:SMT_STG.Risks',
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'studied-client-307710',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)
		
    dataflow_SecondAxis = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/xmlload.py',
        task_id='dataflow_secondaxis',
        options={
            'input': 'SecondAxis.xml',
            'output': 'studied-client-307710:SMT_STG.SecondAxis',
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'studied-client-307710',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)
		
    dataflow_ThirdAxis = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/xmlload.py',
        task_id='dataflow_thirdaxis',
        options={
            'input': 'ThirdAxis.xml',
            'output': 'studied-client-307710:SMT_STG.ThirdAxis',
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'studied-client-307710',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)
        
    GCS_to_BQ_Missions = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_Missions',
        bucket='projet_smart_gcp',
        source_objects=['File/Missions.csv'],
        destination_project_dataset_table='SMT_STG.Missions',
        write_disposition='WRITE_TRUNCATE',
        schema_fields=[
        {'name': 'CS_Ref', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_CopyOfStatus', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'PlanCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_PreviousAudit', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_ReferenceAudits', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'LinkTypology', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'MissionTypology', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_Cycle', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'Language', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'Archived', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'ArchivingDate', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'ArchivingResponsibleFor', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'Reason', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'AuditedSites', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_AuditedCountries', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'AuditedProcesses', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'Services', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_ZoneScope', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_DivisionScope', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_CAnet', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_UnitsSold', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_UnitsProduced', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_REXResultatDexploitation', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_BAI', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_StatutoryEmployees', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_TotalEmployeesFTE', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_OverdueValue', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_BadDebtProvisionValue', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_ReturnsValue', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_ServiceRateDivPerc', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_StockValue', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_DestructionValue', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_InfluencersValue', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_NbNonStatutoryEmployees', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'Planned', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'Criticity', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'Type', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_CurrMissPha', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'Initiator', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'Objective', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'AuditContext', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'ExNihilo', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_Referentials', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'ToDuplicate', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'RefCreate', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'ActualStartDate', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'ActualEndDate', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'Agenda', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_EntityDAF', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_EntityICM', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_InChargeOfRecos', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'InternalRN', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'ExternalRN', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_RecoTransDate', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_RecoAccepDate', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_approvReco', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_APValidated', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_APValDate', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_RepIssDate', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'CS_ActionPlanClosed', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'MissionManager', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'MissionManagers', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'AuditTeam', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'GlobalMark', 'type': 'STRING', 'mode': 'NULLABLE'},
		{'name': 'LabelMark', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        skip_leading_rows=1,
        field_delimiter=';',
        dag=dag)

    GCS_to_BQ_Referentiel = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_Referentiel',
        bucket='projet_smart_gcp',
        source_objects=['File/Referentiel.csv'],
        destination_project_dataset_table='SMT_STG.Referentiel',
        write_disposition='WRITE_TRUNCATE',
		schema_fields=[
        {'name': 'CS_WFLevel', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Parent', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MasterAuditReferential', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_AuditId', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_AuditReferenceDataLink', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Level', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'OriginLevel', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Code', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'OrderCode', 'type': 'STRING', 'mode': 'NULLABLE'},			
        {'name': 'ProcessOrigin', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Domain', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AuditPointOrigin', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_ControlPoint', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_CSNewItem', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Mission', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_Progress', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'KPCount', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'RecoCount', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DirectoryWorkingPManagers', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'DirectoryWorkingPAuditors', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CChoiceList', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CAnswer', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AnswerAttachedFile', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_AssessControl', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_SubProcessAssessment', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'LevelNo', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Hyperlink', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_Hyperlink2', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_Hyperlink3', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_Hyperlink4', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_CopyOfAuditGuide', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Risks', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MissionManagers', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'MissionAuditTeam', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CreatedBy', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CreatedOn', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ModifiedBy', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ModifiedOn', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'}
		],
        skip_leading_rows=1,
        field_delimiter=';',
        dag=dag)

    GCS_to_BQ_FindingsExportImport = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_FindingsExportImport',
        bucket='projet_smart_gcp',
        source_objects=['File/CS_FindingsExportImport.csv'],
        destination_project_dataset_table='SMT_STG.CS_FindingsExportImport',
        write_disposition='WRITE_TRUNCATE',
		schema_fields=[
        {'name': 'Reference', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Mission', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'WorkProgram', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_ShowTop5Subject', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_CorresTop5Subject', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_PreviousAuditFinding', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_ConcernedDivisions', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_ConcernedFunc', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_Levelofrisk', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Risks', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AttachedFileLink', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_ReferenceProcess', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_WorkProgramProcess', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AuditReportDisplay', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AuditReportOrder', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'LastModif', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Source', 'type': 'STRING', 'mode': 'NULLABLE'}
		],
        skip_leading_rows=1,
        field_delimiter=';',
        dag=dag)
		
    GCS_to_BQ_Recommendations = gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
        task_id='gcs_to_bq_Recommendations',
        bucket='projet_smart_gcp',
        source_objects=['File/Recommendations.csv'],
        destination_project_dataset_table='SMT_STG.Recommendations',
        write_disposition='WRITE_TRUNCATE',
		schema_fields=[
        {'name': 'CS_LinkToAnExistingRecommandation', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_MainRecommandation', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Mission', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Template', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Reference', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_Reference', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AllowFollowUp', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_WorkProgLevel', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_ProcessLevel', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_HOProcess', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_SubprocessLevel', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AuditedSite', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Archived', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ArchivingDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ArchivingResponsibleFor', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Reason', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Services', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AssociatedKeyPoint', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_AssociatedFindings', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_AssociatedFindingsRisks', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_Levelofrisk', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_OtherSuggestedRisks', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_ZoneCommentsIA', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_RecommendationAnswer', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_AcceptanceDate', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_Status', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_ImplemRatPercent', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_ImplemRatAudit', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Calendar', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'FUCreation', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CreatedBy', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CreatedOn', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ModifiedBy', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ModifiedOn', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'LastModif', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Source', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'AuditTypology', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_MigrationImplementation', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'CS_FollowupCreation', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        skip_leading_rows=1,
        field_delimiter=';',
        dag=dag)
    # [START composer_simple_relationships]
    # Define the order in which the tasks complete by using the >> and <<
    # operators. In this example, hello_python executes before goodbye_bash.
    dataflow_Plans >> dataflow_ConcernedFunctions >> dataflow_Top5Subjects
    dataflow_MissionTypes >> dataflow_Processes >> dataflow_RecommendationCriticity
    dataflow_Risks >> dataflow_SecondAxis >> dataflow_ThirdAxis
    GCS_to_BQ_Missions
    GCS_to_BQ_Referentiel
    GCS_to_BQ_FindingsExportImport
    GCS_to_BQ_Recommendations
    # [END composer_simple_relationships]
# [END composer_simple]