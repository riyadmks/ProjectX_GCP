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
    'start_date': datetime.datetime(2021, 3, 8),
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
        'Projet_SMART_DWH_GCP',
        #schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:
        
    dataflow_Zone = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_Zone.py',
        task_id='dataflow_zone',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_Division = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_Division.py',
        task_id='dataflow_division',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)
		
    dataflow_Process = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_Process.py',
        task_id='dataflow_process',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_Audit = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_Audit.py',
        task_id='dataflow_audit',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_AuditDivision = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_AuditDivision.py',
        task_id='dataflow_auditdivision',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_AuditPlan = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_AuditPlan.py',
        task_id='dataflow_auditplan',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_AuditProcess = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_AuditProcess.py',
        task_id='dataflow_auditprocess',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_AuditType = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_AuditType.py',
        task_id='dataflow_audittype',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_ConcernedFunction = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_ConcernedFunction.py',
        task_id='dataflow_concernedfunction',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_Risk = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_Risk.py',
        task_id='dataflow_risk',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_RiskLevel = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_RiskLevel.py',
        task_id='dataflow_risklevel',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_Top5Subject = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_Top5Subject.py',
        task_id='dataflow_topsubject',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_WorkProgram = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_WorkProgram.py',
        task_id='dataflow_workprogram',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_WorkProgramAudit = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_WorkProgramAudit.py',
        task_id='dataflow_workprogramaudit',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_WorkProgramRisk = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_WorkProgramRisk.py',
        task_id='dataflow_workprogramrisk',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_Recommendation = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_Recommendation.py',
        task_id='dataflow_recommendation',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_Finding = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_Finding.py',
        task_id='dataflow_finding',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_FindingDivision = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_FindingDivision.py',
        task_id='dataflow_findingdivision',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_FindingRecommendation = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_FindingRecommendation.py',
        task_id='dataflow_findingrecommendation',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_FindingConcernedFunction = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_FindingConcernedFunction.py',
        task_id='dataflow_findingconcernedfunction',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

    dataflow_FindingRisk = DataFlowPythonOperator(
	    py_file='/home/airflow/gcs/dags/STG_DWH_FindingRisk.py',
        task_id='dataflow_findingrisk',
        options={
            'setup_file': '/home/airflow/gcs/dags/setup.py'
        },
        dataflow_default_options={
            'project': 'high-diagram-298919',
            'staging_location': 'gs://projet_smart_gcp/tmp',
            'temp_location': 'gs://projet_smart_gcp/tmp'
        },
		requirements=['google-cloud-storage==1.36.1', 'xmltodict==0.12.0'],
        dag=dag)

	dataflow_Zone
	dataflow_Risk >> dataflow_RiskLevel
	dataflow_Top5Subject
	dataflow_Process
	dataflow_Recommendation
	dataflow_Division
	dataflow_ConcernedFunction
	dataflow_WorkProgram >> dataflow_WorkProgramAudit >> dataflow_WorkProgramRisk
	dataflow_Audit >> dataflow_AuditProcess >> dataflow_AuditType >> dataflow_AuditDivision >> dataflow_AuditPlan
	dataflow_Finding >> dataflow_FindingDivision >> dataflow_FindingRecommendation >> dataflow_FindingRisk >> dataflow_FindingConcernedFunction

    # [END composer_simple_relationships]
# [END composer_simple]