import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.task_runners import SequentialTaskRunner
from Common import generateFlowRunName, generateTaskRunName, currentYear
from Common import convertToDataFrame, data_path
import polars as pl
import pandas as pd
import datetime
import argparse

# TODO: add cursor to extractCrimeReports() to allow for incremental extraction

@task(name='Economics ACS API Call',
      task_run_name=generateTaskRunName,
      tags=['api-call'],
      cache_key_fn=task_input_hash,
      cache_expiration=datetime.timedelta(days=90)
    )
def call_economics_api(): 
    url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Demographic_WebMercator/MapServer/42/query?outFields=*&where=1%3D1&f=geojson'
    result = requests.get(url).json()
    result = pl.from_records(pl.from_records(result['features'])['properties'])
    result = result.drop("OBJECTID")
    return result

@task(name='Demographics ACS API Call',
      task_run_name=generateTaskRunName,
      tags=['api-call'],
      cache_key_fn=task_input_hash,
      cache_expiration=datetime.timedelta(days=90)
    )
def call_demographics_api(): 
    url = 'https://maps2.dcgis.dc.gov/dcgis/rest/services/DCGIS_DATA/Demographic_WebMercator/MapServer/39/query?outFields=*&where=1%3D1&f=geojson'
    result = requests.get(url).json()
    result = pl.from_records(pl.from_records(result['features'])['properties'])
    result = result.drop("OBJECTID")
    return result

@task(name='Exporting CSV of pl.DataFrame',
      task_run_name=generateTaskRunName,
      cache_key_fn=task_input_hash,
      cache_expiration=datetime.timedelta(days=90)
    )
def create_csv(file_name:str, dataframe:pl.DataFrame) -> str:
    dataframe.write_csv(data_path / f'{file_name}.csv')
    return data_path / f'{file_name}.csv'
# # run flow of all api calls using task_runner=ConcurrentTaskRunner()

@flow(name='ACS Extraction',
      flow_run_name=generateFlowRunName)
def extract_acs():
    data = {'acsEcon': call_economics_api(),
            'acsDemo': call_demographics_api()}
    for file in data:
        path = create_csv(file, data[file])
        data[file] = path
    
    return data
        
