import os
import pandas as pd
from Common import data_path, currentYear, uri
from prefect import task, flow
from prefect.tasks import task_input_hash
import argparse
import datetime

@task(name='Write Crime Reports to Database',
      log_prints=True,
      retries=3,
      cache_key_fn=task_input_hash,
      cache_expiration=datetime.timedelta(hours=24))
def write_crime_reports_to_db(start_year:int=currentYear.fn(),
                                end_year:int=currentYear.fn()):
    df = pd.read_csv(data_path / f'dc_crimes_{start_year}_{end_year}.csv')
    df.to_sql('dc_crimes', uri, if_exists='replace', index=False)
    
@task(name='Write ACS reports to Database',
      log_prints=True,
      retries=3,
      cache_key_fn=task_input_hash,
      cache_expiration=datetime.timedelta(hours=24))
def write_acs_econ_report_to_db():
    df = pd.read_csv(data_path / 'acsEcon.csv')
    df.to_sql('acs_econ', uri, if_exists='replace', index=False)
    
@task(name='Write ACS reports to Database',
      log_prints=True,
      retries=3,
      cache_key_fn=task_input_hash,
      cache_expiration=datetime.timedelta(hours=24))
def write_acs_demo_report_to_db():
    df = pd.read_csv(data_path / 'acsDemo.csv')
    df.to_sql('acs_econ', uri, if_exists='replace', index=False)

@flow(name='Ingest Crime Reports to Database')
def main(start_year:int=currentYear.fn(), end_year:int=currentYear.fn()):
    write_crime_reports_to_db(start_year, end_year)
    write_acs_econ_report_to_db()
    write_acs_demo_report_to_db()
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DC Crimes Data Ingestion')
    parser.add_argument('--sy', type=int, help='Specify start year')
    parser.add_argument('--ey', type=int, help='Specify end year')
    args = parser.parse_args()
    main(args.sy or currentYear.fn(), args.ey or currentYear.fn())