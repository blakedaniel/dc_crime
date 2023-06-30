import requests
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.task_runners import SequentialTaskRunner
from Common import generateFlowRunName, generateTaskRunName, currentYear
from Common import convertToDataFrame, data_path
import datetime
import argparse

# TODO: add cursor to extractCrimeReports() to allow for incremental extraction

@task(name='Crime Report Year ID Curation',
      task_run_name=generateTaskRunName,
      tags=['datetime'])
def compute_year_id(start_year:int=2017, end_year:int=currentYear.fn()):
    """
    Turn years of interest into ids relevant for DC crime report
    URL, which starts at 0 for 2017 and increases by 1 for each
    subsequent year.
    
    This is used in extractCrimeReports().

    Args:
        start_year (int): first year of extraction range
        end_year (int): last year of extraction range (inclusive)

    Returns:
        _type_: tuple
    """
    end_year = end_year - 2017
    start_year = start_year - 2017 if start_year - 2017 >= 0 else 0
    year_ids = (id for id in range(start_year, end_year))
    return year_ids

@task(name='Crime Report API Call',
      task_run_name=generateTaskRunName,
      tags=['api-call'],
      cache_key_fn=task_input_hash,
      cache_expiration=datetime.timedelta(hours=24)
    )
def call_crime_reports_api(year_ids:tuple):
    results = []
    for year_id in year_ids: 
        url = f'https://maps2.dcgis.dc.gov/dcgis/rest/services/FEEDS/MPD/MapServer/{year_id}/query?where=1%3D1&outFields=*&outSR=4326&f=json'
        incidents = requests.get(url).json() 
        for incident in incidents['features']: 
            results.append(incident['attributes'])
    return results

# run flow of all api calls using task_runner=ConcurrentTaskRunner()

@flow(name='Crime Report Extraction',
      flow_run_name=generateFlowRunName)
def extract_crime_reports(start_year:int=2017, end_year:int=currentYear.fn()):
    """
    Flow that exracts the crime report from DC's online database, starting from as yearl
    as 2017 and going to to present time.

    Args:
        start_year (int): first year of extraction range, default: 2017
        end_year (int): last year of extraction range (inclusive), defaul: 2023

    Returns:
        _type_: Polar.DataFrame
    """
    year_ids = compute_year_id(start_year, end_year)
    results = call_crime_reports_api(year_ids)
    results = convertToDataFrame(results)
    return results


# move everything from below into a seperate main flow file 
@flow
def extract_crime_main(start_year:int=2017, end_year:int=currentYear.fn()):
    extract_crime_reports(start_year, end_year).write_csv(data_path / 
                                                        f'dc_crimes_{start_year}_{end_year}.csv')
    return data_path / f'dc_crimes_{start_year}_{end_year}.csv'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DC Crimes Data Extraction')
    parser.add_argument('--sy', type=int, help='Specify start year')
    parser.add_argument('--ey', type=int, help='Specify end year')
    args = parser.parse_args()
    extract_crime_main(args.sy or currentYear.fn(), args.ey or currentYear.fn())