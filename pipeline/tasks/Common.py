from prefect.runtime import flow_run, task_run
from prefect import task
import polars as pl
import datetime
from pathlib import Path
import os

# common paths
data_path = os.path.abspath(__file__)
data_path = Path(data_path).parent.parent / 'data'

# environment setup
if os.getenv('ENVIRONMENT') == 'development':
    uri = os.getenv('POSTGRES_URI')
elif os.getenv('ENVIRONMENT') == 'production':
    uri = os.getenv('POSTGRES_URI')

# default run names
def generateFlowRunName():
    flow_name = flow_run.get_flow_name()
    flow_date = datetime.datetime.utcnow()
    return f'{flow_name}-{flow_date}'

def generateTaskRunName():
    task_name = task_run.get_task_name()
    task_date = datetime.datetime.utcnow()
    return f'{task_name}-{task_date}'

# common tasks
@task(tags=['datetime'])
def currentYear():
    return datetime.date.today().year

@task(name='API Response Converter')
def convertToDataFrame(results):
    return pl.from_dicts(results)

# common flows

# configs
if os.getenv('ENVIRONMENT') == 'dev':
    uri = os.getenv('POSTGRES_URI')
elif os.getenv('ENVIRONMENT') == 'prod':
    uri = os.getenv('POSTGRES_URI') # TODO: update to production uri