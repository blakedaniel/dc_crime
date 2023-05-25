from prefect.runtime import flow_run, task_run
import datetime
import polars as pl
from prefect import flow, task

# defaul run names
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