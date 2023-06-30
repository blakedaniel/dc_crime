import os

if os.getenv('ENVIRONMENT') == 'dev':
    uri = os.getenv('POSTGRES_URI')
elif os.getenv('ENVIRONMENT') == 'prod':
    uri = os.getenv('POSTGRES_URI') # TODO: update to production uri