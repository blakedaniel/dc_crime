FROM python:3.9.7

RUN apt-get install wget
RUN pip install polars sqlalchemy psycopg2 prefect prefect-docker

WORKDIR /app
COPY pipeline /app/

ENTRYPOINT ["bash"]