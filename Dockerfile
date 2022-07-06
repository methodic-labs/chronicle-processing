# get base image
FROM prefecthq/prefect:1.1.0
RUN apt-get update && apt-get install postgresql-devel
RUN pip install pandas numpy uuid pendulum SQLAlchemy prefect psycopg2
