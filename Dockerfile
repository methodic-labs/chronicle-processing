# get base image
FROM prefecthq/prefect:1.1.0

RUN pip install pandas numpy uuid pendulum SQLAlchemy prefect psycopg2
