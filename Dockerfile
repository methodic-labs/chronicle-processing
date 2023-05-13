# get base image
FROM prefecthq/prefect:2.8.3-python3.11

#RUN echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list
#RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -

RUN apt-get update && apt-get install -y git libpq-dev
RUN pip install pandas numpy uuid pendulum SQLAlchemy prefect psycopg2

RUN git clone https://www.github.com/methodic-labs/chronicle-processing
RUN pip install ./chronicle-processing/methodic_packages/pymethodic
