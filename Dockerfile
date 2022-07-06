# get base image
FROM prefecthq/prefect:1.1.0

RUN pip install numpy uuid pendulum SQLAlchemy
