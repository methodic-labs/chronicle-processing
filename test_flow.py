from prefect.blocks.system import Secret, String
from prefect import task, flow
import sqlalchemy as sq

@flow(log_prints=True)
def test_flow():
    secret_block = Secret.load("ds-pwd-redshift")
    password = secret_block.get() # Password to source database
    hostname = String.load("ds-redshift-hostname")

    dbuser = "datascience" # "Username to source database
    port = 5439  # Port of source database

    engine = sq.create_engine(f'postgresql://{dbuser}:{password}@{hostname}:{port}/redshift')
    print("Connection successful!")
    engine.dispose()



if __name__ == "__main__":
    test_flow()

