from methodic_packages.pymethodic import preprocessing
# from pymethodic.constants import interactions, columns
from pymethodic.constants import columns as es
from datetime import timedelta
from prefect import task
# from . import utils as ut
import pendulum
import pandas as pd
import dateutil

TIMEZONE = pendulum.timezone("UTC")

@task
def query_usage_table(start, end, filters, engine, counting=False, users=[], studies=[]):
    '''Constructs the query on the usage data table - to count and also to grab data.
    users = optional people to select for, input as ['participant_id1', 'participant_id2']
    studies = optional studies to select for, input as ['study_id1', 'study_id2']
    COUNTING: returns a scalar
    NOT COUNTING: returns a sqlalchemy ResultProxy (a tuple)'''

    selection = "count(*)" if counting else "*"
    if len(users) > 0:
        person_filter = f''' and \"participant_id\" IN ({', '.join(f"'{u}'" for u in users)})'''
    else:
        person_filter = ""
    if len(studies) > 0:
        study_filter = f''' and \"study_id\" IN ({', '.join(f"'{s}'" for s in studies)})'''
    else:
        study_filter = ""

    data_query = f'''SELECT {selection} from chronicle_usage_events 
        WHERE "event_timestamp" between '{start}' and '{end}' 
        and "interaction_type" IN ({', '.join(f"'{w}'" for w in filters)}){person_filter}{study_filter};'''

    if counting:
        output = engine.execute(data_query).scalar()
    else:
        output = engine.execute(data_query)

    return output


@task(log_stdout=True)
def get_person_preprocessed_data(
    person: "the participantID",
    datatable: "the raw data",
    # constants: "dictionary of constants",
    startdatetime = None,
    enddatetime = None
) -> pd.DataFrame:
    '''
    Select the data to be preprocessed, one person at a time, and run through preprocessor
    '''

    df = datatable.loc[datatable['participant_id'] == person]
    print(f"Person with ID {person} has {df.shape[0]} datapoints.")

    column_replacement = es.column_rename
    df = df.rename(columns=column_replacement)

    ###---------- PREPROCESS HERE!! RETURNS DF. NEED TO ROWBIND
    df_prep = preprocessing.preprocess_dataframe.run(df, sessioninterval = [30])

    if isinstance(df_prep, None.__class__):
        return df_prep

    return df_prep
