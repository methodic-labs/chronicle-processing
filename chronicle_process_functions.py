from pymethodic.pymethodic import preprocessing
from pymethodic.pymethodic import utils as ut
# from pymethodic.constants import interactions, columns
from pymethodic.pymethodic.constants import columns as es
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
def search_timebin(
    start_iso: "start timestamp in ISO string format",
    end_iso: "end timestamp in ISO string format", 
    engine: "sqlalchemy connection engine",
    participants = [], studies = []) -> pd.DataFrame:
    '''
    Collects all data points in the chronicle_usage_events table in a certain time interval.
    Returns a pandas dataframe
    '''

    starttime_range = dateutil.parser.parse(start_iso)
    endtime_range = dateutil.parser.parse(end_iso)

    starttime_chunk = starttime_range

    filters = [
        "Move to Background",
        "Move to Foreground",
        "Unknown importance: 7",
        "Unknown importance: 10",
        "Unknown importance: 11",
        "Unknown importance: 12",
        "Unknown importance: 15",
        "Unknown importance: 16",
        "Unknown importance: 26"
    ]

    all_hits = []
    time_interval = 60

    while starttime_chunk < endtime_range:
        endtime_chunk = min(endtime_range, starttime_chunk + timedelta(minutes=time_interval))
        print("#---------------------------------------------------------#")
        print(f"Searching {starttime_chunk} to {endtime_chunk}")

        count_of_data = query_usage_table.run(starttime_chunk, endtime_chunk, filters, engine, counting=True, users=participants, studies=studies)

        # If there are too many results: make interval narrower and redo
        if (count_of_data > 50000):
            if time_interval < 0.001:
                raise ValueError(f"There are too many hits ({count_of_data}) for this date in < 0.001 seconds.")

            print(
                f"There are more hits than 50K for this interval and condition ({count_of_data}), narrowing to {time_interval / 2} minute intervals and redoing search")
            refined_time = time_interval / 2
            end_interval = starttime_chunk + timedelta(minutes=refined_time)
            print(f"Now searching between {starttime_chunk} and {end_interval}.")
            count_of_data = query_usage_table.run(starttime_chunk, end_interval, filters, engine, counting=True, users=participants, studies=studies)
            print(f"Refined count: {count_of_data}, getting data.")

            data = query_usage_table.run(starttime_chunk, end_interval, filters, engine, users=participants)
            for row in data:
                row_as_dict = dict(row)
                all_hits.append(row_as_dict)

            endtime_chunk = end_interval  # make the endtime = the refined endtime to properly advance time periods

        # If < 50k results:
        print(f"There are {count_of_data} data points, getting data.")
        data = query_usage_table.run(starttime_chunk, endtime_chunk, filters, engine, users=participants, studies=studies)
        # print(f"data object from raw table query is of type {type(data)}")
        for row in data:
            row_as_dict = dict(row)
            all_hits.append(row_as_dict)

        starttime_chunk = min(endtime_chunk, endtime_range)  # advance the start time to the end of the search chunk
        # print(f"all_hits is of type {print(type(all_hits))}")
    return pd.DataFrame(all_hits)


@task(log_stdout=True)
def get_person_preprocessed_data(
    person: "the participantID",
    datatable: "the raw data",
    # constants: "dictionary of constants",
    startdatetime = None,
    enddatetime = None,
    days_back = 5
) -> pd.DataFrame:
    '''
    Select the data to be preprocessed, one person at a time, and run through preprocessor
    '''

    df = datatable.loc[datatable['participant_id'] == person]
    print(f"Person with ID {person} has {df.shape[0]} datapoints.")

    column_replacement = es.column_rename
    df = df.rename(columns=column_replacement)

    timezone = df['app_timezone'].unique()[0]
    if len(df['app_timezone'].unique()) > 1:
        ut.logger.run(f"Person with ID {person} has data in > 1 timezone! Investigate manually.")
        pass

    ####----------- Why are we renaming columns? Skipping for now.
    ## ...also seems redundant with 1st step in preprocess_dataframe

    # if constants['org'] == "CAFE":
    #     # rename columns
    #     property_metadata = entitySetsAPI.get_all_entity_set_property_metadata(
    #         constants['chronicle_app_data_id'])
    #     column_replacement = {
    #         ut.get_fqn_string(edmAPI.get_property_type(ptid).type): f'app_{mtd.title}' \
    #         for ptid, mtd in property_metadata.items()
    #     }
    # else:

    # subset the data for daily processing, only batches of 5 days at a time
    if startdatetime is not None:
        startdatetime = str(startdatetime)
        startdatetime = pendulum.parse(startdatetime, tz=timezone)
        if enddatetime is not None:
            enddatetime = str(enddatetime)
            enddatetime = pendulum.parse(enddatetime, tz=timezone)

        # current delta time set to 5 days, though that can totally be subject of change :)
        df['app_date_logged_date'] = pd.to_datetime(df.app_date_logged)
        df = df[(df['app_date_logged_date'] >= startdatetime -  timedelta(days=int(days_back))) & (df['app_date_logged_date'] <= enddatetime)]
        df.drop(['app_date_logged_date'], axis=1, inplace=True)
        # ut.logger(f"Person with ID {df['participant_id'][0]} has {df.shape[0]} datapoints after filtering.")

    ###---------- PREPROCESS HERE!! RERTURNS DF. NEED TO ROWBIND
    df_prep = preprocessing.preprocess_dataframe.run(df, sessioninterval = [30])

    if isinstance(df_prep, None.__class__):
        return df_prep

    return df_prep
