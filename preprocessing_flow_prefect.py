from prefect.tasks.secrets import PrefectSecret
from prefect import task, Flow, Parameter
import sqlalchemy as sq
from datetime import timedelta
import pandas as pd
import pendulum

# from pymethodic import utils as ut
# from pymethodic import preprocessing
import chronicle_process_functions

#--------- Preprocessing and Summarizing Chronicle data via Methodic. By date, specific participant and/or by study
# TIMEZONE = pendulum.timezone("UTC")

@task(checkpoint=False)
def connect(dbuser, password, hostname, port):
    ''' sqlalchemy connection engine '''
    engine = sq.create_engine(f'postgresql://{dbuser}:{password}@{hostname}:{port}/redshift')
    return engine

@task
def default_time_params(tz_input="UTC"):
    ''' set default start time - midnight UTC.'''
    timezone = pendulum.timezone(tz_input)
    start_default = pendulum.today(timezone).subtract(days=1)
    end_default = pendulum.today(timezone)

    return (start_default, end_default)


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

# EXTRACT
@task(log_stdout=True)
def search_timebin(
    start_iso: "start timestamp in ISO string format",
    end_iso: "end timestamp in ISO string format",
    tz_input: "input timezone in which to search. Defaults to UTC",
    engine: "sqlalchemy connection engine",
    participants = [], studies = []) -> pd.DataFrame:
    '''
    Collects all data points in the chronicle_usage_events table in a certain time interval.
    Returns a pandas dataframe.
    Start and End will be a string, coming from input args.
    '''

    timezone = pendulum.timezone(tz_input)
    starttime_range = pendulum.parse(start_iso, tz=timezone)
    endtime_range = pendulum.parse(end_iso, tz=timezone)

    print("#######################################################################")
    print(f"## Pulling data from {starttime_range} to {endtime_range} {tz_input} time. ##")
    print("#######################################################################")

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

    starttime_chunk = starttime_range
    all_hits = []
    time_interval = 60  # minutes

    while starttime_chunk < endtime_range:
        endtime_chunk = min(endtime_range, starttime_chunk + timedelta(minutes=time_interval))
        print("#---------------------------------------------------------#")
        print(f"Searching {starttime_chunk} to {endtime_chunk}")

        first_count = query_usage_table.run(starttime_chunk, endtime_chunk, filters, engine,
                                        counting=True, users=participants, studies=studies)
        count_of_data = first_count

        if (count_of_data < 1000):
            print(f"There are few data points, expanding to {time_interval * 2} minute intervals and redoing search")
            refined_time = time_interval * 2
            end_interval = starttime_chunk + timedelta(minutes=refined_time)
            print(f"Now searching between {starttime_chunk} and {end_interval}.")

            second_count = query_usage_table.run(starttime_chunk, end_interval, filters, engine, counting=True,
                                             users=participants, studies=studies)
            count_of_data = second_count
            endtime_chunk = end_interval  # make the endtime = the refined endtime to properly advance time periods
            time_interval = refined_time
            continue

        if (count_of_data > 50000):
            print(
                f"There are more hits than 50K for this interval, narrowing to {time_interval / 2} minute intervals and redoing search")
            refined_time = time_interval / 2
            end_interval = starttime_chunk + timedelta(minutes=refined_time)
            print(f"Now searching between {starttime_chunk} and {end_interval}.")

            second_count = query_usage_table.run(starttime_chunk, end_interval, filters, engine, counting=True,
                                             users=participants, studies=studies)
            count_of_data = second_count
            endtime_chunk = end_interval  # make the endtime = the refined endtime to properly advance time periods
            time_interval = refined_time
            continue

        print(f"There are {count_of_data} data points, getting data.")

        data = query_usage_table.run(starttime_chunk, endtime_chunk, filters, engine, users=participants, studies=studies)
        # print(f"data object from raw table query is of type {type(data)}")
        for row in data:
            row_as_dict = dict(row)
            all_hits.append(row_as_dict)

        starttime_chunk = min(endtime_chunk, endtime_range)  # advance the start time to the end of the search chunk
        time_interval = 60  # reset to original
    return pd.DataFrame(all_hits)


# TRANSFORM
@task(log_stdout=True)
def chronicle_process(rawdatatable, startdatetime=None, enddatetime=None, tz_input="UTC", days_back=5):
    ''' rawdatatable: pandas dataframe passed in from the search_timebin function.'''
    if rawdatatable.shape[0] == 0:
        print("No found app usages :-\ ...")
        return None

    timezone = pendulum.timezone(tz_input)
    if startdatetime is not None:
        startdatetime = str(startdatetime)
        startdatetime = pendulum.parse(startdatetime, tz=timezone)
    if enddatetime is not None:
        enddatetime = str(enddatetime)
        enddatetime = pendulum.parse(enddatetime, tz=timezone)

    # Loop over all participant IDs (could be parellized at some point):
    preprocessed_data = []
    ids = rawdatatable['participant_id'].unique()

    if len(ids) > 0:
        for person in ids:
            df = rawdatatable.loc[rawdatatable['participant_id'] == person]
            print(f"Analyzing data for {person}:  {df.shape[0]} datapoints.")

            # ------- PREPROCESSING - 1 person at a time. RETURNS DF
            person_df_preprocessed = chronicle_process_functions.get_person_preprocessed_data.run(
                person,
                rawdatatable,
                startdatetime,
                enddatetime,
                days_back
            )

            preprocessed_data.append(person_df_preprocessed)

            if isinstance(person_df_preprocessed, None.__class__):
                print(f"After preprocessing, no data remaining for person {person}.")
                continue

        if len(preprocessed_data) > 0:
            preprocessed_data = pd.concat(preprocessed_data)
            print("#######################################################################")
            print(f"## Finish preprocessing, with {preprocessed_data.shape[0]} total rows! ##")
            print("#######################################################################")
    else:
        print(f"No participants found in this date range")

    print("#---------------------------------------------#")
    print("Pipeline finished successfully !")
    return preprocessed_data

# WRITE - CSV OR REDSHIFT
def export_data(data, filepath, filename, csv=True):
    if isinstance(data, pd.DataFrame):
        if csv:
            if filepath and filename:
                try:
                    data.to_csv(f"{filepath}/{filename}.csv", index = False)
                    print("Data written to csv!")
                except:
                    raise ValueError("You must specify a filepath and a filename to output a csv!")
    #     else:
            # TO-DO: WRITE INTO REDSHIFT. MUST CLARIFY "days_back" processing first.


def main():
    daily_range = default_time_params.run() #needs to be in this format to work outside of Flow
    start_default = str(daily_range[0])
    end_default = str(daily_range[1])

    # builds the DAG
    with Flow("preprocessing_daily") as flow:
        # Set up input parameters
        startdatetime = Parameter("startdatetime", default = start_default) #'Start datetime for interval to be integrated.'
        enddatetime = Parameter("enddatetime", default = end_default) #'End datetime for interval to be integrated.'
        participants = Parameter("participants", default=[]) #"Specific participant(s) candidate_id.", required=False
        studies = Parameter("studies", default=[]) #"Specific study/studies.", required=False
        daysback = Parameter("daysback", default=5) #"For daily processor, the number of days back.", required=False
        tz_input = Parameter("timezone", default = "UTC")
        dbuser = Parameter("dbuser", default="datascience") # "Username to source database
        hostname = Parameter("hostname", default="chronicle.cey7u7ve7tps.us-west-2.redshift.amazonaws.com")
        port = Parameter("port", default=5439)  # Port of source database
        filepath = Parameter("filepath", default="")
        filename = Parameter("filename", default="")
        password = PrefectSecret("dbpassword") # Password to source database

        engine = connect(dbuser, password, hostname, port)
        print("Connection successful!")

        # Extract:
        raw = search_timebin(startdatetime, enddatetime, tz_input, engine, participants, studies)

        # Transform:
        processed = chronicle_process(raw, startdatetime, enddatetime, tz_input, daysback)

        # Write out:
        written = export_data(processed, filepath, filename, csv=True)

    flow.register(project_name="Preprocessing")

if __name__ == '__main__':
    main()

