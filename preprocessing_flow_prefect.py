from prefect.tasks.secrets import PrefectSecret
from prefect import task, Flow, Parameter, context
from datetime import timedelta
import sqlalchemy as sq
from uuid import uuid4
import pandas as pd
import numpy as np
import psycopg2
import pendulum
import time

# from pymethodic import utils as ut
# from pymethodic import preprocessing
import chronicle_process_functions

#--------- Preprocessing and Summarizing Chronicle data via Methodic. By date, specific participant and/or by study
# Timezone is UTC by default, but can be changed with an input arg
# TIMEZONE = pendulum.timezone("UTC")

@task(checkpoint=False)
def connect(dbuser, password, hostname, port, type="sqlalchemy"):
    ''' sqlalchemy or psycopg2 connection engine '''
    if type=="sqlalchemy":
        engine = sq.create_engine(f'postgresql://{dbuser}:{password}@{hostname}:{port}/redshift')
        return engine
    elif type=="psycopg2":
        conn = psycopg2.connect(user=dbuser,
                                password=password,
                                host=hostname,
                                port=port,
                                dbname='redshift')
        return conn

@task
def default_time_params(tz_input="UTC"):
    ''' set default start time - midnight UTC.
    Daily processing needs to go back 2 days to correctly set the flag for "large time gap" (1 day)'''
    timezone = pendulum.timezone(tz_input)
    start_default = pendulum.today(timezone).subtract(days=2)
    end_default = pendulum.today(timezone).subtract(days=1)

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
        print(f"Endtime is now {endtime_chunk}, count is {count_of_data}.")

        # But stop relooping after 1 day even if few data, move on.
        if count_of_data < 1000 and time_interval < 1440:
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

        if count_of_data > 50000:
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
    print("#-------------------------------#")
    print("Finished retrieving raw data!")
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
                enddatetime
            )

            if isinstance(person_df_preprocessed, None.__class__):
                print(f"After preprocessing, no data remaining for person {person}.")
                continue

            preprocessed_data.append(person_df_preprocessed)

        if not preprocessed_data or all(x is None for x in preprocessed_data):
            print(f"No participants found in this date range")
            pass
        elif len(preprocessed_data) > 0:
            preprocessed_data = pd.concat(preprocessed_data)
            run_id = pendulum.now().strftime('%Y-%m%d-%H%M%S-') + str(uuid4())
            preprocessed_data.insert(0, 'run_id', run_id)
            print("#######################################################################")
            print(f"## Finish preprocessing, with {preprocessed_data.shape[0]} total rows! ##")
            print("#######################################################################")


    print("#---------------------------------------------#")
    print("Pipeline finished successfully !")
    return preprocessed_data


# WRITE - CSV
@task(log_stdout=True)
def export_csv(data, filepath, filename):
    if isinstance(data, pd.DataFrame):
        if filepath and filename:
            try:
                data.to_csv(f"{filepath}/{filename}.csv", index = False)
                print("Data written to csv!")
            except:
                raise ValueError("You must specify a filepath and a filename to output a csv!")


# WRITE - REDSHIFT
@task
def query_export_table(start, end, timezone, counting=True, users=[], studies=[]):
    '''Helper function - constructs the query on the usage exports table - to count and also to delete data.
    users = optional people to select for, input as ['participant_id1', 'participant_id2']
    studies = optional studies to select for, input as ['study_id1', 'study_id2']
'''
    timezone_of_data = timezone

    # Timestamp must be converted to the timezone of the production db (which is UTC) for proper comparison
    start_utc = pendulum.parse(start, tz=timezone_of_data).in_tz('UTC')
    end_utc = pendulum.parse(end, tz=timezone_of_data).in_tz('UTC')

    selection = "SELECT count(*)" if counting else "DELETE"

    if len(users) > 0:
        person_filter = f''' and \"participant_id\" IN ({', '.join(f"'{u}'" for u in users)})'''
    else:
        person_filter = ""
    if len(studies) > 0:
        study_filter = f''' and \"study_id\" IN ({', '.join(f"'{s}'" for s in studies)})'''
    else:
        study_filter = ""

    data_query = f'''{selection} from preprocessed_usage_events
        WHERE "app_datetime_start" between '{start_utc}' and '{end_utc}' 
        {person_filter}{study_filter};'''

    return data_query

@task(log_stdout=True)
def write_preprocessed_data(dataset, conn, retries=3):
    # get rid of python NaTs for empty timestamps
    if isinstance(dataset, pd.DataFrame):
        dataset[['app_datetime_start', 'app_datetime_end', 'app_duration_seconds']] = \
            dataset[['app_datetime_start', 'app_datetime_end', 'app_duration_seconds']] \
                .replace({np.NaN: None})

    start_range = str(min(dataset['app_datetime_start']))
    end_range = str(max(dataset['app_datetime_start']))
    participants = [dataset['participant_id'].unique()[0]]
    studies = [dataset['study_id'].unique()[0]]
    timezone = dataset['app_timezone'].unique()[0]

    tries = retries
    cursor = conn.cursor()

    # Check for existing overlapping processed data
    for i in range(tries):
        try:
            older_data_q = query_export_table(start_range, end_range, timezone, counting=True,
                                              users=participants,
                                              studies=studies)
            cursor.execute(older_data_q)
        except Exception as e:
            if i < tries - 1:  # i is zero indexed
                print(e)
                print("Rolling back and retrying...")
                cursor.execute("ROLLBACK")
                conn.commit()
                time.sleep(5)
                continue
            else:
                raise
        break

    conn.commit()
    older_data = cursor.fetchall()[0][0]  # Number of rows of older data found

    for i in range(tries):
        try:
            # Delete old data if needed
            if older_data > 0:
                print(f"Found {older_data} rows overlapping in time range for this study. Dropping before proceeding.")

                # Only executed if needed
                drop_query = query_export_table(start_range, end_range, timezone,
                                                counting=False,
                                                users=participants,
                                                studies=studies)
                cursor.execute(drop_query)
                print(f"Dropped {cursor.rowcount} rows of older preprocessed data.")

            # Insert new data
            # run_id, and another %s must be added later
            dataset2 = dataset.to_numpy()
            args_str = b','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s)", x) for x in
                                 tuple(map(tuple, dataset2)))
            write_new_query = "INSERT INTO preprocessed_usage_events (run_id, study_id, participant_id, \
            app_record_type, app_title, app_full_name,\
            app_datetime_start, app_datetime_end, app_timezone, app_duration_seconds, \
           day, weekdayMF, weekdayMTh, weekdaySTh, \
           app_engage_30s, app_switched_app, app_usage_flags) VALUES" + args_str.decode("utf-8")
            cursor.execute(write_new_query)
        except Exception as e:
            print(e)
            if i < tries - 1:  # i is zero indexed
                print("Rolling back and retrying...")
                cursor.execute("ROLLBACK")
                conn.commit()
                time.sleep(30)
                continue
            else:
                raise
        break

    conn.commit()  # DOESN'T HAPPEN UNTIL THE COMMIT
    print("#-----------------------------------------------#")
    print(
        f"{cursor.rowcount} rows of preprocessed data written to exports table, from {pendulum.parse(start_range).to_datetime_string()} to {pendulum.parse(end_range).to_datetime_string()} {timezone} timezone.")
    print("Preprocessing pipeline finished!")
    print("#-----------------------------------------------#")

@task(log_stdout=True)
def how_export(data, filepath, filename, conn, format="csv"):
    if format=="csv":
        export_csv.run(data, filepath=filepath, filename=filename)
    else:
        write_preprocessed_data.run(data, conn)


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
        export_format = Parameter("export_format", default="csv")
        filepath = Parameter("filepath", default="")
        filename = Parameter("filename", default="")
        dbuser = Parameter("dbuser", default="datascience") # "Username to source database
        hostname = Parameter("hostname", default="chronicle.cey7u7ve7tps.us-west-2.redshift.amazonaws.com")
        port = Parameter("port", default=5439)  # Port of source database
        password = PrefectSecret("dbpassword") # Password to source database

        engine = connect(dbuser, password, hostname, port, type="sqlalchemy")
        print("Connection to raw data successful!")

        # Extract:
        raw = search_timebin(startdatetime, enddatetime, tz_input, engine, participants, studies)

        # Transform:
        processed = chronicle_process(raw, startdatetime, enddatetime, tz_input, daysback)

        # Write out:
        conn = connect(dbuser, password, hostname, port, type="psycopg2")
        print("Connection to export table successful!")

        how_export(processed, filepath, filename, conn, format = export_format)
        # exporting = how_export.run(export_format)
        # print(str(exporting))
        # if exporting == "csv":
        #     export_csv(processed, filepath=filepath, filename=filename)
        # write_preprocessed_data(processed, conn)

    flow.register(project_name="Preprocessing")

if __name__ == '__main__':
    main()

