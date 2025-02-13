import math
import time
import psycopg2
import sqlalchemy as sq
from datetime import timedelta
from uuid import uuid4
import numpy as np
import pandas as pd
import pendulum
# sys.path.insert(1,'/chronicle-processing')
# sys.path.insert(0, '/Users/kimengie/Dev/methodic-labs/chronicle-processing')
from methodic_packages.pymethodic2.pymethodic2 import utils as ut
from methodic_packages.pymethodic2.pymethodic2.constants import interactions
from methodic_packages.pymethodic2 import preprocessing  #TO-DO - why fails when adding another .pymethodic2

run_id = pendulum.now().strftime('%Y-%m%d-%H%M%S-') + str(uuid4())
logger = ut.write_to_log(run_id, 'preprocessing_output.log')

# FOR DAILY PROCESSING
def default_time_params(tz_input="UTC"):
    ''' set default start time - midnight UTC.
    Daily processing needs to go back 2 days to correctly set the flag for "large time gap" (1 day)'''
    timezone = pendulum.timezone(tz_input)
    start_default = pendulum.today(timezone).subtract(days=3)
    end_default = pendulum.today(timezone).subtract(days=2)

    return (start_default, end_default)

def connect(dbuser, password, hostname, port, dbname, type="sqlalchemy"):
    ''' sqlalchemy or psycopg2 connection engine '''
    if type=="sqlalchemy":
        engine = sq.create_engine(f'postgresql://{dbuser}:{password}@{hostname}:{port}/{dbname}')
        return engine
    elif type=="psycopg2":
        conn = psycopg2.connect(user=dbuser,
                                password=password,
                                host=hostname,
                                port=port,
                                dbname=dbname)
        return conn


# EXTRACT
# @task(log_stdout=True)
def search_timebin(
        start_iso: "start timestamp in ISO string format",
        end_iso: "end timestamp in ISO string format",
        tz_input: "input timezone in which to search. Defaults to UTC",
        engine: "sqlalchemy connection engine",
        participants=[], studies=[], exclude=False) -> pd.DataFrame:
    '''
    Collects all data points in the chronicle_usage_events table in a certain time interval.
    Returns a pandas dataframe.
    Start and End will be a string, coming from input args.
    '''

    timezone = pendulum.timezone(tz_input)
    starttime_range = pendulum.parse(start_iso, tz=timezone)
    endtime_range = pendulum.parse(end_iso, tz=timezone)

    logger.info(f"""#------------------------------------------------------------#
    ## Pulling data from {starttime_range} to {endtime_range} {tz_input} time. ##
    #------------------------------------------------------------#""")

    filters = [
        interactions.user_interaction,
        interactions.start_up,
        interactions.power_off,
        interactions.notification_seen,
        interactions.notification_interruption,
        interactions.screen_interactive1,
        interactions.screen_interactive2,
        interactions.screen_non_interactive1,
        interactions.screen_non_interactive2,
        interactions.wat
        ] + interactions.foreground + interactions.background

    starttime_chunk = starttime_range
    all_hits = []
    time_interval = 720  # minutes. This is 12 hrs.

    while starttime_chunk < endtime_range:

        endtime_chunk = min(endtime_range, starttime_chunk + timedelta(minutes=time_interval))
        logger.info(
            f'Searching {starttime_chunk.strftime("%y-%m-%d %H:%M")} to {endtime_chunk.strftime("%y-%m-%d %H:%M")}')

        first_count = ut.query_usage_table(starttime_chunk, endtime_chunk, filters, engine,
                                               counting=True, users=participants, studies=studies, exclude=exclude)
        count_of_data = first_count
        # print(f"Endtime is now {endtime_chunk}, count is {count_of_data}.")

        # But stop relooping after 1 day even if few data, move on.
        if count_of_data < 1000 and time_interval < 1440:
            logger.info(
                f"There are few data points ({count_of_data}), expanding to {time_interval * 2} minutes and redoing search")
            refined_time = time_interval * 2
            end_interval = starttime_chunk + timedelta(minutes=refined_time)
            logger.info(f"Now searching between {starttime_chunk} and {end_interval}.")

            second_count = ut.query_usage_table(starttime_chunk, end_interval, filters, engine, counting=True,
                                                    users=participants, studies=studies, exclude=exclude)
            count_of_data = second_count
            endtime_chunk = end_interval  # make the endtime = the refined endtime to properly advance time periods
            time_interval = refined_time
            continue

        while count_of_data > 1600000:
            logger.info(
                f"There are {count_of_data} hits, narrowing to {time_interval / 2} minutes and redoing search")
            refined_time = time_interval / 2
            end_interval = starttime_chunk + timedelta(minutes=refined_time)
            logger.info(f"Now searching between {starttime_chunk} and {end_interval}.")

            second_count = ut.query_usage_table(starttime_chunk, end_interval, filters, engine, counting=True,
                                                    users=participants, studies=studies, exclude=exclude)
            count_of_data = second_count
            endtime_chunk = end_interval  # make the endtime = the refined endtime to properly advance time periods
            time_interval = refined_time
            continue

        logger.info(f"There are {count_of_data} data points, getting data.")
        data = ut.query_usage_table(starttime_chunk, endtime_chunk, filters, engine, users=participants,
                                        studies=studies, exclude=exclude)
        # print(f"data object from raw table query is of type {type(data)}")
        for row in data:
            row_as_dict = dict(row)
            all_hits.append(row_as_dict)

        starttime_chunk = min(endtime_chunk, endtime_range)  # advance the start time to the end of the search chunk
        time_interval = 720  # reset to original
    logger.info("Finished retrieving raw data!")
    return pd.DataFrame(all_hits)


# TRANSFORM
# @task(log_stdout=True)
def chronicle_process(rawdatatable, run_id):
    ''' rawdatatable: pandas dataframe passed in from the search_timebin function.'''
    if rawdatatable.shape[0] == 0:
        logger.info("No found app usages :-\ ...")
        return None

    # Loop over all participant IDs (could be parallelized at some point):
    preprocessed_data = []
    ids = rawdatatable['participant_id'].unique()

    if len(ids) > 0:
        for person in ids:
            df = rawdatatable.loc[rawdatatable['participant_id'] == person]
            logger.info(f"Analyzing data for {person}:  {df.shape[0]} datapoints.")

            # ------- PREPROCESSING - 1 person at a time. RETURNS DF
            person_df_preprocessed = preprocessing.get_person_preprocessed_data.run(
                person,
                rawdatatable
            )

            if isinstance(person_df_preprocessed, None.__class__):
                logger.info(f"After preprocessing, no data remaining for person {person}.")
                continue

            preprocessed_data.append(person_df_preprocessed)

        if not preprocessed_data or all(x is None for x in preprocessed_data):
            logger.info(f"No participants found in this date range")
            pass
        elif len(preprocessed_data) > 0:
            preprocessed_data = pd.concat(preprocessed_data)
            preprocessed_data.insert(0, 'run_id', run_id)
            logger.info(f"## Finished preprocessing, with {preprocessed_data.shape[0]} total rows! ##")

    logger.info(f"""#---------------------------------------------#
                Data preprocessed!""")
    return preprocessed_data


# WRITE - CSV
def export_csv(data, filepath, filename):
    if isinstance(data, pd.DataFrame):
        if filepath and filename:
            try:
                data.to_csv(f"{filepath}/{filename}.csv", index=False)
                logger.info(f"Data written to csv!")
            except:
                logger.error("You must specify a filepath and a filename to output a csv!")
                raise ValueError("You must specify a filepath and a filename to output a csv!")


# WRITE - REDSHIFT
def write_preprocessed_data(dataset, conn, retries=3):
    # get rid of python NaTs for empty timestamps
    if isinstance(dataset, pd.DataFrame):
        dataset[['app_datetime_end', 'app_duration_seconds']] = \
            dataset[['app_datetime_end', 'app_duration_seconds']] \
                .replace({np.NaN: None})
        dataset[['app_usage_flags']] = dataset[['app_usage_flags']].astype(str)

    if dataset is None or dataset.empty:
        logger.info(f"No data to write! Pipeline is exiting.")
        ut.write_log_summary.run(run_id, "", conn, "No data to write! Pipeline is exiting.")
        return

    participants = dataset['participant_id'].unique().tolist()
    studies = dataset['study_id'].unique().tolist()
    tries = retries
    cursor = conn.cursor()
    final_data = []

    cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
    #     print (cursor.fetchall())

    # Check for existing overlapping processed data - must be done by participant as the timezone may vary by person
    # to accurately query the table containing preprocessed data on the backend
    for i in range(tries):
        try:
            for p in participants:
                df = dataset.loc[dataset['participant_id'] == p]
                # print(f"df columns are {df.columns}") #---------------- TEMP
                start_range = str(min(df['app_datetime_start']))
                end_range = str(max(df['app_datetime_start']))
                timezone = df['app_timezone'].unique()[0]
                studies = df['study_id'].unique().tolist()

                older_data_q = ut.query_export_table.run(start_range, end_range, timezone,
                                                         counting=True,
                                                         users=p,
                                                         studies=studies)
                #                 print(f"Query to check for older data for participant {p}: {older_data_q}")
                cursor.execute(older_data_q)
                conn.commit()
                older_data_count = cursor.fetchall()[0][0]  # Number of rows of older data found

                # Delete old data if needed
                if older_data_count > 0:
                    logger.info(
                        f"Found {older_data_count} older rows overlapping in time range for user {p}. Dropping before proceeding.")

                    # This is only executed if needed
                    drop_query = ut.query_export_table.run(start_range, end_range, timezone,
                                                           counting=False,
                                                           users=p,
                                                           studies=studies)
                    cursor.execute(drop_query)
                    logger.info(f"Dropped {cursor.rowcount} rows of older preprocessed data.")

                final_data.append(df)  # happens regardless of whether older data is dropped
                # print("Appended final data.")    #-----------------------TEMP
        except Exception as e:
            if i < tries - 1:  # i is zero indexed
                logger.exception(e)
                logger.exception("Rolling back and retrying...")
                cursor.execute("ROLLBACK")
                conn.commit()
                time.sleep(5)
                continue
            else:
                raise
        break

        # Insert new data
    if not final_data or all(x is None for x in final_data):
        logger.info("No new preprocessed data found in this date range")
        pass
    elif len(final_data) > 0:
        final_data = pd.concat(final_data)
        # print(f"final_data columns are {final_data.columns}") #----------- TEMP
        dataset2 = final_data.to_numpy()
        args_str = b','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in
                             tuple(map(tuple, dataset2)))
        limit = 15206216  # max bytes allowed in Redshift writes = 16777216 bytes
        n_writes_needed = math.ceil(len(args_str) / limit)

        print(f"Number of writes is {n_writes_needed}, limit {limit}, data {len(args_str)}")

        rows_in_preprocessed = len(dataset2)
        bytes_per_row = len(args_str) / rows_in_preprocessed
        rows_to_write = math.floor(limit / bytes_per_row)  # = number of rows until data reaches the limit

        print(f"""Check rows = {rows_in_preprocessed}, bytes per row {bytes_per_row}, 
        rows to write = {rows_to_write}.""")

        chunk_startrow = 0
        iteration_count = 1

        for i in range(n_writes_needed):
            if rows_to_write * iteration_count > len(dataset2):
                chunk_endrow = len(dataset2)
            else:
                chunk_endrow = rows_to_write * iteration_count

            chunk_df = dataset2[chunk_startrow:chunk_endrow]

            chunk_str = b','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", x) for x in
                                  tuple(map(tuple, chunk_df)))

            print(f"Chunk size is len(chunk_str) = {len(chunk_str)}")

            write_new_query = "INSERT INTO preprocessed_usage_events (run_id, study_id, participant_id, \
                app_record_type, app_title, app_full_name,\
                app_datetime_start, app_datetime_end, app_timezone, app_duration_seconds, \
               day, weekdayMF, weekdayMTh, weekdaySTh, \
               app_engage_30s, app_switched_app, app_usage_flags) VALUES" + chunk_str.decode("utf-8")

            cursor.execute(write_new_query)
            message = f"{cursor.rowcount} rows of preprocessed data written to exports table, from {pendulum.parse(start_range).to_datetime_string()} to {pendulum.parse(end_range).to_datetime_string()} {timezone} timezone."
            logger.info(message)
            # needs the sqlalchemy engine but if using pandas, needs the psycopg2 engine.
            ut.write_log_summary(run_id, studies, conn, message)

            chunk_startrow = chunk_endrow
            logger.info(f"Chunk {iteration_count} written.")
            iteration_count += 1

    conn.commit()  # DOESN'T HAPPEN UNTIL THE COMMIT
    logger.info(f"""
    #-----------------------------------------------#
    Preprocessing pipeline finished, for {studies} studies!
    #-----------------------------------------------#
    """)

def how_export(data, studies, filepath, filename, conn, format="csv"):
    if data is None:
        logger.info("No found app usages for time period. Nothing written, pipeline exiting.")
        ut.write_log_summary(run_id, studies, conn,
                             "No found app usages for time period. Nothing written, pipeline exiting.")
    if format == "csv":
        export_csv(data, filepath=filepath, filename=filename)
        logger.info(f"Data written to {filepath}/{filename}.csv")
        ut.write_log_summary(run_id, studies, conn, f"Data written to {filepath}/{filename}.csv")
    else:
        write_preprocessed_data(data, conn)

