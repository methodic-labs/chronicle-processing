from datetime import timedelta
from pytz import timezone
from prefect import task
import pandas as pd
import numpy as np
import dateutil.tz
import pendulum
# import os

from .constants import interactions, columns
from . import utils

@task
def read_data(filenm):
    ''' Used in the "preprocess_folder" function.'''
    personid = "-".join(str(filenm).split(".")[-2].split("ChronicleData-")[1:])
    thisdata = pd.read_csv(filenm)
    thisdata['person'] = personid
    return thisdata
    
@task
def clean_data(thisdata):
    '''
    This function transforms a csv file (or dataframe?) into a clean dataset:
    - only move-to-foreground and move-to-background actions
    - extracts person ID
    - extracts datetime information and rounds to 10ms
    - sorts events from the same 10ms by (1) foreground, (2) background
    '''
    utils.logger.run("Cleaning data", level = 1)
    thisdata = thisdata.dropna(subset=['app_record_type', 'app_date_logged']).copy()
    thisdata = thisdata.drop_duplicates(ignore_index = True).copy()
    if len(thisdata)==0:
        return(thisdata)
    thisdata = thisdata.loc[thisdata['app_record_type'] != 'Usage Stat']
    if not 'app_timezone' in thisdata.keys() or any(thisdata['app_timezone']==None):
        utils.logger.run("WARNING: Record has no timezone information.  Registering reported time as UTC.")
        thisdata['app_timezone'] = "UTC"
    if not 'app_title' in thisdata.columns:
        thisdata['app_title'] = ""
    thisdata['app_title'] = thisdata['app_title'].fillna("")
    thisdata = thisdata[['study_id', 'participant_id', 'app_title', 'app_full_name', 'app_record_type', 'app_date_logged', 'app_timezone']].copy()
    # fill timezone by preceding timezone and then backwards
    thisdata = thisdata.sort_values(by=['app_date_logged']).\
        reset_index(drop=True).\
        fillna(method="ffill").fillna(method="bfill")
    thisdata['date_tzaware'] = thisdata.apply(utils.get_dt,axis=1) # transforms string to a timestamp, returns local time
    thisdata['action'] = thisdata.apply(utils.get_action,axis=1)
    thisdata = thisdata.sort_values(by=['date_tzaware', 'action']).reset_index(drop=True)

    thisdata.drop(['action'], axis=1, inplace=True)
    return thisdata

@task
def get_timestamps(curtime, prevtime=False, row=None, precision=60, localtz='UTC'):
    '''
    Function transforms an app usage statistic into bins (according to the desired precision).
    Returns a dataframe with the number of rows the number of time units (= precision in minutes).
    USE LOCAL TIME to extract the correct date (include timezone) but UTC to extract duration.
    '''
    if not prevtime:
        starttime = curtime.astimezone(dateutil.tz.gettz(localtz))
        outtime = [{
            columns.prep_datetime_start: starttime,
            columns.prep_datetime_end: np.NaN,
            "date": starttime.strftime("%Y-%m-%d"),
            "starttime": starttime.strftime("%H:%M:%S.%f"),
            "endtime": np.NaN,
            columns.prep_duration_seconds: np.NaN,
            columns.prep_record_type: np.NaN,
            "participant_id": row['participant_id'],
            columns.full_name: row[columns.full_name],
            columns.title: row[columns.title],
            columns.flags: np.NaN
        }]

        return pd.DataFrame(outtime)

    #round down to precision
    prevtimehour = prevtime.replace(microsecond=0,second=0,minute=0)
    seconds_since_prevtimehour = np.floor((prevtime-prevtimehour).seconds/precision)*precision
    prevtimerounded = prevtimehour+timedelta(seconds=seconds_since_prevtimehour)

    # Timedif will always be max=3600 sec for hourly chunks. Make check in UTC in case the timezones differ
    # timepoints_n = number of new rows
    # i.e., 3 rows if it is 3+ hrs of usage split into hourly chunks
    timedif = curtime - prevtimerounded
    timepoints_n = int(np.floor(timedif.seconds/precision)+int(timedif.days*24*60*60/precision))

    # run over timepoints and append datetimestamps
    delta = timedelta(seconds=0)
    outtime = []
    total_duration = []

    # START TIME IS SOMETIMES PREVIOUS TIME INSTEAD OF DATE LOGGED - HERE'S WHERE DISCREPANCIES CAN COME IN.
    for timepoint in range(timepoints_n+1):
        starttime = prevtime if timepoint == 0 else prevtimerounded+delta #POWER OFF BEHAVIOR
        endtime = curtime if timepoint == timepoints_n else prevtimerounded+delta+timedelta(seconds=precision)
        duration = np.round((endtime - starttime).total_seconds())

        starttime_local = starttime.astimezone(dateutil.tz.gettz(localtz))
        endtime_local = endtime.astimezone(dateutil.tz.gettz(localtz))

        outmetrics = {
            columns.prep_datetime_start: starttime_local,
            columns.prep_datetime_end: endtime_local,
            "date": starttime_local.strftime("%Y-%m-%d"),
            "starttime": starttime_local.strftime("%H:%M:%S.%f"),
            "endtime": endtime_local.strftime("%H:%M:%S.%f"),
            "day": (starttime_local.weekday()+1)%7+1,
            "weekdayMF": 1 if starttime_local.weekday() < 5 else 0,
            "weekdayMTh": 1 if starttime_local.weekday() < 4 else 0,
            "weekdaySTh": 1 if (starttime_local.weekday() < 4 or starttime_local.weekday()==6) else 0,
            "hour": starttime_local.hour,
            "quarter": int(np.floor(starttime_local.minute/15.))+1,
            columns.prep_duration_seconds: duration
        }

        outmetrics['participant_id'] = row['participant_id']
        outmetrics[columns.full_name] = row[columns.full_name]
        outmetrics[columns.title] = row[columns.title]

        delta = delta+timedelta(seconds=precision)

        total_duration.append(duration)
        flag = "3-HR APP DURATION" if sum(total_duration) > 10800 else None           # This previously was "LONG USAGE"
        outmetrics[columns.flags] = flag

        outtime.append(outmetrics)

    return pd.DataFrame(outtime)

@task
def extract_usage(dataframe,precision=3600):
    '''
    Function to extract usage from a filename.
    Precision in seconds.
    '''

    # If cols don't exist, they're added and filled with NaNs
    cols = ['participant_id',
            'app_full_name',
            'application_label',
            'date',
            'app_datetime_start', # was 'app_start_timestamp',
            'app_datetime_end', # was 'app_end_timestamp',
            'starttime',
            'endtime',
            'day',  # note: starts on Sunday !
            'weekdayMF',
            'weekdayMTh',
            'weekdaySTh',
            'hour',
            'quarter',
            'app_duration_seconds',
            'app_record_type'] # was 'app_data_type'
    other_interactions = {
        'Unknown importance: 16': "Screen Non-interactive", # if you left it running
        'Unknown importance: 15': "Screen Interactive",
        'Unknown importance: 10': "Notification Seen",
        'Unknown importance: 12': "Notification Interruption",
        'Unknown importance: 26': "Power Off",
        'Unknown importance: 27': "Device Start up",
        'User Interaction': "User Interaction"
    }


    alldata = []

    rawdata = clean_data.run(dataframe)
    openapps = {}
    latest_unbackgrounded = False

    for idx, row in rawdata.iterrows():
        
        interaction = row['app_record_type']
        app = row['app_full_name']

        # USE UTC TIME to get durations - issues with daylight savings etc.
        # Converts to local time within `get_timestamps` only to infer days/dates
        curtime = row.app_date_logged  #date_tzaware

        # make dict entry of every app that was moved to foreground. Time is local.
        if interaction == 'Move to Foreground':
            openapps[app] = {"open" : True,
                             "time": curtime}

            # For each row in the raw data, iterate through the 'openapps' dict keys
            # if the app isn't the old app, then it's no longer opened, so "close" all of the other apps
            # only the latest Foregrounded is "open"
            for olderapp, appdata in openapps.items():
                if app == olderapp:
                    continue
                # dict of the ONE app immediately preceding the "open" one.
                # fg_time = timestamp of the app after it = timestamp of when it became not in the foreground
                # unbgd_time = its own timestamp = time when it went to foreground (aka was "unbackgrounded")
                if appdata['open'] == True and appdata['time'] < curtime:
                    latest_unbackgrounded = {'unbgd_app':olderapp, 'fg_time':curtime, 'unbgd_time':appdata['time']}
                    openapps[olderapp]['open'] = False

        # if it's an app in the background
        if interaction == 'Move to Background':

            # ONLY applies to the one latest unbackgrounded app.
            if latest_unbackgrounded and app == latest_unbackgrounded['unbgd_app']:
                timediff = curtime - latest_unbackgrounded['fg_time']

                if timediff < timedelta(seconds=1):
                    timepoints = get_timestamps.run(curtime, latest_unbackgrounded['unbgd_time'],
                                                    precision=precision, row=row, localtz=row.app_timezone)

                    timepoints[columns.prep_record_type] = 'App Usage'
                    timepoints['app_timezone'] = row.app_timezone
                    timepoints['study_id'] = row.study_id
                    timepoints['app_title'] = row.app_title

                    alldata.append(timepoints)

                    openapps[app]['open'] = False
                    latest_unbackgrounded = False  # reset to empty

            # BACK TO FOREGROUND PROCESSING - the 'Move to background' app is skipped if so
            if app in openapps.keys() and openapps[app]['open'] == True:
                prevtime = openapps[app]['time']  # time it was opened - the raw data timestamp

                # Checks that end (curtime) is later than start (prevtime)
                if curtime-prevtime<timedelta(0):
                    raise ValueError(f"ALARM: timepoints out of order for {app} starting {prevtime}, ending {curtime}!")

                # HERE IS WHERE APP USAGE IS SPLIT INTO HOURLY CHUNKS AND DURATIONS ARE CALCULATED
                # split up timepoints by precision, 3600 seconds (1 hr)
                timepoints = get_timestamps.run(curtime, prevtime, precision=precision, row=row, localtz=row.app_timezone)

                timepoints[columns.prep_record_type] = 'App Usage'
                timepoints['app_timezone'] = row.app_timezone
                timepoints['study_id'] = row.study_id
                timepoints['app_title'] = row.app_title

                alldata.append(timepoints)

                openapps[app]['open'] = False

        # if the interaction is a part of screen non/interactive or notifications...
        # logic should be the same
        if interaction in other_interactions.keys():
            timepoints = get_timestamps.run(curtime, precision=precision, row=row, localtz=row.app_timezone)
            timepoints[columns.prep_record_type] = other_interactions[interaction]
            timepoints['app_timezone'] = row.app_timezone
            timepoints['study_id'] = row.study_id
            timepoints['app_title'] = row.app_title
            timepoints[columns.flags] = None
            alldata.append(timepoints)

    if len(alldata)>0:
        alldata = pd.concat(alldata, axis=0)
        alldata = alldata.sort_values(by=[columns.prep_datetime_start, columns.prep_datetime_end]).reset_index(drop=True)
        cols_to_select = list(set(cols).intersection(set(alldata.columns)))
        cols_to_select.extend(['app_timezone', 'app_title', columns.flags])
        alldata[cols_to_select].reset_index(drop=True)
        return alldata

@task
def check_overlap_add_sessions(data, session_def = [5*60]):
    '''
    Function to loop over dataset, spot overlaps (and remove them), and add columns
    to indicate whether a new session has been started or not.
    Make a column, "app_switched_app", 0 if not switched from previous row, 1 if so.
    '''
    data_nonzero = data[data[columns.prep_duration_seconds] > 0].reset_index(drop=True) #filter only where there's some durations

    # # Create session column(s) - one per session input argument. Usually one.
    for sess in session_def:
        data_nonzero[f'app_engage_{int(sess)}s'] = 0

    data_nonzero[columns.switch_app] = 0
    # Loop over dataset:
    # - prevent overlap (with warning)
    # - check if a new session is started
    for idx,row in data_nonzero.iterrows():
        if idx == 0:
            for sess in session_def:
                data_nonzero.at[idx, f'app_engage_{int(sess)}s'] = 1 #1st row: automatically set 'app_engage_300'==1

        # Create timedelta of starttime - endtime(previous row)
        nousetime = row[columns.prep_datetime_start].astimezone(timezone("UTC")) - data_nonzero[columns.prep_datetime_end].iloc[idx - 1].astimezone(timezone("UTC"))

        # If an entry started before the previous row closed - 1)warn,
        # ...2) overwrite/shorten endtime to next start time, 3) recalc duration
        if nousetime < timedelta(microseconds=0) and row[columns.prep_datetime_start].date == row[columns.prep_datetime_end].date:
            utils.logger(
                f'''WARNING: Overlapping usage for participant {row['participant_id']}: {data_nonzero.iloc[idx - 1][columns.full_name]}
                    was open since {data_nonzero.iloc[idx - 1][columns.prep_datetime_start].strftime("%Y-%m-%d %H:%M:%S")} 
                    when {row[columns.full_name]} was opened on {row[columns.prep_datetime_start].strftime("%Y-%m-%d %H:%M:%S")}. \
                    Manually closing {data_nonzero.iloc[idx - 1][columns.full_name]}...''')
            data_nonzero.at[idx-1,columns.prep_datetime_end] = row[columns.prep_datetime_start]
            data_nonzero.at[idx-1, columns.prep_duration_seconds] = (data_nonzero.at[idx - 1, columns.prep_datetime_end] - data_nonzero.at[idx - 1, columns.prep_datetime_start]).seconds

        # # If the timedelta is > the "session" arg (5 min default), flag w/1
        else:
            for sess in session_def:
                if nousetime > timedelta(seconds = sess):
                    data_nonzero.at[idx, f'app_engage_{int(sess)}s'] = 1

        # Populate app_switched_app for all rows. If previous row is the same app, make it 0. If not 1.
        data_nonzero.at[idx, columns.switch_app] = 1-(row[columns.full_name]==data_nonzero[columns.full_name].iloc[idx-1])*1

    return data_nonzero.reset_index(drop=True)

@task
def log_exceed_durations_minutes(row, threshold, outfile):
    timestamp = row[columns.prep_datetime_start].astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    with open(outfile, "a+") as fl:
        fl.write("Person {participant} used {app} more than {threshold} minutes on {timestamp}\n".format(
            participant = row['participant_id'],
            app = row[columns.full_name],
            threshold = threshold,
            timestamp = timestamp
        ))

@task
def preprocess_dataframe(dataframe, precision=3600,sessioninterval = [5*60]):
    # dataframe = utils.backwards_compatibility(dataframe)
    utils.logger.run("LOG: Extracting usage...",level=1)
    tmp = extract_usage.run(dataframe,precision=precision)   #HERE get app durations
    if not isinstance(tmp,pd.DataFrame) or np.sum(tmp[columns.prep_duration_seconds]) == 0:
        utils.logger.run(f"WARNING: Person {dataframe['participant_id'].iloc[0]} does not seem to contain relevant data.  Skipping...")
        return None
    utils.logger.run("LOG: checking overlap session...",level=1)
    data = check_overlap_add_sessions.run(tmp,session_def=sessioninterval)
    data = utils.add_warnings.run(data) #Add flags for long usage or usage gaps
    non_timed = tmp[tmp[columns.prep_duration_seconds].isna()]
    flagcols = [x for x in non_timed.columns if 'engage' in x or 'switch' in x]
    non_timed.loc[:, (flagcols)] = None
    data = pd.concat([data, non_timed], ignore_index=True, sort=False)\
        .sort_values(columns.prep_datetime_start)\
        .reset_index(drop=True)
    data = data.astype({"day": 'Int16',
                        "weekdayMF": 'Int16',
                        "weekdayMTh": 'Int16',
                        "weekdaySTh": 'Int16'})  # prevent ints from converting to floats if there's NA values
    data[['day', 'weekdayMF', 'weekdayMTh', 'weekdaySTh', "app_engage_30s", "app_switched_app", "app_usage_flags"]] = \
        data[['day', 'weekdayMF', 'weekdayMTh', 'weekdaySTh',"app_engage_30s", "app_switched_app", "app_usage_flags"]].replace({np.NaN: None})
    data_fordownload = data[['study_id', 'participant_id', 'app_record_type', 'app_title', 'app_full_name',
                             'app_datetime_start', 'app_datetime_end', 'app_timezone', 'app_duration_seconds',
                             'day', 'weekdayMF', 'weekdayMTh', 'weekdaySTh',
                             'app_engage_30s', 'app_switched_app', 'app_usage_flags']]
    data_fordownload.sort_values(by=['app_datetime_start'], inplace=True)
    return data_fordownload
    
    
@task(log_stdout=True)
def get_person_preprocessed_data(
    person: "the participantID",
    datatable: "the raw data",
) -> pd.DataFrame:
    '''
    Select the data to be preprocessed, one person at a time, and run through preprocessor
    '''

    df = datatable.loc[datatable['participant_id'] == person]
    print(f"Person with ID {person} has {df.shape[0]} datapoints.")

    column_replacement = columns.column_rename
    df = df.rename(columns=column_replacement)

    ###---------- PREPROCESS HERE!! RETURNS DF. NEED TO ROWBIND
    df_prep = preprocess_dataframe.run(df, sessioninterval = [30])

    if isinstance(df_prep, None.__class__):
        return df_prep

    return df_prep
