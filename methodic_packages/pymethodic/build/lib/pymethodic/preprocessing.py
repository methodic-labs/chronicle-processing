from datetime import timedelta
from pytz import timezone
from prefect import task
import pandas as pd
import numpy as np
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
    utils.logger.run("Cleaning data", level=1)
    thisdata = thisdata.dropna(subset=['app_record_type', 'app_date_logged'])
    thisdata = thisdata.drop_duplicates(ignore_index=True)
    if len(thisdata) == 0:
        return (thisdata)
    thisdata = thisdata[thisdata['app_record_type'] != 'Usage Stat']
    if not 'app_timezone' in thisdata.keys() or any(thisdata['app_timezone'] == None):
        utils.logger.run("WARNING: Record has no timezone information.  Registering reported time as UTC.")
        thisdata['app_timezone'] = "UTC"
    if not 'app_title' in thisdata.columns:
        thisdata['app_title'] = ""
    thisdata['app_title'] = thisdata['app_title'].fillna("")
    thisdata = thisdata[['study_id', 'participant_id', 'app_title', 'app_full_name', 'app_record_type', 'app_date_logged', 'app_timezone']]
    # fill timezone by preceding timezone and then backwards
    thisdata = thisdata.sort_values(by=['app_date_logged']). \
        reset_index(drop=True). \
        fillna(method="ffill").fillna(method="bfill")
    thisdata['date_tzaware'] = thisdata.apply(utils.get_dt, axis=1)  # transforms string to a timestamp, returns local time
    thisdata['action'] = thisdata.apply(utils.get_action, axis=1)
    thisdata = thisdata.sort_values(by=['date_tzaware', 'action']).reset_index(drop=True)

    return thisdata.drop(['action'], axis=1)

@task
def get_timestamps(curtime, prevtime=False, row=None, precision=60):
    '''
    Function transforms an app usage statistic into bins (according to the desired precision).
    Returns a dataframe with the number of rows the number of time units (= precision in seconds).
    USE LOCAL TIME to extract the correct date (include timezone)
    '''
    if not prevtime:
        starttime = curtime
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
            columns.title: row[columns.title]
        }]

        return pd.DataFrame(outtime)

    # round down to precision
    prevtimehour = prevtime.replace(microsecond=0, second=0, minute=0)
    seconds_since_prevtimehour = np.floor((prevtime - prevtimehour).seconds / precision) * precision
    prevtimerounded = prevtimehour + timedelta(seconds=seconds_since_prevtimehour)

    # number of timepoints on precision scale (= new rows )
    # Make timedif check in UTC in case the timezones differ
    timedif = (pendulum.instance(curtime).in_timezone('UTC') - pendulum.instance(prevtimerounded).in_timezone('UTC'))

    timepoints_n = int(np.floor(timedif.seconds / precision) + int(timedif.days * 24 * 60 * 60 / precision))

    # run over timepoints and append datetimestamps
    delta = timedelta(seconds=0)
    outtime = []

    # START TIME IS SOMETIMES PREVIOUS TIME INSTEAD OF DATE LOGGED - HERE'S WHERE THE DISCREPANCY COMES IN.
    for timepoint in range(timepoints_n + 1):
        starttime = prevtime if timepoint == 0 else prevtimerounded + delta
        endtime = curtime if timepoint == timepoints_n else prevtimerounded + delta + timedelta(seconds=precision)

        outmetrics = {
            columns.prep_datetime_start: starttime,
            columns.prep_datetime_end: endtime,
            "date": starttime.strftime("%Y-%m-%d"),
            "starttime": starttime.strftime("%H:%M:%S.%f"),
            "endtime": endtime.strftime("%H:%M:%S.%f"),
            "day": (starttime.weekday() + 1) % 7 + 1,
            "weekdayMF": 1 if starttime.weekday() < 5 else 0,
            "weekdayMTh": 1 if starttime.weekday() < 4 else 0,
            "weekdaySTh": 1 if (starttime.weekday() < 4 or starttime.weekday() == 6) else 0,
            "hour": starttime.hour,
            "quarter": int(np.floor(starttime.minute / 15.)) + 1,
            columns.prep_duration_seconds: np.round((endtime - starttime).total_seconds())
        }

        outmetrics['participant_id'] = row['participant_id']
        outmetrics[columns.full_name] = row[columns.full_name]
        outmetrics[columns.title] = row[columns.title]

        delta = delta + timedelta(seconds=precision)
        outtime.append(outmetrics)

    return pd.DataFrame(outtime)

@task
def extract_usage(dataframe, precision=3600):
    '''
    Function to extract usage from a filename.
    Precision in seconds.
    '''

    # If cols don't exist, they're added and filled with NaNs
    cols = ['participant_id',
            'app_full_name',
            'application_label',
            'date',
            'app_datetime_start',  # was 'app_start_timestamp',
            'app_datetime_end',  # was 'app_end_timestamp',
            'starttime',
            'endtime',
            'day',  # note: starts on Sunday !
            'weekdayMF',
            'weekdayMTh',
            'weekdaySTh',
            'hour',
            'quarter',
            'app_duration_seconds',
            'app_record_type']  # was 'app_data_type'

    other_interactions = {
        'Unknown importance: 16': "Screen Non-interactive",
        'Unknown importance: 15': "Screen Interactive",
        'Unknown importance: 10': "Notification Seen",
        'Unknown importance: 12': "Notification Interruption"
    }

    alldata = []

    rawdata = clean_data.run(dataframe)
    openapps = {}
    latest_unbackgrounded = False

    for idx, row in rawdata.iterrows():

        interaction = row['app_record_type']
        app = row['app_full_name']

        # USE LOCAL TIME - must be for `get_timestamps` to be correct when it infers dates
        curtime = row.date_tzaware  # app_date_logged.

        # make dict entry of every app that was moved to foreground. Time is local.
        if interaction == 'Move to Foreground':
            openapps[app] = {"open": True,
                             "time": curtime}

            # Iterate through older apps
            # if the app isn't the old app, then it's no longer opened, so "close" all of the other apps
            # only the latest Foregrounded is "open"
            for olderapp, appdata in openapps.items():
                if app == olderapp:
                    continue
                # dict of the ONE app immediately preceding the "open" one.
                # fg_time = timestamp of the app after it = timestamp of when it became not in the foreground
                # unbgd_time = its own timestamp = time when it went to foreground (aka was "unbackgrounded")
                if appdata['open'] == True and appdata['time'] < curtime:
                    latest_unbackgrounded = {'unbgd_app': olderapp, 'fg_time': curtime, 'unbgd_time': appdata['time']}
                    openapps[olderapp]['open'] = False

        # if it's an app in the background
        if interaction == 'Move to Background':

            # ONLY applies to the one latest unbackgrounded app.
            if latest_unbackgrounded and app == latest_unbackgrounded['unbgd_app']:
                # Make timediff check in UTC in case the timezones differ
                timediff = pendulum.instance(curtime).in_timezone('UTC') - pendulum.instance(
                    latest_unbackgrounded['fg_time']).in_timezone('UTC')

                if timediff < timedelta(seconds=1):
                    timepoints = get_timestamps.run(curtime, latest_unbackgrounded['unbgd_time'], precision=precision,
                                                    row=row)

                    timepoints[columns.prep_record_type] = 'App Usage'
                    timepoints['app_timezone'] = row.app_timezone
                    timepoints['study_id'] = row.study_id
                    timepoints['app_title'] = row.app_title

                    alldata.append(timepoints)

                    openapps[app]['open'] = False

                    latest_unbackgrounded = False  # reset to empty

            # ONLY the one open app.
            if app in openapps.keys() and openapps[app]['open'] == True:

                # get time it was opened - the raw data timestamp
                prevtime = openapps[app]['time']

                # If the start/end of app usage is in different timezones, convert to the end timezone
                # it they are the same, this changes nothing.
                cur_tzname = curtime.tzinfo.zone
                prevtime = prevtime.astimezone(cur_tzname)

                # Make timediff check in UTC in case the timezones differ
                if pendulum.instance(curtime).in_timezone('UTC') - pendulum.instance(prevtime).in_timezone(
                        'UTC') < timedelta(0):
                    raise ValueError("ALARM ALARM: timepoints out of order !!")

                # split up timepoints by precision
                timepoints = get_timestamps.run(curtime, prevtime, precision=precision, row=row)

                timepoints[columns.prep_record_type] = 'App Usage'
                timepoints['app_timezone'] = row.app_timezone
                timepoints['study_id'] = row.study_id
                timepoints['app_title'] = row.app_title

                alldata.append(timepoints)

                openapps[app]['open'] = False

        if interaction == 'Unknown importance: 26':  # power_off

            for app in openapps.keys():

                if openapps[app]['open'] == True:

                    # get time of opening
                    prevtime = openapps[app]['time']

                    if curtime - prevtime < timedelta(0):
                        raise ValueError("ALARM ALARM: timepoints out of order !!")

                    # split up timepoints by precision
                    timepoints = get_timestamps.run(curtime, prevtime, precision=precision, row=row)

                    timepoints[columns.prep_record_type] = 'Power Off'
                    timepoints['app_timezone'] = row.app_timezone
                    timepoints['study_id'] = row.study_id
                    timepoints['app_title'] = row.app_title

                    alldata.append(timepoints)

                    openapps[app] = {'open': False}

        # if the interaction is a part of screen non/interactive or notifications...
        # logic should be the same
        if interaction in other_interactions.keys():
            timepoints = get_timestamps.run(curtime, precision=precision, row=row)
            timepoints[columns.prep_record_type] = other_interactions[interaction]
            timepoints['app_timezone'] = row.app_timezone
            timepoints['study_id'] = row.study_id
            timepoints['app_title'] = row.app_title
            alldata.append(timepoints)

    if len(alldata) > 0:
        alldata = pd.concat(alldata, axis=0)
        alldata = alldata.sort_values(by=[columns.prep_datetime_start, columns.prep_datetime_end]).reset_index(
            drop=True)
        cols_to_select = list(set(cols).intersection(set(alldata.columns)))
        cols_to_select.extend(['app_timezone', 'app_title'])
        alldata[cols_to_select].reset_index(drop=True)
        return alldata


@task
def check_overlap_add_sessions(data, session_def=[5 * 60]):
    '''
    Function to loop over dataset, spot overlaps (and remove them), and add columns
    to indicate whether a new session has been started or not.
    Make a column, "app_switched_app", 0 if not switched from previous row, 1 if so.
    '''
    data_nonzero = data[data[columns.prep_duration_seconds] > 0].reset_index(
        drop=True)  # filter only where there's some durations

    # # Create session column(s) - one per session input argument. Usually one.
    for sess in session_def:
        data_nonzero[f'app_engage_{int(sess)}s'] = 0

    data_nonzero[columns.switch_app] = 0
    # Loop over dataset:
    # - prevent overlap (with warning)
    # - check if a new session is started
    for idx, row in data_nonzero.iterrows():
        if idx == 0:
            for sess in session_def:
                data_nonzero.at[idx, f'app_engage_{int(sess)}s'] = 1  # 1st row: automatically set 'app_engage_300'==1

        # Create timedelta of starttime - endtime(previous row)
        nousetime = row[columns.prep_datetime_start].astimezone(timezone("UTC")) - \
                    data_nonzero[columns.prep_datetime_end].iloc[idx - 1].astimezone(timezone("UTC"))

        # If an entry started before the previous row closed - 1)warn,
        # ...2) overwrite/shorten endtime to next start time, 3) recalc duration
        if nousetime < timedelta(microseconds=0) and row[columns.prep_datetime_start].date == row[
            columns.prep_datetime_end].date:
            utils.logger(
                f'''WARNING: Overlapping usage for participant {row['participant_id']}: {data_nonzero.iloc[idx - 1][columns.full_name]}
                    was open since {data_nonzero.iloc[idx - 1][columns.prep_datetime_start].strftime("%Y-%m-%d %H:%M:%S")} 
                    when {row[columns.full_name]} was opened on {row[columns.prep_datetime_start].strftime("%Y-%m-%d %H:%M:%S")}. \
                    Manually closing {data_nonzero.iloc[idx - 1][columns.full_name]}...''')
            data_nonzero.at[idx - 1, columns.prep_datetime_end] = row[columns.prep_datetime_start]
            data_nonzero.at[idx - 1, columns.prep_duration_seconds] = (
                        data_nonzero.at[idx - 1, columns.prep_datetime_end] - data_nonzero.at[
                    idx - 1, columns.prep_datetime_start]).seconds

        # # If the timedelta is > the "session" arg (5 min default), flag w/1
        else:
            for sess in session_def:
                if nousetime > timedelta(seconds=sess):
                    data_nonzero.at[idx, f'app_engage_{int(sess)}s'] = 1

        # Populate app_switched_app for all rows. If previous row is the same app, make it 0. If not 1.
        data_nonzero.at[idx, columns.switch_app] = 1 - (
                    row[columns.full_name] == data_nonzero[columns.full_name].iloc[idx - 1]) * 1

    return data_nonzero.reset_index(drop=True)


@task
def log_exceed_durations_minutes(row, threshold, outfile):
    timestamp = row[columns.prep_datetime_start].astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    with open(outfile, "a+") as fl:
        fl.write("Person {participant} used {app} more than {threshold} minutes on {timestamp}\n".format(
            participant=row['participant_id'],
            app=row[columns.full_name],
            threshold=threshold,
            timestamp=timestamp
        ))


@task
def preprocess_dataframe(dataframe, precision=3600, sessioninterval=[5 * 60]):
    # dataframe = utils.backwards_compatibility(dataframe)
    utils.logger.run("LOG: Extracting usage...", level=1)
    tmp = extract_usage.run(dataframe, precision=precision)
    if not isinstance(tmp, pd.DataFrame) or np.sum(tmp[columns.prep_duration_seconds]) == 0:
        utils.logger.run(
            f"WARNING: Person {dataframe['participant_id'].iloc[0]} does not seem to contain relevant data.  Skipping...")
        return None
    utils.logger.run("LOG: checking overlap session...", level=1)
    data = check_overlap_add_sessions.run(tmp, session_def=sessioninterval)
    data = utils.add_warnings.run(data)
    non_timed = tmp[tmp[columns.prep_duration_seconds].isna()]
    flagcols = [x for x in non_timed.columns if 'engage' in x or 'switch' in x]
    non_timed.loc[:, (flagcols)] = None
    data = pd.concat([data, non_timed], ignore_index=True, sort=False) \
        .sort_values(columns.prep_datetime_start) \
        .reset_index(drop=True)
    data = data.astype({"day": 'Int16',
                        "weekdayMF": 'Int16',
                        "weekdayMTh": 'Int16',
                        "weekdaySTh": 'Int16'})  # prevent ints from converting to floats if there's NA values
    data[['day', 'weekdayMF', 'weekdayMTh', 'weekdaySTh', "app_engage_30s", "app_switched_app", "app_usage_flags"]] = \
        data[['day', 'weekdayMF', 'weekdayMTh', 'weekdaySTh', "app_engage_30s", "app_switched_app",
              "app_usage_flags"]].replace({np.NaN: None})
    data_fordownload = data[['study_id', 'participant_id', 'app_record_type', 'app_title', 'app_full_name',
                             'app_datetime_start', 'app_datetime_end', 'app_timezone', 'app_duration_seconds',
                             'day', 'weekdayMF', 'weekdayMTh', 'weekdaySTh',
                             'app_engage_30s', 'app_switched_app', 'app_usage_flags']]
    return data_fordownload



