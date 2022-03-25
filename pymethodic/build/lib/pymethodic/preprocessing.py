from datetime import timedelta
from dateutil import parser
from pytz import timezone
import pandas as pd
import numpy as np
import os
import time

from .constants import interactions, columns
from . import utils

def read_data(filenm):
    personid = "-".join(str(filenm).split(".")[-2].split("ChronicleData-")[1:])
    thisdata = pd.read_csv(filenm)
    thisdata['person'] = personid
    return thisdata
    

def clean_data(thisdata):
    '''
    This function transforms a csv file into a clean dataset:
    - only move-to-foreground and move-to-background actions
    - extracts person ID
    - extracts datetime information and rounds to 10ms
    - sorts events from the same 10ms by (1) foreground, (2) background
    '''
    utils.logger("Cleaning data", level = 1)
    thisdata = thisdata.dropna(subset=[columns.raw_record_type, columns.raw_date_logged])
    thisdata = thisdata.drop_duplicates(ignore_index = True)
    if len(thisdata)==0:
        return(thisdata)
    thisdata = thisdata[thisdata[columns.raw_record_type] != 'Usage Stat']
    if not columns.timezone in thisdata.keys() or any(thisdata[columns.timezone]==None):
        utils.logger("WARNING: Record has no timezone information.  Registering reported time.")
        thisdata[columns.timezone] = "UTC"
    if not columns.title in thisdata.columns:
        thisdata[columns.title] = ""
    thisdata[columns.title] = thisdata[columns.title].fillna("")
    thisdata = thisdata[[columns.title, columns.full_name, columns.raw_record_type, columns.raw_date_logged, 'person', columns.timezone]]
    # fill timezone by preceding timezone and then backwards
    thisdata = thisdata.sort_values(by=[columns.raw_date_logged]).reset_index(drop=True).fillna(method="ffill").fillna(method="bfill")
    thisdata['dt_logged'] = thisdata.apply(utils.get_dt,axis=1)
    thisdata['action'] = thisdata.apply(utils.get_action,axis=1)
    thisdata = thisdata.sort_values(by=['dt_logged', 'action']).reset_index(drop=True)

    return thisdata.drop(['action'],axis=1)

def get_timestamps(curtime, prevtime=False, row=None, precision=60):
    '''
    Function transforms an app usage statistic into bins (according to the desired precision).
    Returns a dataframe with the number of rows the number of time units (= precision).
    Precision in seconds.
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
            "participant_id": row['person'],
            columns.full_name: row[columns.full_name],
            columns.title: row[columns.title]
        }]

        return pd.DataFrame(outtime)

    #round down to precision

    prevtimehour = prevtime.replace(microsecond=0,second=0,minute=0)
    seconds_since_prevtimehour = np.floor((prevtime-prevtimehour).seconds/precision)*precision
    prevtimerounded = prevtimehour+timedelta(seconds=seconds_since_prevtimehour)

    # number of timepoints on precision scale (= new rows )
    timedif = (curtime-prevtimerounded)
    timepoints_n = int(np.floor(timedif.seconds/precision)+int(timedif.days*24*60*60/precision))

    # run over timepoints and append datetimestamps
    delta = timedelta(seconds=0)
    outtime = []

    for timepoint in range(timepoints_n+1):
        starttime = prevtime if timepoint == 0 else prevtimerounded+delta
        endtime = curtime if timepoint == timepoints_n else prevtimerounded+delta+timedelta(seconds=precision)

        outmetrics = {
            columns.prep_datetime_start: starttime,
            columns.prep_datetime_end: endtime,
            "date": starttime.strftime("%Y-%m-%d"),
            "starttime": starttime.strftime("%H:%M:%S.%f"),
            "endtime": endtime.strftime("%H:%M:%S.%f"),
            "day": (starttime.weekday()+1)%7+1,
            "weekdayMF": 1 if starttime.weekday() < 5 else 0,
            "weekdayMTh": 1 if starttime.weekday() < 4 else 0,
            "weekdaySTh": 1 if (starttime.weekday() < 4 or starttime.weekday()==6) else 0,
            "hour": starttime.hour,
            "quarter": int(np.floor(starttime.minute/15.))+1,
            columns.prep_duration_seconds: np.round((endtime - starttime).total_seconds())
        }

        outmetrics['participant_id'] = row['person']
        outmetrics[columns.full_name] = row[columns.full_name]
        outmetrics[columns.title] = row[columns.title]

        delta = delta+timedelta(seconds=precision)
        outtime.append(outmetrics)

    return pd.DataFrame(outtime)

def extract_usage(dataframe,precision=3600):
    '''
    function to extract usage from a filename.  Precision in seconds.
    '''

    cols = ['participant_id',
            columns.full_name,
            columns.title,
            'date',
            columns.prep_datetime_start,
            columns.prep_datetime_end,
            'starttime',
            'endtime',
            'day',  # note: starts on Sunday !
            'weekdayMF',
            'weekdayMTh',
            'weekdaySTh',
            'hour',
            'quarter',
            columns.prep_duration_seconds,
            columns.prep_record_type]

    other_interactions = {
        interactions.screen_non_interactive : "Screen Non-interactive",
        interactions.screen_interactive: "Screen Interactive",
        interactions.notification_seen: "Notification Seen",
        interactions.notification_interruption: "Notification Interruption"
    }

    alldata = []
    rawdata = clean_data(dataframe)
    openapps = {}
    latest_unbackgrounded = False

    for idx, row in rawdata.iterrows():
        
        interaction = row[columns.raw_record_type]
        app = row[columns.full_name]

        # decode timestamp and correct for timezone
        curtime = row.dt_logged
        curtime_zulustring = curtime.astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")

        if interaction == interactions.foreground:
            openapps[app] = {"open" : True,
                             "time": curtime}

            # iterate through older apps
            # if the app isn't the old app, then it's no longer opened, so "close" all of the other apps
            for olderapp, appdata in openapps.items():
                
                if app == olderapp:
                    continue
                                        
                if appdata['open'] == True and appdata['time'] < curtime:
                    
                    latest_unbackgrounded = {'unbgd_app':olderapp, 'fg_time':curtime, 'unbgd_time':appdata['time']}

                    # utils.logger(f"WARNING: App {app} is moved to foreground on {curtime_zulustring}\
                    #                 but {olderapp} was still open.  Discarding {olderapp} now...")

                    openapps[olderapp]['open'] = False

        # if it's an app in the background
        if interaction == interactions.background:

            if latest_unbackgrounded and app == latest_unbackgrounded['unbgd_app']:
                timediff = curtime - latest_unbackgrounded['fg_time']
                
                if timediff < timedelta(seconds=1):

                    timepoints = get_timestamps(curtime, latest_unbackgrounded['unbgd_time'], precision=precision, row=row)

                    timepoints[columns.prep_record_type] = 'App Usage'

                    alldata.append(timepoints)

                    openapps[app]['open'] = False

                    latest_unbackgrounded = False

            if app in openapps.keys() and openapps[app]['open']==True:

                # get time of opening
                prevtime = openapps[app]['time']

                if curtime-prevtime<timedelta(0):
                    raise ValueError("ALARM ALARM: timepoints out of order !!")

                # split up timepoints by precision
                timepoints = get_timestamps(curtime,prevtime,precision=precision,row=row)

                timepoints[columns.prep_record_type] = 'App Usage'

                alldata.append(timepoints)
                
                openapps[app]['open'] = False

        if interaction == interactions.power_off:
	        
            for app in openapps.keys():
            
                if openapps[app]['open'] == True:
                    
                    # get time of opening
                    prevtime = openapps[app]['time']
                    
                    if curtime-prevtime<timedelta(0):
                        raise ValueError("ALARM ALARM: timepoints out of order !!")
                    
                    # split up timepoints by precision
                    timepoints = get_timestamps(curtime,prevtime,precision=precision,row=row)
                    
                    timepoints[columns.prep_record_type] = 'Power Off'

                    alldata.append(timepoints)
                    
                    openapps[app] = {'open': False}
        
        # if the interaction is a part of screen non/interactive or notifications...
        # logic should be the same
        if interaction in other_interactions.keys():
            timepoints = get_timestamps(curtime, precision=precision, row=row)
            timepoints[columns.prep_record_type] = other_interactions[interaction]
            alldata.append(timepoints)

    alldata = pd.concat(alldata, axis = 0)
    if len(alldata)>0:
        alldata = alldata.sort_values(by=[columns.prep_datetime_start, columns.prep_datetime_end]).reset_index(drop=True)
        cols_to_select = list(set(cols).intersection(set(alldata.columns)))
        return alldata[cols_to_select].reset_index(drop=True)


def check_overlap_add_sessions(data, session_def = [5*60]):
    '''
    Function to loop over dataset, spot overlaps (and remove them), and add columns
    to indicate whether a new session has been started or not.
    '''
    data = data[data[columns.prep_duration_seconds] > 0].reset_index(drop=True)

    # initiate session column(s)
    for sess in session_def:
        data['app_engage_%is'%int(sess)] = 0

    data[columns.switch_app] = 0
    # loop over dataset:
    # - prevent overlap (with warning)
    # - check if a new session is started
    for idx,row in data.iterrows():
        if idx == 0:
            for sess in session_def:
                data.at[idx, 'app_engage_%is'%int(sess)] = 1

        # check time between previous and this app usage
        nousetime = row[columns.prep_datetime_start].astimezone(timezone("UTC")) - data[columns.prep_datetime_end].iloc[idx - 1].astimezone(timezone("UTC"))

        # check overlap
        if nousetime < timedelta(microseconds=0) and row[columns.prep_datetime_start].date == row[columns.prep_datetime_end].date:
            utils.logger("WARNING: Overlapping usage for participant %s: %s was open since %s when %s was openened on %s. \
            Manually closing %s..."%(
                row['participant_id'],
                data.iloc[idx-1][columns.full_name],
                data.iloc[idx-1][columns.prep_datetime_start].strftime("%Y-%m-%d %H:%M:%S"),
                row[columns.full_name],
                row[columns.prep_datetime_start].strftime("%Y-%m-%d %H:%M:%S"),
                data.iloc[idx-1][columns.full_name]
            ))
            data.at[idx-1,columns.prep_datetime_end] = row[columns.prep_datetime_start]
            data.at[idx-1, columns.prep_duration_seconds] = (data.at[idx - 1, columns.prep_datetime_end] - data.at[idx - 1, columns.prep_datetime_start]).seconds

        # check sessions
        else:
            for sess in session_def:
                if nousetime > timedelta(seconds = sess):
                    data.at[idx, 'app_engage_%is'%int(sess)] = 1

        # check appswitch
        data.at[idx, columns.switch_app] = 1-(row[columns.full_name]==data[columns.full_name].iloc[idx-1])*1
    return data.reset_index(drop=True)

def log_exceed_durations_minutes(row, threshold, outfile):
    timestamp = row[columns.prep_datetime_start].astimezone(tz=timezone("UTC")).strftime("%Y-%m-%d %H:%M:%S")
    with open(outfile, "a+") as fl:
        fl.write("Person {participant} used {app} more than {threshold} minutes on {timestamp}\n".format(
            participant = row['participant_id'],
            app = row[columns.full_name],
            threshold = threshold,
            timestamp = timestamp
        ))

def preprocess_dataframe(dataframe, precision=3600,sessioninterval = [5*60], logdir=None, logopts={}):
    dataframe = utils.backwards_compatibility(dataframe)
    utils.logger("LOG: Extracting usage...",level=1)
    tmp = extract_usage(dataframe,precision=precision)
    if not isinstance(tmp,pd.DataFrame) or np.sum(tmp[columns.prep_duration_seconds]) == 0:
        return None
        utils.logger("WARNING: File %s does not seem to contain relevant data.  Skipping..."%filename)
    utils.logger("LOG: checking overlap session...",level=1)
    data = check_overlap_add_sessions(tmp,session_def=sessioninterval)
    data = utils.add_warnings(data)
    non_timed = tmp[tmp[columns.prep_duration_seconds].isna()]
    flagcols = [x for x in non_timed.columns if 'engage' in x or 'switch' in x]
    non_timed[flagcols] = None
    data = pd.concat([data, non_timed], ignore_index=True, sort=False)\
        .sort_values(columns.prep_datetime_start)\
        .reset_index(drop=True)

    return data
    
    
def preprocess_folder(infolder,outfolder,precision=3600,sessioninterval = [5*60], logdir=None, logopts={}):

    if not os.path.exists(outfolder):
        os.mkdir(outfolder)

    for filename in [x for x in os.listdir(infolder) if x.startswith("Chronicle")]:
        utils.logger("LOG: Preprocessing file %s..."%filename,level=1)
        dataframe = read_data(os.path.join(infolder,filename))
        data = preprocess_dataframe(dataframe, precision=precision,sessioninterval = sessioninterval, logdir=logdir, logopts=logopts)
        if data is not None:
            outfilename = filename.replace('ChronicleData','ChronicleData_preprocessed')
            data.to_csv(os.path.join(outfolder,outfilename),index=False)

def add_preprocessed_columns(data):
    if data.shape[0] == 0:
        return data
    data[columns.prep_datetime_start] = data[columns.prep_datetime_start].astype(str).replace('nan', )
    data[columns.prep_datetime_end] = data[columns.prep_datetime_end].astype(str).replace('nan',None)

    data[columns.prep_datetime_start] = pd.to_datetime(data[columns.prep_datetime_start].replace('nan', ''), infer_datetime_format = True, utc = True)
    data[columns.prep_datetime_end] = pd.to_datetime(data[columns.prep_datetime_end].replace('nan', ''), infer_datetime_format = True, utc = True)
    data['duration_minutes'] = data.apply(lambda x: x[columns.prep_duration_seconds] / 60., axis = 1)
    data['firstdate'] = min(data[columns.prep_datetime_start]).date()
    data['lastdate'] = max(data[columns.prep_datetime_end][~data[columns.prep_datetime_end].isna()]).date()
    data['date'] = data.apply(lambda x: x[columns.prep_datetime_start].date(), axis =1)
    data[columns.prep_datetime_start] = data.apply(lambda x: x[columns.prep_datetime_start], axis = 1)
    data[columns.prep_datetime_end] = data.apply(lambda x: x[columns.prep_datetime_end], axis = 1)
    data["day"] = data.apply(lambda x: (x[columns.prep_datetime_start].weekday() + 1) % 7 + 1, axis = 1)
    data["weekdayMF"] = data.apply(lambda x: 1 if x[columns.prep_datetime_start].weekday() < 5 else 0, axis = 1)
    data["weekdayMTh"] = data.apply(lambda x: 1 if x[columns.prep_datetime_start].weekday() < 4 else 0, axis = 1)
    data["weekdaySTh"] = data.apply(lambda x: 1 if (x[columns.prep_datetime_start].weekday() < 4 or x[columns.prep_datetime_start].weekday() == 6) else 0, axis = 1)
    data["hour"] = data.apply(lambda x: x[columns.prep_datetime_start].hour, axis = 1)
    data["quarter"] = data.apply(lambda x: utils.round_down_to_quarter(x[columns.prep_datetime_start]), axis = 1)
    data = utils.add_session_durations(data)
    return data

    # if 'log_exceed_durations_minutes' in logopts.keys():
    #     if not os.path.exists(logdir):
    #         os.mkdir(logdir)
    #     for threshold in logopts['log_exceed_durations_minutes']:
    #         subset = data[data.duration_minutes > float(threshold)]
    #         outfile = os.path.join(logdir, "log_exceed_durations_minutes_%s.txt" % threshold)
    #         if len(subset) > 0:
    #             for idx, row in data[data.duration_minutes > threshold].iterrows():
    #                 log_exceed_durations_minutes(row, threshold, outfile)

