from datetime import datetime, timedelta, timezone
from .constants import columns
from prefect import task
import dateutil.parser
import dateutil.tz
import pandas as pd
import numpy as np
import pendulum

# @task
# def logger(message, level=1):
#     time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
#     prefix = "༼ つ ◕_◕ ༽つ" if level==0 else "-- "
#     print("%s %s: %s"%(prefix,time,message))

def get_dt(row):
    '''
    Transforms the reported datetime to a timestamp, IN LOCAL TIME based on the reported timezone in the row.
    A few notes:
    - Time is rounded to 10 milliseconds, to make sure the apps are in the right order.
      A potential downside of this is that when a person closes and re-opens an app
      within 10 milliseconds, it will be regarded as closed.
    '''
    if type(row['app_date_logged']) is str:
        zulutime = dateutil.parser.parse(row[columns.raw_date_logged])
        localtime = zulutime.replace(tzinfo=timezone.utc).astimezone(dateutil.tz.gettz(row[columns.timezone]))
        # microsecond = min(round(localtime.microsecond / 10000)*10000, 990000)
        # localtime = localtime.replace(microsecond = microsecond)
        return localtime
    if (type(row['app_date_logged']) is datetime) or (type(row['app_date_logged']) is pd.Timestamp):
        localtime = row[columns.raw_date_logged].astimezone(dateutil.tz.gettz(row[columns.timezone]))
        return localtime


def get_action(row):
    '''
    This function creates a column with a value 0 for foreground action, 1 for background
    action.  This can be used for sorting (when times are equal: foreground before background)
    '''
    if row[columns.raw_record_type]== 'Move to Foreground':
        return 0
    if row[columns.raw_record_type]== 'Move to Background':
        return 1


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


@task
def query_export_table(start, end, timezone, counting=True, users=[], studies=[]):
    '''Helper function -  SQL query passed to the usage exports table
    makes queries - to count and also to delete data.
    For writing to the preprocessing backend table in Redshift
    '''
    # Timestamp must be converted to the timezone of the production db (which is UTC) for proper comparison
    timezone_of_data = timezone

    start_utc = pendulum.parse(start, tz=timezone_of_data).in_tz('UTC') #must be timezone of the system
    end_utc = pendulum.parse(end, tz=timezone_of_data).in_tz('UTC')

    selection = "SELECT count(*)" if counting else "DELETE"

    if len(users) > 0:
        person_filter = f''' and \"participant_id\" = \'{users}\''''
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


def combine_flags(row):
    flags = []
    if row.no_usage_6hrs and not row.no_usage_12hrs:
        flags.append("6-HR TIME GAP")
    if row.no_usage_12hrs and row.no_usage_6hrs and not row.no_usage_1day:
        flags.append("12-HR TIME GAP")
    if row.no_usage_1day and row.no_usage_6hrs and row.no_usage_12hrs:
        flags.append("1-DAY TIME GAP")
    return flags

@task
def add_warnings(df):
    timediff = pd.to_datetime(df[columns.prep_datetime_start], utc=True) - \
        pd.to_datetime(df[columns.prep_datetime_end].shift(), utc=True)
    df['no_usage_1day'] = timediff >= timedelta(days=1)   # This previously was "LARGE TIME GAP"
    df['no_usage_6hrs'] = timediff >= timedelta(hours=6)
    df['no_usage_12hrs'] = timediff >= timedelta(hours=12)
    df[columns.flags] = np.where(df[columns.flags] == "3-HR APP DURATION",
                                 "3-HR APP DURATION",
                                 df.apply(combine_flags, axis=1))
    df = df.drop(['no_usage_1day', 'no_usage_6hrs', 'no_usage_12hrs'], axis=1)
    return df


@task
def recode(row,recode):
    newcols = {x:None for x in recode.columns}
    if row[columns.full_name] in recode.index:
        for col in recode.columns:
            newcols[col] = recode[col][row[columns.full_name]]

    return pd.Series(newcols)


@task
def fill_dates(dataset,datelist):
    '''
    This function checks for empty days and fills them with 0's.
    '''
    for date in datelist:
        if not date in dataset.index:
            newrow = pd.Series({k:0 for k in dataset.columns}, name=date)
            dataset = dataset.append(newrow)
    return dataset

@task
def fill_hours(dataset,datelist):
    '''
    This function checks for empty days/hours and fills them with 0's.
    '''
    for date in datelist:
        datestr = date.strftime("%Y-%m-%d")
        for hour in range(24):
            multind = (datestr,hour)
            if not multind in dataset.index:
                newrow = pd.Series({k:0 for k in dataset.columns}, name=multind)
                dataset = dataset.append(newrow)
    return dataset

@task
def fill_quarters(dataset,datelist):
    '''
    This function checks for empty days/hours/quarters and fills them with 0's.
    '''
    for date in datelist:
        datestr = date.strftime("%Y-%m-%d")
        for hour in range(24):
            for quarter in range(1,5):
                multind = (datestr,hour,quarter)
                if not multind in dataset.index:
                    newrow = pd.Series({k:0 for k in dataset.columns}, name=multind)
                    dataset = dataset.append(newrow)
    return dataset

@task
def fill_appcat_hourly(dataset,datelist,catlist):
    '''
    This function checks for empty days/hours and fills them with 0's for all categories.
    '''
    for date in datelist:
        datestr = date.strftime("%Y-%m-%d")
        for hour in range(24):
            for cat in catlist:
                multind = (datestr,str(cat),hour)
                if not multind in dataset.index:
                    newrow = pd.Series({k:0 for k in dataset.columns}, name=multind)
                    dataset = dataset.append(newrow)
    return dataset

@task
def fill_appcat_quarterly(dataset,datelist,catlist):
    '''
    This function checks for empty days/hours/quarters and fills them with 0's for all categories.
    '''
    for date in datelist:
        datestr = date.strftime("%Y-%m-%d")
        for hour in range(24):
            for quarter in range(1,5):
                for cat in catlist:
                    multind = (datestr,str(cat),hour,quarter)
                    if not multind in dataset.index:
                        newrow = pd.Series({k:0 for k in dataset.columns}, name=multind)
                        dataset = dataset.append(newrow)
    return dataset

@task
def cut_first_last(dataset, includestartend, maxdays, first, last):
    first_parsed = dateutil.parser.parse(str(first))
    last_parsed = dateutil.parser.parse(str(last))

    first_obs = min(dataset[columns.prep_datetime_start])
    last_obs = max(dataset[columns.prep_datetime_end])

    # cutoff start: upper bound of first timepoint if not includestartend
    first_cutoff = first_parsed if includestartend \
        else first_parsed.replace(hour=0, minute=0, second=0, microsecond=0)+timedelta(days=1)
    first_cutoff = first_cutoff.replace(tzinfo = first_obs.tzinfo)
    
    # cutoff end: lower bound of last timepoint if not includestartend
    last_cutoff = last_parsed if includestartend \
        else last_parsed.replace(hour=0, minute=0, second=0, microsecond=0)
    
    # last day to be included in datelist: day before last timepoint if not includestartend
    last_day = last_parsed if includestartend \
        else last_parsed.replace(hour=0, minute=0, second=0, microsecond=0)-timedelta(days=1)
    last_day = last_day.replace(tzinfo = first_obs.tzinfo)
        
    if maxdays is not None:
        last_cutoff = first_cutoff + timedelta(days = maxdays)
        last_day = (first_cutoff + timedelta(days = maxdays)).replace(tzinfo = first_obs.tzinfo)
    
    if (len(dataset[columns.prep_datetime_end]) == 0):
        datelist = []
    else:
        enddate_fix = min(
            last_day,
            max(dataset[columns.prep_datetime_end])
        )
        datelist = pd.date_range(start = first_cutoff, end = enddate_fix, freq='D')
    
    dataset = dataset[
        (dataset[columns.prep_datetime_start] >= first_cutoff) & \
        (dataset[columns.prep_datetime_end] <= last_day)].reset_index(drop=True)
            
    return dataset, datelist

@task
def add_session_durations(dataset):
    engagecols = [x for x in dataset.columns if x.startswith('engage')]
    for sescol in engagecols:
        newcol = '%s_dur'%sescol
        sesids = np.where(dataset[sescol]==1)[0][1:]
        starttimes = np.array(dataset[columns.prep_datetime_start].loc[np.append([0], sesids)][:-1], dtype='datetime64[ns]')
        endtimes = np.array(dataset[columns.prep_datetime_end].loc[sesids-1], dtype='datetime64[ns]')
        durs = (endtimes-starttimes)/ np.timedelta64(1, 'm')
        dataset[newcol] = 0
        for idx,sesid in enumerate(np.append([0],sesids)):
            if idx == len(sesids):
                continue
            lower = sesid
            upper = len(dataset) if idx == len(sesids)-1 else sesids[idx+1]
            dataset.loc[np.arange(lower,upper),newcol] = durs[idx]
    return dataset

@task
def backwards_compatibility(dataframe):
    dataframe = dataframe.rename(
        columns = {
            'general.fullname': columns.full_name,
            'ol.recordtype': columns.prep_record_type,
            'ol.datelogged': columns.prep_date_logged,
            'general.Duration': columns.prep_duration_seconds,
            'ol.datetimestart': columns.prep_datetime_start,
            'general.EndTime': columns.prep_datetime_end,
            'ol.timezone': columns.timezone,
            'app_fullname': columns.full_name,
            'start_timestamp': columns.prep_datetime_start,
            'end_timestamp': columns.prep_datetime_end,
            'duration_seconds': columns.prep_duration_seconds,
            'switch_app': columns.switch_app,
            'ol.title': columns.title
        },
        errors = 'ignore'
    )
    return dataframe

@task
def round_down_to_quarter(x):
    if pd.isna(x):
        return None
    return int(np.floor(x.minute / 15.)) + 1




