from datetime import datetime, timedelta, timezone
from .constants import columns, interactions
from collections import Counter
import dateutil.parser
import pandas as pd
import numpy as np
import pytz
import os
import re

def get_dt(row):
    '''
    This function transforms the reported (string) datetime to a timestamp.
    A few notes:
    - Time is rounded to 10 milliseconds, to make sure the apps are in the right order.
      A potential downside of this is that when a person closes and re-opens an app
      within 10 milliseconds, it will be regarded as closed.
    '''    
    zulutime = dateutil.parser.parse(row[columns.raw_date_logged])
    localtime = zulutime.replace(tzinfo=timezone.utc).astimezone(tz=pytz.timezone(row[columns.timezone]))
    # microsecond = min(round(localtime.microsecond / 10000)*10000, 990000)
    # localtime = localtime.replace(microsecond = microsecond)
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

def recode(row,recode):
    newcols = {x:None for x in recode.columns}
    if row[columns.full_name] in recode.index:
        for col in recode.columns:
            newcols[col] = recode[col][row[columns.full_name]]

    return pd.Series(newcols)

def logger(message,level=1):
    time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    prefix = "༼ つ ◕_◕ ༽つ" if level==0 else "-- "
    print("%s %s: %s"%(prefix,time,message))

def fill_dates(dataset,datelist):
    '''
    This function checks for empty days and fills them with 0's.
    '''
    for date in datelist:
        if not date in dataset.index:
            newrow = pd.Series({k:0 for k in dataset.columns}, name=date)
            dataset = dataset.append(newrow)
    return dataset


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

def round_down_to_quarter(x):
    if pd.isna(x):
        return None
    return int(np.floor(x.minute / 15.)) + 1


def combine_flags(row):
    flags = []
    if row.no_usage:
        flags.append("LARGE TIME GAP")
    if row.long_usage:
        flags.append("LONG APP DURATION")
    return flags


def add_warnings(df):
    df['no_usage'] = pd.to_datetime(df[columns.prep_datetime_start], utc=True) - \
                     pd.to_datetime(df[columns.prep_datetime_end].shift(), utc= True) > \
                     timedelta(days=1)
    df['long_usage'] = df[columns.prep_duration_seconds] > 3 * 60 * 60
    df[columns.flags] = df.apply(combine_flags, axis=1)
    df = df.drop(['no_usage', 'long_usage'], axis=1)
    return df


