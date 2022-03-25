full_name = "app_full_name"
title = "app_title"
timezone = "app_timezone"
participant_id = "participant_id"

prep_datetime_start = "app_start_timestamp"
prep_datetime_end = "app_end_timestamp"
prep_record_type = "app_data_type"
prep_date_logged = "app_date"
prep_duration_seconds = "app_duration_seconds"

raw_datetime_start = "app_datetime_start"
raw_datatime_end = 'app_datetime_end'
raw_date_logged = "app_date_logged"
raw_record_type = "app_record_type"

switch_app = "app_switched_app"
engage_30s = "app_engage_30s"
flags = "app_usage_flags"

column_rename = {'app_package_name': 'app_full_name',
                 'interaction_type': 'app_record_type',
                 # 'ol.altitude': 'app_altitude',
                 # 'location.latitude': 'app_latitude',
                 # 'location.longitude': 'app_longitude',
                 'event_timestamp': 'app_date_logged',
                 # 'general.Duration': 'app_duration_seconds',
                 # 'ol.datetimestart': 'app_datetime_start',
                 # 'general.EndTime': 'app_datetime_end',
                 'timezone': 'app_timezone',
                 'application_label': 'app_title',
                 'ol.newapp': 'app_new_app',
                 'ol.newperiod': 'app_new_period',
                 'ol.warning': 'app_warning'
                 }