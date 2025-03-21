# Chronicle preprocessing in Methodic
This repository contains code to process data from the Chronicle app, via Methodic.  
  
Installation of the `pymethodic2` module (located here) is required.
1. Clone this repository.
2. In Terminal (Mac) or the command prompt (Windows) navigate to the `methodic_packages/pymethodic2` folder
3. Type in `pip3 install .`
4. To preprocess raw data on your local computer, use `Notebooks/chronicle_preprocessing_local.ipynb`. 


The following fields are the outputs of Chronicle preprocessing:
- *participant_id:* Taken from the raw device data.
- *interaction_type:* _(previously named `app_data_type`)_ The type of app interaction (i.e., "App Usage", "Screen Inactive").
- *app_title:* The title of the app
- *app_fullname:* The name of the app as it appears in the raw data.
- *app_datetime_start:* _(previously named `app_start_timestamp`)_ The start timestamp of the app usage - in the timezone specified in the `app_timezone` column.
- *app_datetime_end:* _(previously named `app_end_timestamp`)_ The end timestamp of the app usage - in the timezone specified in the `app_timezone` column.
- *app_timezone:* The timezone taken from the raw device data.
- *app_duration_seconds:* Duration (in seconds)
- *day:* The day of the week.  The week starts on Sunday, i.e. 1 = Sunday, 2 = Monday, etc.
- *weekdayMF:* Whether or not this usage falls on a weekday: 1 = Mon-Fri, 0 = Sat+Sun
- *weekdayMTh:* Whether or not this usage falls on a weekday: 1 = Mon-Thu, 0 = Fri-Sun
- *weekdaySTh:* Whether or not this usage falls on a weekday: 1 = Sun-Thu, 0 = Fri+Sat
- *app_engage_30s:* T/F - was it the first time person used their phone is x minutes/seconds 
- *app_switched_app:* _(previously named `app_switch_app`)_  T/F - app was a different app than the one before
- *app_usage_flags:* Text flags for the following conditions, if present: "6-HR TIME GAP", "12-HR TIME GAP", "1-DAY TIME GAP" _(1 day; previously named "Large time gap")_, "3-HR APP DURATION" _(previously named "Long app duration")_.
