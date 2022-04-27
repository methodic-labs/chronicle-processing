# chronicle-processing
This repository contains code to process data from the Chronicle app, via Methodic.  
  
Installation of the `pymethodic` module (located here) is required.
1. Clone this repository.
2. In Terminal (Mac) or the command prompt (Windows) Navigate to the `pymethodic` folder
3. Type in `pip3 install .`


The following fields are the outputs of Chronicle preprocessing:
- *participant_id:* Taken from the raw device data.
- *app_record_type:* The type of app interaction (i.e., "App Usage", "Screen Inactive").
- *app_title:* The title of the app
- *app_fullname:* The name of the app as it appears in the raw data.
- *app_datetime_start:* The start timestamp of the app usage - in the timezone specified in the `app_timezone` column.
- *app_datetime_end:* The end timestamp of the app usage - in the timezone specified in the `app_timezone` column.
- *app_timezone:* The timezone taken from the raw device data.
- *app_duration_seconds:* Duration (in seconds)
- *day:* The day of the week.  The week starts on Sunday, i.e. 1 = Sunday, 2 = Monday, etc.
- *weekdayMF:* Whether or not this usage falls on a weekday: 1 = Mon-Fri, 0 = Sat+Sun
- *weekdayMTh:* Whether or not this usage falls on a weekday: 1 = Mon-Thu, 0 = Fri-Sun
- *weekdaySTh:* Whether or not this usage falls on a weekday: 1 = Sun-Thu, 0 = Fri+Sat
- *app_engage_30s:* T/F - was it the first time person used their phone is x minutes/seconds 
- *app_switched_app:* T/F - app was a different app than the one before
- *app_usage_flags:* Text flags for the following conditions, if present: "Large time gap" (1 day), "Long app duration" (3 hrs).
