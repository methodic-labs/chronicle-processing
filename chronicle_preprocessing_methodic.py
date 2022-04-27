from argparse import ArgumentParser
from datetime import timedelta
import sqlalchemy as sq
import pandas as pd
import dateutil
import pytz

from pymethodic.pymethodic import utils as ut
import chronicle_process_functions

#------- Can process by DATE
# Also by specific participant and/or by STUDY


def get_parser():
    parser = ArgumentParser(description = 'PyMethodic: Preprocessing and Summarizing Chronicle data via Methodic')
    parser.add_argument('--startdatetime', action = 'store',
        help = 'Start Datetime for interval to be integrated.')
    parser.add_argument('--enddatetime', action='store',
        help = 'End Datetime for interval to be integrated.')
#     parser.add_argument('--organization', action='store',
#         help = 'Organization to choose for apps V3')
    parser.add_argument('--by_study', action='store_true')
    parser.add_argument('--participants', nargs = '+',
        help = "Specific participant(s) candidate_id.", required = False, default = [])
    parser.add_argument('--studies', nargs = '+',
        help = "Specific study/studies.", required = False, default = [])
    parser.add_argument('--daysback',
        help = "For daily processor, the number of days back.", required = False, default = 5)
    parser.add_argument('--dbuser', default="datascience", help = "Username to source database")
    parser.add_argument('--password', help="Password to source database")
    parser.add_argument('--hostname', default="chronicle.cey7u7ve7tps.us-west-2.redshift.amazonaws.com")
    parser.add_argument('--port', default=5439, help="Port of source database")
    return parser



# Processes ALL STUDIES - defaults to the past day. THIS WAS ONLY SET UP FOR CAFE
def chronicle_process_by_date(startdatetime, enddatetime, engine,
                              days_back=5, participants = [], studies = []):

    startdatetime = dateutil.parser.parse(startdatetime).replace(tzinfo=dateutil.tz.tzutc())
    startdatetime = startdatetime.astimezone(pytz.timezone("America/Los_Angeles"))
    enddatetime = dateutil.parser.parse(enddatetime).replace(tzinfo=dateutil.tz.tzutc())
    enddatetime = enddatetime.astimezone(pytz.timezone("America/Los_Angeles"))

    chunk_starttime = startdatetime - timedelta(days=1)
    chunk_endtime = startdatetime

    preprocessed = []

    while chunk_endtime < enddatetime:
        chunk_starttime = chunk_starttime + timedelta(days=1)
        chunk_endtime = min(chunk_starttime + timedelta(days=1), enddatetime)

        start_iso = chunk_starttime.isoformat()
        end_iso = chunk_endtime.isoformat()

        print("#######################################################################")
        print(f"## Processing {start_iso} to {enddatetime.isoformat()} PST ##")
        print("#######################################################################")

        ut.logger("Grabbing data...", level=0)

        # GET RAW DATA - filter by studies and/or participants
        data_written = chronicle_process_functions.search_timebin(
            start_iso,
            end_iso,
            engine,
            participants,
            studies)

        if len(data_written) == 0:
            print("No one has pushed data recently!")
            continue

        # PROCESS
        data = chronicle_process(data_written, startdatetime, enddatetime, days_back)
        preprocessed.append(data)

    if len(preprocessed) > 0:
        preprocessed = pd.concat(preprocessed)
    return preprocessed

# def chronicle_process_by_study(constants, studies = [], participants = []):
#
#     prod_studies_temp = dataAPI.load_entity_set_data(constants['chronicle_study_id'])
#     prod_studies = []
#     if len(studies) > 0:
#         for x in prod_studies_temp:
#             try:
#                 if x['general.fullname'][0] in studies:
#                     prod_studies.append(x)
#             except:
#                 pass
#     else:
#         prod_studies = prod_studies_temp
#
#     datapoints = pd.DataFrame()
#     for x in prod_studies:
#
#         print("#######################################################################")
#         print("## processing study %s ##" % (x['general.fullname'][0]))
#         print("#######################################################################")
#
#         try:
#             sid = x['general.stringid'][0]
#             if constants['org'] == "CAFE":
#                 pid = entitySetsAPI.get_entity_set_id(f'chronicle_participants_{sid}')
#             else:
#                 pid = entitySetsAPI.get_entity_set_id(f'chronicle_{constants["org"]}_participants')
#             data = dataAPI.load_entity_set_data(pid)
#             if len(data) == 0:
#                 ol.logger("No participants")
#                 continue
#             study_participants = pd.DataFrame(
#                 [
#                     {
#                         'entity_set_id': pid,
#                         "person_key_id": x['openlattice.@id'][0],
#                         'person_data_key': x['nc.SubjectIdentification'][0]
#                     } for x in data if 'nc.SubjectIdentification' in x.keys()
#                 ]
#             )
#
#             datapoints = pd.concat([datapoints, study_participants], ignore_index=True)
#
#         except openlattice.ApiException as e:
#             ut.logger("Can't reach study.")
#             continue
#
#         if len(participants) > 0:
#             datapoints = datapoints[datapoints.person_data_key.isin(participants)]
#
#         chronicle_process(constants, datapoints)


def chronicle_process(rawdatatable, startdatetime = None, enddatetime = None, days_back = 5):
    ''' Used in the processing functions above at their end;
    this does the actual preprocessing.
    '''

    if rawdatatable.shape[0] == 0:
        ut.logger("No found app usages :-\ ...")
        return None

    # loop over all participant IDs (could be parellized if
    # at some point this should be run on a individual server).
    # For each person:
    # - get preprocessed data for subject
    # - write somewhere

    preprocessed_data = []
    ids = rawdatatable['participant_id'].unique()

    if len(ids) > 0:
        for person in ids:
            df = rawdatatable.loc[rawdatatable['participant_id'] == person]
            ut.logger(f"Analyzing data for {person}:  {df.shape[0]} datapoints.", level=0)


            # PREPROCESSING - 1 person at a time. RETURNS DF
            person_df_preprocessed = chronicle_process_functions.get_person_preprocessed_data(
                person,
                rawdatatable,
                # constants,
                startdatetime,
                enddatetime,
                days_back
            )

            preprocessed_data.append(person_df_preprocessed)

            if isinstance(person_df_preprocessed, None.__class__):
                ut.logger(f"After preprocessing, no data remaining for person {person}/{rawdatatable.shape[0]}.", level=0)
                continue

    #--------- WRITE SOMEWHERE
    #         ut.logger("Integrating data for person %i/%i"%(idx+1,datapointsframe.shape[0]), level = 0)

        if len(preprocessed_data) > 0:
            preprocessed_data = pd.concat(preprocessed_data)
    else:
        ut.logger(f"No participants found in this date range")

    print("#---------------------------------------------#")
    ut.logger("Pipeline finished successfully !", level=0)
    return preprocessed_data


def main():
    opts = get_parser().parse_args()

    engine = sq.create_engine(f'postgresql://{opts.dbuser}:{opts.password}@{opts.hostname}:{opts.port}/redshift')

    chronicle_process_by_date(opts.startdatetime, opts.enddatetime, engine, opts.daysback, opts.participants, opts.by_study)

if __name__ == '__main__':
    main()
