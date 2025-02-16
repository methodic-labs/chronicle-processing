from argparse import ArgumentParser
from uuid import uuid4
import pendulum
import sys

sys.path.insert(1,'/chronicle-processing')
from pymethodic2 import utils as ut
from pymethodic2 import preprocessing #chronicle_process_functions

import preprocessing_functions_methodic as pf

#------------Not using Prefect, but argparse to pass in variables.
#------- Also - Can process by DATE
# Also by specific participant and/or by STUDY


def get_parser():
    parser = ArgumentParser(description = 'PyMethodic: Preprocessing and Summarizing Chronicle data via Methodic')
    parser.add_argument('--startdatetime', required=False, action = 'store',
        help = 'Start Datetime for interval to be integrated.')
    parser.add_argument('--enddatetime', required=False, action='store',
        help = 'End Datetime for interval to be integrated.')
    parser.add_argument('--tz_input', default="UTC")
#     parser.add_argument('--organization', action='store',
#         help = 'Organization to choose for apps V3')
    parser.add_argument('--by_study', required=False, action='store_true')
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
    parser.add_argument('--dbname', default="redshift", help="Prod database name")
    parser.add_argument('--filepath', default = "", required=False, help = "Optional filepath for saving results as csv.")
    parser.add_argument('--filename', default = "", required=False, help = "Optional filename for saving results as csv.")
    parser.add_argument('--export_format', default="", required=False, help="To optionally saving results as csv, input 'csv' in this argumment.")

    # args, unknown = parser.parse_known_args()
    args = parser.parse_args()
    return args


def get_date_range_for_processing(user_input_start, user_input_end):
    if user_input_start is not None:
        start = user_input_start
        end = user_input_end
    else:
        daily_range = pf.default_time_params() #needs to be in this format to work outside of Flow
        start = str(daily_range[0])
        end = str(daily_range[1])

    return (start, end)



def main():
    # SET UP PARAMETERS
    args = get_parser().parse_args()
    run_id = pendulum.now().strftime('%Y-%m%d-%H%M%S-') + str(uuid4())

    # Date range for preprocessing is either a user-specified range, or 3-days ago to 2-days ago.
    startdatetime = str(get_date_range_for_processing(args.startdatetime, args.enddatetime)[0])
    enddatetime = str(get_date_range_for_processing(args.startdatetime, args.enddatetime)[1])

    engine = pf.connect(args.prod_user, args.prod_password, args.prod_host, args.prod_port, args.prod_dbname, type="sqlalchemy")
    print("Connection to raw data successful!")

    # Extract:
    raw = pf.search_timebin(startdatetime, enddatetime, args.tz_input, engine, args.participants, args.studies)

    # Transform:
    processed = pf.chronicle_process(raw, run_id)
    # chronicle_process_by_date(opts.startdatetime, opts.enddatetime, engine, opts.daysback, opts.participants, opts.by_study)

    # Write out:
    conn = pf.connect(args.prod_user, args.prod_password, args.prod_host, args.prod_port, args.prod_dbname, type="psycopg2")
    # local_engine = connect(dbuser, password, hostname, port, dbname, type="psycopg2")
    print("Connection to export table successful!")

    # For times when there's no data to preprocess, and "processed' returns nothing and does not exist
    try:
        #     print(f"Trying how_export with {processed.shape, studies, filepath, filename, conn, format}")
        pf.how_export(processed, args.studies, args.filepath, args.filename, conn, format=args.export_format)
        ut.write_log_summary(run_id, args.studies, conn,
                             f"Preprocessing pipeline finished for {args.studies} between {startdatetime} and {enddatetime}!")
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()


###---------------- OLDER FUNCTIOS TO RETURN TO LATER
# Processes ALL STUDIES - defaults to the past day. THIS WAS ONLY SET UP FOR CAFE
# def chronicle_process_by_date(startdatetime, enddatetime, engine,
#                               days_back=5, participants = [], studies = []):
#
#     startdatetime = dateutil.parser.parse(startdatetime).replace(tzinfo=dateutil.tz.tzutc())
#     startdatetime = startdatetime.astimezone(pytz.timezone("America/Los_Angeles"))
#     enddatetime = dateutil.parser.parse(enddatetime).replace(tzinfo=dateutil.tz.tzutc())
#     enddatetime = enddatetime.astimezone(pytz.timezone("America/Los_Angeles"))
#
#     chunk_starttime = startdatetime - timedelta(days=1)
#     chunk_endtime = startdatetime
#
#     preprocessed = []
#
#     while chunk_endtime < enddatetime:
#         chunk_starttime = chunk_starttime + timedelta(days=1)
#         chunk_endtime = min(chunk_starttime + timedelta(days=1), enddatetime)
#
#         start_iso = chunk_starttime.isoformat()
#         end_iso = chunk_endtime.isoformat()
#
#         print("#######################################################################")
#         print(f"## Processing {start_iso} to {enddatetime.isoformat()} PST ##")
#         print("#######################################################################")
#
#         ut.print_helper("Grabbing data...", level=0)
#
#         # GET RAW DATA - filter by studies and/or participants
#         data_written = preprocessing.search_timebin(  # was chronicle_process_functions
#             start_iso,
#             end_iso,
#             engine,
#             participants,
#             studies)
#
#         if len(data_written) == 0:
#             print("No one has pushed data recently!")
#             continue
#
#         # PROCESS
#         data = chronicle_process(data_written, startdatetime, enddatetime, days_back)
#         preprocessed.append(data)
#
#     if len(preprocessed) > 0:
#         preprocessed = pd.concat(preprocessed)
#     return preprocessed

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

