from prefect import task, Flow, Parameter, context, unmapped
from prefect.tasks.prefect import create_flow_run
from prefect.tasks.secrets import PrefectSecret
from prefect.run_configs import DockerRun
from prefect.storage import GitHub
from prefect.executors import LocalDaskExecutor
import pandas as pd
import pendulum

# import sys
# sys.path.insert(1,'/chronicle-processing')
# import chronicle_process_functions

##---------------- Running > 1 day of preprocessing (i.e. catchup jobs)-----------#
# Writing into Redshift has a byte limit that is reached after ~2-3 days of heavy usage by 1 study. Iterating over days
@task
def bulk_time_params(start, end, delta, tz_input="UTC"):
    '''FOR CATCHUP JOBS > 2 DAYS
       Returns a tuple of start and end datetimes, each in a list, for child flows.
       start: datetime string in format YYYY-MM-DD HH:mm:ss
       delta: the number of days each integration will cover.'''
    timezone = pendulum.timezone(tz_input)
    start_range = pendulum.parse(start, tz=timezone)
    end_range = pendulum.parse(end, tz=timezone)

    # Chunk out flows by X days
    starts = pd.date_range(start_range, end_range, freq=f'{delta}d').strftime('%Y-%m-%d %H:%M').to_list()
    if end_range.strftime('%Y-%m-%d %H:%M') in starts: starts.remove(end_range.strftime('%Y-%m-%d %H:%M'))
    ends = starts[1:]  # take out first "start" date, make the others the end dates for seamless integration
    ends.append(end_range.strftime('%Y-%m-%d %H:%M'))  # add the end date

    print(f"There are {len(starts)} start and {len(ends)} end datetimes in this batch integration.")
    return (starts, ends)




with Flow("preprocessing_bulk", storage=GitHub(repo="methodic-labs/chronicle-processing", path="preprocessing_bulk_prefect.py"),
          run_config=DockerRun(image="methodiclabs/chronicle-processing")) as flow:
    studies = Parameter("studies", default=[])  # "Specific study/studies.", required=False
    tz_input = Parameter("timezone", default="UTC")
    password = PrefectSecret("dbpassword")
    start_range = Parameter("start_range")  # 'Start datetime for interval to be integrated.'
    end_range = Parameter("end_range")  # 'End datetime for interval to be integrated.
    day_delta = Parameter("day_delta", default=3)  # The # of days to chunk out per interval, for each flow run

    print(tz_input)

    time_parameters = bulk_time_params(start_range, end_range, day_delta, tz_input)
    print(time_parameters)
    starts = str(time_parameters[0])
    ends = str(time_parameters[1])

    all_bulk_params = []
    print(range(len(starts)))
    for i in range(len(starts)):
        single_flow_params = {"startdatetime": starts[i],
                        "enddatetime": ends[i],
                        "participants": [],
                        "studies": studies,
                        "timezone": tz_input,
                        "export_format": "",
                        "filepath": "",
                        "filename": "",
                        "dbuser": "datascience",
                        "hostname": "chronicle.cey7u7ve7tps.us-west-2.redshift.amazonaws.com",
                        "port": 5439,
                        "password": password}
        all_bulk_params.append(single_flow_params)

    mapped_flows = create_flow_run.map(
        parameters=all_bulk_params,
        flow_name=unmapped("preprocessing_daily"),
        project_name=unmapped("Preprocessing"),
    )


def main():
    #Register the flow when this is run as a script.
    flow.register(project_name="Preprocessing")

if __name__ == '__main__':
    main()
