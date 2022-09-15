from prefect import task, Flow, Parameter, context, unmapped
from prefect.tasks.prefect import create_flow_run
from prefect.tasks.secrets import PrefectSecret
from prefect.run_configs import DockerRun
from prefect.storage import GitHub
from prefect.executors import LocalDaskExecutor
import pandas as pd
import pendulum

##---------------- Running > 1 day of preprocessing (i.e. catchup jobs)-----------#
# Writing into Redshift has a byte limit that is reached after ~2-3 days of heavy usage by 1 study. Iterating over days
import prefect

@task
def convert_to_utc(input_list):
    if all(isinstance(s, str) for s in input_list):
        dt = [pendulum.parse(x) for x in input_list]
        dt2 = [x.in_tz('UTC') for x in dt]
        dt3 = [x.strftime('%Y-%m-%d %H:%M') for x in dt2]
    else:
        print("Expected dates in a list of strings!")

    return dt3


@task(log_stdout=True)
def bulk_time_params(start, end, delta, tz_input="UTC") -> tuple:   #output_type=1,
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

    return(starts, ends)


# Only necessary because one can't break down parameter outputs in a Flow.
@task(log_stdout=True)
def all_bulk_params(times_tuple, studies, participants, password, tz_input):
    '''Written specifically to take in the output of bulk_time_params
    function
    times_list: a tuple of 2 lists
    password: password to the db to be written into
    output: a list of dictionaries, each with the parameters of a Prefect flow run'''
    starts = times_tuple[0]
    ends = times_tuple[1]

    if tz_input != "UTC":
        starts = convert_to_utc.run(starts)
        ends = convert_to_utc.run(ends)

    all_params = []
    print(range(len(starts)))
    for i in range(len(starts)):
        single_flow_params = {"startdatetime": starts[i],
                              "enddatetime": ends[i],
                              "participants": participants,
                              "studies": studies,
                              "timezone": "UTC",
                              "export_format": "",
                              "filepath": "",
                              "filename": "",
                              "dbuser": "datascience",
                              "hostname": "chronicle.cey7u7ve7tps.us-west-2.redshift.amazonaws.com",
                              "port": 5439,
                              "password": password}
        all_params.append(single_flow_params)
    # print(all_params[0]) #check
    return all_params


with Flow("preprocessing_bulk", storage=GitHub(repo="methodic-labs/chronicle-processing", path="preprocessing_bulk_prefect.py"),
          run_config=DockerRun(image="methodiclabs/chronicle-processing")) as flow:
    start_range = Parameter("start_range")  # 'Start datetime for interval to be integrated.'
    end_range = Parameter("end_range")  # 'End datetime for interval to be integrated.
    day_delta = Parameter("day_delta", default=3)  # The # of days to chunk out per interval, for each flow run
    studies = Parameter("studies", default=[])  # "Specific study/studies.", required=False
    participants = Parameter("participants", default=[])  # "Specific participant(s) candidate_id.", required=False
    tz_input = Parameter("timezone", default="UTC")
    password = PrefectSecret("dbpassword")

    time_params = bulk_time_params(start_range, end_range, day_delta, tz_input)
    all_runs_params = all_bulk_params(time_params, studies, participants, password, tz_input)

    mapped_flows = create_flow_run.map(
        parameters=all_runs_params,
        flow_name=unmapped("preprocessing_daily"),
        project_name=unmapped("Preprocessing"),
    )


def main():
    #Register the flow when this is run as a script.
    flow.register(project_name="Preprocessing")

if __name__ == '__main__':
    main()
