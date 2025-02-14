import sys
from datetime import datetime, timedelta
import boto3
from prettytable import PrettyTable


# aws boto3 client
class Boto3ClientSingleton:
    _instances = {}

    def __new__(cls, service_name, *args, **kwargs):
        if service_name not in cls._instances:
            cls._instances[service_name] = boto3.client(service_name, *args, **kwargs)
        return cls._instances[service_name]


aws_sf_client = Boto3ClientSingleton("stepfunctions")


# state machine execution result class
class StateMachineExecutionResult:
    def __init__(self, state_machine_name, start_time, status):
        """Constructor to initialize name and age"""
        self.state_machine_name = state_machine_name
        self.start_time = start_time
        self.status = status

    def __getattr__(self, attr):
        """Handles undefined attribute access."""
        return f"'{attr}' attribute not found"

    def __str__(self):
        return f'{self.state_machine_name}\t{self.start_time}\t{self.status}'


def round_to_half_hour(dt):
    """Rounds a datetime object to the nearest half-hour"""
    minute = dt.minute
    if minute < 30:
        rounded_minute = 0
    else:
        rounded_minute = 30
    return dt.replace(minute=rounded_minute, second=0, microsecond=0)


def get_broker_state_machine_arn(broker):
    output_dict = {}
    stateMachines = []
    response = aws_sf_client.list_state_machines(maxResults=1000)
    stateMachines.extend(response['stateMachines'])

    if 'nextToken' in response:
        next_token = response['nextToken']
    else:
        next_token = None

    while next_token is not None:
        response = aws_sf_client.list_state_machines(maxResults=1000, nextToken=next_token)
        stateMachines.extend(response['stateMachines'])
        if 'nextToken' in response:
            next_token = response['nextToken']
        else:
            next_token = None

    # Print the state machines
    for sm in stateMachines:
        step_function_name = sm['name']
        step_function_arn = sm['stateMachineArn']
        if step_function_name.lower().startswith(f'{broker}-production'):
            output_dict[step_function_name] = step_function_arn

    return output_dict


def get_state_machine_last_run(state_machine_arn):
    # response = client.list_executions(stateMachineArn=state_machine_arn, maxResults=1) # not working
    # get last 10 runs
    response = aws_sf_client.list_executions(stateMachineArn=state_machine_arn, maxResults=10)
    state_machine_name = state_machine_arn.split(':')[-1]

    if response['executions'] != []: # why not works?
        # sort based on startDate
        execution_result = response['executions']
        execution_result_sorted = sorted(execution_result, key=lambda x: x["startDate"], reverse=True)

        execution_result = execution_result_sorted[0]
        start_date = execution_result['startDate'].replace(tzinfo=None).replace(microsecond=0)
        status = execution_result['status']
        state_machine_execution_result = StateMachineExecutionResult(state_machine_name, start_date, status)
    else:
        state_machine_execution_result = StateMachineExecutionResult(state_machine_name, '1970-01-01 00:00:00', 'NOT_RUN')

    return state_machine_execution_result


def get_broker_all_state_machines_last_run(broker):
    state_machines_status = []

    state_machine_arns = get_broker_state_machine_arn(broker)
    for state_machine_arn in state_machine_arns.values():
        state_machine_execution_result = get_state_machine_last_run(state_machine_arn)
        state_machines_status.append(state_machine_execution_result)

    return state_machines_status


def get_broker_all_state_machines_last_run_formatted(broker):
    output_table = PrettyTable(['State_Machine', 'Check_Time', 'Start_Time', 'Status', 'Ready_To_Release'])
    state_machines_status = get_broker_all_state_machines_last_run(broker)

    for result in state_machines_status:
        state_machine = result.state_machine_name
        start_time = result.start_time
        status = result.status

        # if the state machine finished and start time later than check time, then this state machine is finished
        # if the state machine finished but start time before check time and within minutes, then it is not start yet
        # if the start machine finished but start time before check time more than 1 hour, then it is not a half an hour job
        if (status == 'SUCCEEDED' and start_time > check_time) or (status == 'NOT_RUN'):
            ready_to_release = 'YES'
        elif status == 'SUCCEEDED' and start_time < check_time and (check_time - start_time).total_seconds() / 3600 < 0.5:
            ready_to_release = 'NOT_START'
        elif status == 'SUCCEEDED' and start_time < check_time and (check_time - start_time).total_seconds() / 3600 > 1:
            ready_to_release = 'YES'
        elif status == 'RUNNING':
            ready_to_release = 'NO'
        else:
            ready_to_release = 'MANUAL_CHECK'

        output_table.add_row([state_machine, check_time, start_time, status, ready_to_release])

    print(output_table.get_string(sortby='Ready_To_Release'))
    return output_table




if __name__ == '__main__':

    # for broker_state_machine, broker_state_machine_arn in broker_state_machines.items():
    #     print(f'{broker_state_machine}: {broker_state_machine_arn}')

    # stateMachineExecutionResult = get_state_machine_last_run('arn:aws:states:ap-southeast-2:859004686855:stateMachine:tmgm-production-2nd-parallel-state')
    # print(stateMachineExecutionResult)

    broker = 'anzo'
    check_time = round_to_half_hour(datetime.now())

    # get all state machine status
    state_machines_status = get_broker_all_state_machines_last_run_formatted(broker)
