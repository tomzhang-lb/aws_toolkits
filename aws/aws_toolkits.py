from datetime import datetime, timedelta
import boto3
from aiohttp import ClientError
from prettytable import PrettyTable


# aws boto3 client
class Boto3ClientSingleton:
    _instances = {}

    def __new__(cls, service_name, *args, **kwargs):
        if service_name not in cls._instances:
            cls._instances[service_name] = boto3.client(service_name, *args, **kwargs)
        return cls._instances[service_name]


# state machine execution result class
class StateMachineExecutionResult:
    def __init__(self, state_machine_name, start_time, status):
        """Constructor initialization"""
        self.state_machine_name = state_machine_name
        self.start_time = start_time
        self.status = status

    def __getattr__(self, attr):
        """Handles undefined attribute access."""
        return f"'{attr}' attribute not found"

    def __str__(self):
        return f'{self.state_machine_name}\t{self.start_time}\t{self.status}'


class EventBridgeRuleState:
    def __init__(self, rule_name, state, rule_event_bus_name):
        """Constructor initialization"""
        self.rule_name = rule_name
        self.state = state
        self.rule_event_bus_name = rule_event_bus_name

    def __getattr__(self, attr):
        """Handles undefined attribute access."""
        return f"'{attr}' attribute not found"

    def __repr__(self):
        return f'{self.rule_name}\t{self.state}\t{self.rule_event_bus_name}'


def round_to_half_hour(dt):
    """Rounds a datetime object to the nearest half-hour"""
    minute = dt.minute
    if minute < 30:
        rounded_minute = 0
    else:
        rounded_minute = 30
    return dt.replace(minute=rounded_minute, second=0, microsecond=0)


def get_broker_state_machine_arn(aws_sf_client, broker, branch='production'):
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
        if step_function_name.lower().startswith(f'{broker}-{branch}'):
            output_dict[step_function_name] = step_function_arn

    return output_dict


def get_state_machine_last_run(aws_sf_client, state_machine_arn):
    # response = client.list_executions(stateMachineArn=state_machine_arn, maxResults=1) # not working
    # get last 10 runs
    response = aws_sf_client.list_executions(stateMachineArn=state_machine_arn, maxResults=10)
    state_machine_name = state_machine_arn.split(':')[-1]

    if response['executions']:
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


def get_broker_all_state_machines_last_run(aws_sf_client, broker, branch='production'):
    state_machines_status = []

    state_machine_arns = get_broker_state_machine_arn(aws_sf_client, broker, branch=branch)
    if state_machine_arns:
        for state_machine_arn in state_machine_arns.values():
            state_machine_execution_result = get_state_machine_last_run(aws_sf_client, state_machine_arn)
            state_machines_status.append(state_machine_execution_result)
    else:
        print(f'No such state machine found: {broker}-{branch}* hence no execution results')

    return state_machines_status


def get_broker_all_state_machines_last_run_formatted(aws_sf_client, broker, check_time=datetime.now(), branch='production'):
    output_table = PrettyTable(['State_Machine', 'Check_Time', 'Start_Time', 'Status', 'Ready_To_Release'])
    check_time = round_to_half_hour(check_time)
    state_machines_status = get_broker_all_state_machines_last_run(aws_sf_client, broker, branch=branch)

    if state_machines_status:
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
    else:
        pass
    return output_table


def get_broker_rule_status(aws_ev_client, broker, branch='production'):
    response = aws_ev_client.list_rules(NamePrefix=f'{broker}-{branch}', Limit=100)
    event_bridge_rule_states = []
    rule_table = PrettyTable(['Rule_Name', 'State', 'Event_Bus'])

    if not response['Rules']:
        print(f'{broker}-{branch}* rule not found')
    else:
        for rule in response["Rules"]:
            rule_name = rule["Name"]
            rule_state = rule["State"]
            rule_event_bus_name = rule["EventBusName"]

            event_bridge_rule_state = EventBridgeRuleState(rule_name, rule_state, rule_event_bus_name)
            rule_table.add_row([rule_name, rule_state, rule_event_bus_name])
            event_bridge_rule_states.append(event_bridge_rule_state)

    print(rule_table.get_string(sortby='State', reversesort=True))
    return event_bridge_rule_states


def disable_event_bridge_rules(aws_sf_client, rule_name, event_bus):
    try:
        aws_sf_client.disable_rule(
            Name=rule_name,
            EventBusName=event_bus
        )
    except Exception as e:
        print(e)


def enable_event_bridge_rules(aws_sf_client, rule_name, event_bus):
    try:
        aws_sf_client.enable_rule(
            Name=rule_name,
            EventBusName=event_bus
        )
    except Exception as e:
        print(e)


def disable_broker_rules(aws_sf_client, broker, branch='production'):
    print(f'Rule Status Before Disable:')
    event_bridge_rule_states = get_broker_rule_status(aws_sf_client, broker, branch)

    for event_bridge_rule_state in event_bridge_rule_states:
        if event_bridge_rule_state.state == 'ENABLED':
            disable_event_bridge_rules(aws_sf_client, event_bridge_rule_state.rule_name, event_bridge_rule_state.rule_event_bus_name)

    print(f'Rule Status After Disable:')
    get_broker_rule_status(aws_sf_client, broker, branch)


def enable_broker_rules(aws_sf_client, broker, branch='production'):
    print(f'Rule Status Before Enable:')
    event_bridge_rule_states = get_broker_rule_status(aws_sf_client, broker, branch)

    for event_bridge_rule_state in event_bridge_rule_states:
        if event_bridge_rule_state.rule_name == 'DISABLED':
            enable_event_bridge_rules(aws_sf_client, event_bridge_rule_state.rule_name, event_bridge_rule_state.rule_event_bus_name)

    print(f'Rule Status After Enable:')
    get_broker_rule_status(aws_sf_client, broker, branch)


if __name__ == '__main__':

    # for broker_state_machine, broker_state_machine_arn in broker_state_machines.items():
    #     print(f'{broker_state_machine}: {broker_state_machine_arn}')

    # stateMachineExecutionResult = get_state_machine_last_run('arn:aws:states:ap-southeast-2:859004686855:stateMachine:tmgm-production-2nd-parallel-state')
    # print(stateMachineExecutionResult)

    broker = 'dlsm'
    # get all state machine status
    # aws_sf_client = Boto3ClientSingleton('stepfunctions')
    # state_machines_status = get_broker_all_state_machines_last_run_formatted(aws_sf_client, broker)
    aws_ev_client = Boto3ClientSingleton('events')
    # enable_broker_rules(aws_ev_client, broker, branch='dev-iad-2733')
    disable_broker_rules(aws_ev_client, broker, branch='dev-iad-2733')
