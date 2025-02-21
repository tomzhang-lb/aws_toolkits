from datetime import datetime, timezone
import boto3
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
    def __init__(self, rule_name, state, rule_event_bus_name, managed_by, target_ids):
        """Constructor initialization"""
        self.rule_name = rule_name
        self.state = state
        self.rule_event_bus_name = rule_event_bus_name
        self.managed_by = managed_by
        self.target_ids = target_ids

    def __getattr__(self, attr):
        """Handles undefined attribute access."""
        return f"'{attr}' attribute not found"

    def __repr__(self):
        return f'{self.rule_name}\t{self.state}\t{self.rule_event_bus_name}\t{self.managed_by}\t{self.target_ids}'


class LambdaFunction:
    def __init__(self, function_name, function_arn, function_version, function_last_executed, function_last_modified):
        self.function_name = function_name
        self.function_arn = function_arn
        self.function_version = function_version
        self.function_last_executed = function_last_executed
        self.function_last_modified = function_last_modified

    def __getattr__(self, attr):
        """Handles undefined attribute access."""
        return f"'{attr}' attribute not found"

    def __repr__(self):
        return f'{self.function_name}\t{self.function_arn}\t{self.function_version}\t{self.function_last_executed}\t{self.function_last_modified}'


class AwsToolkits:
    def __init__(self, broker, branch):
        self.broker = broker.lower()
        self.branch = branch.lower()
        self.__aws_sf_client = Boto3ClientSingleton('stepfunctions')
        self.__aws_events_client = Boto3ClientSingleton('events')
        self.__aws_lambda_client = Boto3ClientSingleton('lambda')
        self.__aws_log_client = Boto3ClientSingleton('logs')

    def __round_to_half_hour(self, dt: datetime):
        """Rounds a datetime object to the nearest half-hour"""
        minute = dt.minute
        if minute < 30:
            rounded_minute = 0
        else:
            rounded_minute = 30
        return dt.replace(minute=rounded_minute, second=0, microsecond=0)

    # __aws_sf_client = Boto3ClientSingleton('stepfunctions')
    # __aws_ev_client = Boto3ClientSingleton('events')

    def get_broker_state_machine_arn(self):
        output_dict = {}
        state_machines = []
        response = self.__aws_sf_client.list_state_machines(maxResults=1000)
        state_machines.extend(response['stateMachines'])

        if 'nextToken' in response:
            next_token = response['nextToken']
        else:
            next_token = None

        while next_token is not None:
            response = self.__aws_sf_client.list_state_machines(maxResults=1000, nextToken=next_token)
            state_machines.extend(response['stateMachines'])
            if 'nextToken' in response:
                next_token = response['nextToken']
            else:
                next_token = None

        # Print the state machines
        for sm in state_machines:
            step_function_name = sm['name']
            step_function_arn = sm['stateMachineArn']
            if step_function_name.lower().startswith(f'{self.broker}-{self.branch}'):
                output_dict[step_function_name] = step_function_arn

        return output_dict

    def get_state_machine_last_run(self, state_machine_arn):
        # response = client.list_executions(stateMachineArn=state_machine_arn, maxResults=1) # not working
        # get last 10 runs
        response = self.__aws_sf_client.list_executions(stateMachineArn=state_machine_arn, maxResults=10)
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
            state_machine_execution_result = StateMachineExecutionResult(state_machine_name, '1970-01-01 00:00:00',
                                                                         'NOT_RUN')

        return state_machine_execution_result

    def __get_broker_all_state_machines_last_run(self):
        state_machines_status = []

        state_machine_arns = self.get_broker_state_machine_arn()
        if state_machine_arns:
            for state_machine_arn in state_machine_arns.values():
                state_machine_execution_result = self.get_state_machine_last_run(state_machine_arn)
                state_machines_status.append(state_machine_execution_result)
        else:
            print(f'No such state machine found: {self.broker}-{self.branch}* hence no execution results')

        return state_machines_status

    def get_broker_all_state_machines_last_run_formatted(self, check_time=datetime.now()):
        output_table = PrettyTable(['State_Machine', 'Check_Time', 'Start_Time', 'Status', 'Ready_To_Release'])
        check_time = self.__round_to_half_hour(check_time)
        state_machines_status = self.__get_broker_all_state_machines_last_run()

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
                elif status == 'SUCCEEDED' and start_time < check_time and (
                        check_time - start_time).total_seconds() / 3600 < 0.5:
                    ready_to_release = 'NOT_START'
                elif status == 'SUCCEEDED' and start_time < check_time and (
                        check_time - start_time).total_seconds() / 3600 > 1:
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

    def list_rule_target_ids(self, rule):
        target_ids = []
        response = self.__aws_events_client.list_targets_by_rule(
            Rule=rule["Name"],
            EventBusName=rule["EventBusName"],
            Limit=100
        )

        if not response['Targets']:
            print(f'{rule.rule_name}* rule not found')
        else:
            for target in response['Targets']:
                target_id = target['Id']
                target_ids.append(target_id)

        return target_ids

    def get_broker_rule_status(self):
        response = self.__aws_events_client.list_rules(NamePrefix=f'{self.broker}-{self.branch}', Limit=100)
        event_bridge_rule_states = []
        rule_table = PrettyTable(['Rule_Name', 'State', 'Event_Bus', 'Managed_By', 'Target_Ids'])

        if not response['Rules']:
            print(f'{self.broker}-{self.branch}* rule not found')
        else:
            for rule in response["Rules"]:
                rule_name = rule["Name"]
                rule_state = rule["State"]
                rule_event_bus_name = rule["EventBusName"]
                rule_managed_by = 'default'
                target_ids = self.list_rule_target_ids(rule)

                event_bridge_rule_state = EventBridgeRuleState(rule_name, rule_state, rule_event_bus_name,
                                                               rule_managed_by, target_ids)
                rule_table.add_row([rule_name, rule_state, rule_event_bus_name, rule_managed_by, target_ids])
                event_bridge_rule_states.append(event_bridge_rule_state)

        print(rule_table.get_string(sortby='State', reversesort=True))
        return event_bridge_rule_states

    def disable_event_bridge_rules(self, rule_name, event_bus):
        try:
            self.__aws_events_client.disable_rule(
                Name=rule_name,
                EventBusName=event_bus
            )
        except Exception as e:
            print(e)

    def enable_event_bridge_rules(self, rule_name, event_bus):
        try:
            self.__aws_events_client.enable_rule(
                Name=rule_name,
                EventBusName=event_bus
            )
        except Exception as e:
            print(e)

    def disable_broker_rules(self):
        print(f'Rule Status Before Disable:')
        event_bridge_rule_states = self.get_broker_rule_status()

        for event_bridge_rule_state in event_bridge_rule_states:
            if event_bridge_rule_state.state == 'ENABLED':
                self.disable_event_bridge_rules(event_bridge_rule_state.rule_name,
                                                event_bridge_rule_state.rule_event_bus_name)

        print(f'Rule Status After Disable:')
        self.get_broker_rule_status()

    def enable_broker_rules(self):
        print(f'Rule Status Before Enable:')
        event_bridge_rule_states = self.get_broker_rule_status()

        for event_bridge_rule_state in event_bridge_rule_states:
            if event_bridge_rule_state.rule_name == 'DISABLED':
                self.enable_event_bridge_rules(event_bridge_rule_state.rule_name,
                                               event_bridge_rule_state.rule_event_bus_name)

        print(f'Rule Status After Enable:')
        self.get_broker_rule_status()

    def list_broker_rules(self):
        response = self.__aws_events_client.list_rules(NamePrefix=f'{self.broker}-{self.branch}', Limit=100)
        event_bridge_rule_states = []
        rule_table = PrettyTable(['Rule_Name', 'State', 'Event_Bus', 'Managed_By', 'Target_Ids'])

        if not response['Rules']:
            print(f'{self.branch}* rule not found')
        else:
            for rule in response["Rules"]:
                rule_name = rule["Name"]
                rule_state = rule["State"]
                rule_event_bus_name = rule["EventBusName"]
                rule_managed_by = 'default'
                target_ids = self.list_rule_target_ids(rule)

                event_bridge_rule_state = EventBridgeRuleState(rule_name, rule_state, rule_event_bus_name,
                                                               rule_managed_by, target_ids)
                rule_table.add_row([rule_name, rule_state, rule_event_bus_name, rule_managed_by, target_ids])
                event_bridge_rule_states.append(event_bridge_rule_state)

        print(rule_table.get_string(sortby='State', reversesort=True))
        return event_bridge_rule_states

    def __delete_rules(self, rules):
        if not rules:
            print(f'No rule to delete')
        else:
            confirm_delete = input('CONFIRM TO DELETE?\n')
            if confirm_delete.lower() == 'yes' or confirm_delete.lower() == 'y':
                for rule in rules:
                    if rule.state == 'ENABLED':
                        print(f'Rule_Name: {rule.rule_name} is still {rule.state}, can NOT delete!')
                    else:
                        try:
                            # remove target first
                            self.__aws_events_client.remove_targets(
                                Rule=rule.rule_name,
                                EventBusName=rule.rule_event_bus_name,
                                Ids=rule.target_ids,
                                Force=True
                            )

                            # delete rule after removing targets
                            self.__aws_events_client.delete_rule(
                                Name=rule.rule_name,
                                EventBusName=rule.rule_event_bus_name,
                                Force=True
                            )

                            print(f'Deleted rule: {rule.rule_name}')
                        except Exception as e:
                            print(e)
            else:
                print(f'Confirm to NOT delete.')

    def delete_broker_rules(self):
        event_bridge_rule_states = self.list_broker_rules()
        self.__delete_rules(event_bridge_rule_states)

    def list_broker_functions(self):
        functions = []
        function_table = PrettyTable(['Function_Name', 'Function_Arn', 'Version', 'Last_Executed', 'Last_Modified'])
        paginator = self.__aws_lambda_client.get_paginator('list_functions')
        for page in paginator.paginate():
            for function in page['Functions']:
                # print(function)
                function_name = function['FunctionName']
                function_arn = function['FunctionArn']
                function_version = function['Version']
                function_last_modified = function['LastModified']
                function_last_executed = self.lambda_last_execution_time(function_name)

                if function_name.lower().startswith(f'{self.broker}-{self.branch}'):
                    lambda_function = LambdaFunction(function_name, function_arn, function_version,
                                                     function_last_executed, function_last_modified)
                    function_table.add_row(
                        [function_name, function_arn, function_version, function_last_executed, function_last_modified])
                    functions.append(lambda_function)
                else:
                    pass
            # break

        print(function_table.get_string(sortby='Last_Executed', reversesort=False))
        return functions

    def lambda_last_execution_time(self, function_name):
        log_group_name = f'/aws/lambda/{function_name}'
        last_execution_time = ''
        try:
            # Get the latest log streams (ordered by LastEventTime)
            response = self.__aws_log_client.describe_log_streams(
                logGroupName=log_group_name,
                orderBy='LastEventTime',
                descending=True,
                limit=1  # Get the most recent log stream
            )

            if response['logStreams']:
                last_event_timestamp = response['logStreams'][0]['firstEventTimestamp']
                last_execution_time = datetime.fromtimestamp(last_event_timestamp / 1000)
                # print(f'Last execution time: {last_execution_time}')
            else:
                print('No executions found for this Lambda.')

        except self.__aws_log_client.exceptions.ResourceNotFoundException:
            print(f'Log group {log_group_name} not found.')
        finally:
            return last_execution_time


if __name__ == '__main__':
    # for broker_state_machine, broker_state_machine_arn in broker_state_machines.items():
    #     print(f'{broker_state_machine}: {broker_state_machine_arn}')

    # stateMachineExecutionResult = get_state_machine_last_run('arn:aws:states:ap-southeast-2:859004686855:stateMachine:tmgm-production-2nd-parallel-state')
    # print(stateMachineExecutionResult)

    broker = 'dlsm'
    branch = 'production'
    # get all state machine status
    aws_toolkits = AwsToolkits(broker, branch)
    # aws_sf_client = Boto3ClientSingleton('stepfunctions')
    aws_toolkits.get_broker_all_state_machines_last_run_formatted()
    # state_machines_status = get_broker_all_state_machines_last_run_formatted(aws_sf_client, broker)

    # enable_broker_rules(aws_ev_client, broker, branch='dev-iad-2733')
    # disable_broker_rules(aws_ev_client, broker, branch='dev-iad-2733')
