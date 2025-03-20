import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import wraps

from dateutil import parser
from prettytable import PrettyTable
from dateutil.relativedelta import relativedelta
from aws.boto3_client import Boto3ClientSingleton
from aws.event_bridge_rule_status import EventBridgeRuleState
from aws.lambda_function import LambdaFunction
from aws.state_machine import StateMachine
from aws.state_machine_execution_result import StateMachineExecutionResult
from aws.utils import round_to_half_hour


class AwsToolkits:
    def __init__(self, broker, branch):
        self.broker = broker.lower()
        self.branch = branch.lower()
        self.__aws_sf_client = Boto3ClientSingleton('stepfunctions')
        self.__aws_events_client = Boto3ClientSingleton('events')
        self.__aws_lambda_client = Boto3ClientSingleton('lambda')
        self.__aws_log_client = Boto3ClientSingleton('logs')
        self.__check_time = round_to_half_hour(dt=datetime.now())
        self.__three_months_ago = self.__check_time - relativedelta(months=3)

    def time_logger(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            start_time = time.time()
            result = func(self, *args, **kwargs)
            end_time = time.time()
            print(f'Time elapsed: {end_time - start_time:.2f} seconds')
            return result
        return wrapper

    def get_broker_state_machine_arn(self):
        state_machines = []
        response_state_machines = []
        response = self.__aws_sf_client.list_state_machines(maxResults=1000)
        response_state_machines.extend(response['stateMachines'])

        if 'nextToken' in response:
            next_token = response['nextToken']
        else:
            next_token = None

        while next_token is not None:
            response = self.__aws_sf_client.list_state_machines(maxResults=1000, nextToken=next_token)
            response_state_machines.extend(response['stateMachines'])
            if 'nextToken' in response:
                next_token = response['nextToken']
            else:
                next_token = None

        # Print the state machines
        for sm in response_state_machines:
            state_machine_name = sm['name']
            state_machine_arn = sm['stateMachineArn']
            state_machine_type = sm['type']
            state_machine_creation_time = sm['creationDate'].replace(tzinfo=None)

            if state_machine_name.lower().startswith(f'{self.broker}-{self.branch}'):
                state_machine = StateMachine(state_machine_name, state_machine_arn, state_machine_creation_time, state_machine_type,'1970-01-01','SUCCEEDED', 'Yes')
                state_machines.append(state_machine)

        return state_machines

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
            last_start_date = execution_result['startDate'].replace(tzinfo=None).replace(microsecond=0)
            status = execution_result['status']
            state_machine_execution_result = StateMachineExecutionResult(state_machine_arn, state_machine_name, last_start_date, status)
        else:
            state_machine_execution_result = StateMachineExecutionResult(state_machine_arn, state_machine_name, '1970-01-01 00:00:00','NOT_RUN')

        return state_machine_execution_result

    def __get_broker_state_machines_last_run(self):
        state_machines_status = []

        state_machine_arns = self.get_broker_state_machine_arn()
        if state_machine_arns:
            for state_machine in state_machine_arns:
                state_machine_arn = state_machine.state_machine_arn
                state_machine_execution_result = self.get_state_machine_last_run(state_machine_arn)
                state_machines_status.append(state_machine_execution_result)

                # it will be safe to drop the state machine whose last run beyond 3 months
                if (self.__check_time - state_machine.state_machine_creation_time).total_seconds() / 3600/24 >= 90:
                    executed_beyond_three_months = 'Yes'
                else:
                    executed_beyond_three_months = 'No'

                state_machine.executed_beyond_three_months = executed_beyond_three_months
                state_machine.state_machine_last_executed = state_machine_execution_result.last_start_time
                state_machine.last_execution_status = state_machine_execution_result.status
        else:
            print(f'No such state machine found: {self.broker}-{self.branch}* hence no execution results')

        return state_machines_status, state_machine_arns

    def get_broker_state_machines_status_for_release(self):
        output_table = PrettyTable(['State_Machine', 'Check_Time', 'Last_Start_Time', 'Status', 'Ready_To_Release'])
        output_table.align = 'l'
        state_machines_status, state_machine_arns = self.__get_broker_state_machines_last_run()

        if state_machines_status:
            for result in state_machines_status:
                state_machine = result.state_machine_name
                last_start_time = result.last_start_time
                status = result.status

                '''
                1.) if the state machine finished and start time later than check time, then this state machine is finished
                2.) if the state machine finished but start time before check time and within minutes, then it is not start yet
                3.) if the start machine finished but start time before check time more than 1 hour, then it is not a half an hour job
                '''

                if (status == 'SUCCEEDED' and last_start_time > self.__check_time) or (status == 'NOT_RUN'):
                    ready_to_release = 'YES'
                elif status == 'SUCCEEDED' and last_start_time < self.__check_time and (self.__check_time - last_start_time).total_seconds() / 3600 < 0.5:
                    ready_to_release = 'NOT_START'
                elif status == 'SUCCEEDED' and last_start_time < self.__check_time and (self.__check_time - last_start_time).total_seconds() / 3600 > 1:
                    ready_to_release = 'YES'
                elif status == 'RUNNING':
                    ready_to_release = 'NO'
                else:
                    ready_to_release = 'MANUAL_CHECK'

                output_table.add_row([state_machine, self.__check_time, last_start_time, status, ready_to_release])
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
        rule_table.align = 'l'

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
        rule_table.align = 'l'

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

    def get_lambda_function_last_execution(self, function_name):
        log_group_name = f'/aws/lambda/{function_name}'
        last_execution_time = datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
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
                last_execution_time = datetime.fromtimestamp(last_event_timestamp / 1000).replace(microsecond=0)
                # print(f'Last execution time: {last_execution_time}')
            else:
                print('No executions found for this Lambda.')

        except self.__aws_log_client.exceptions.ResourceNotFoundException:
            # print(f'Log group {log_group_name} not found.')
            pass
        finally:
            return function_name, last_execution_time

    def list_broker_lambda_functions_to_purge(self):
        functions = []
        function_table = PrettyTable(['Function_Name', 'Function_Arn', 'Version', 'Last_Executed', 'Last_Modified', 'Executed_Within_3_Months'])
        function_table.align = 'l'
        paginator = self.__aws_lambda_client.get_paginator('list_functions')

        for page in paginator.paginate():
            for function in page['Functions']:
                # print(function)
                function_name = function['FunctionName']
                function_arn = function['FunctionArn']
                function_version = function['Version']
                function_last_modified = function['LastModified']

                if function_name.lower().startswith(f'{self.broker}-{self.branch}'):
                    function_name_return, function_last_executed = self.get_lambda_function_last_execution(function_name)
                    active_within_three_months = 'Y' if function_last_executed > self.__three_months_ago or function_last_modified > self.__three_months_ago else 'N'

                    lambda_function = LambdaFunction(function_name, function_arn, function_version,
                                                     function_last_executed, function_last_modified, active_within_three_months)
                    function_table.add_row(
                        [function_name, function_arn, function_version, function_last_executed, function_last_modified, active_within_three_months])
                    functions.append(lambda_function)
                else:
                    pass

        print(function_table.get_string(sortby='Last_Executed', reversesort=False))
        return functions

    def __delete_lambda_functions(self, function):
        try:
            response = self.__aws_lambda_client.delete_function(
                FunctionName=function.function_name
                # Qualifier=function.function_version
            )
            # print(response['ResponseMetadata']['HTTPStatusCode'])
        except self.__aws_lambda_client.exceptions.ResourceNotFoundException:
            print(f'Function {function.function_name} not found.')

    def delete_broker_lambda_functions(self):
        functions = self.list_broker_lambda_functions_to_purge()
        print(f'Functions to delete:')
        function_purge_table = PrettyTable()
        function_purge_table.field_names = ['Function_Name', 'Function_Arn', 'Version', 'Last_Executed', 'Last_Modified', 'Executed_Within_3_Months']
        function_purge_table.align = 'l'
        for function in functions:
            if function.executed_within_three_months == 'N':
                function_purge_table.add_row([function.function_name, function.function_arn, function.function_version, function.function_last_executed, function.function_last_modified, function.executed_within_three_months])

        print(function_purge_table.get_string(sortby='Last_Executed', reversesort=True))

        confirm_delete = input('CONFIRM TO DELETE?\n')
        if confirm_delete.lower() == 'yes' or confirm_delete.lower() == 'y':
            try:
                for function in functions:
                    if function.executed_within_three_months == 'N':
                        self.__delete_lambda_functions(function)
                        print(f'Function: {function.function_name} get deleted.')
            except Exception as e:
                print(e)
        else:
            print(f'Confirm to NOT delete.')

    def get_broker_state_machines_to_purge(self):
        state_machines_to_purge = []
        output_table = PrettyTable(['State_Machine', 'Creation_Time', 'Last_Start_Time', 'Safe_to_Delete'])
        output_table.align = 'l'
        state_machines_status, state_machine_arns = self.__get_broker_state_machines_last_run()

        if state_machine_arns:
            for state_machine in state_machine_arns:
                # it will be safe to drop the state machine whose last run beyond 3 months
                if state_machine.executed_beyond_three_months == 'Yes':
                    state_machines_to_purge.append(state_machine)
                    output_table.add_row([state_machine.state_machine_name, state_machine.state_machine_creation_time, state_machine.state_machine_last_executed, state_machine.executed_beyond_three_months,])
            print(output_table.get_string(sortby='Creation_Time', reversesort=True))
        else:
            pass

        return state_machines_to_purge

    def __delete_state_machine(self, state_machine_arn):
        try:
            response = self.__aws_sf_client.delete_state_machine(
                stateMachineArn=state_machine_arn
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                print(f'State Machine {state_machine_arn} deleted.')
            else:
                print(f'Error in deleting State Machine {state_machine_arn}.')
        except self.__aws_sf_client.exceptions.ResourceNotFoundException:
            print(f'State Machine {state_machine_arn} not found.')

    def delete_broker_state_machines(self):
        state_machines_to_purge = self.get_broker_state_machines_to_purge()

        if state_machines_to_purge:
            confirm_delete = input('CONFIRM TO DELETE?\n')
            if confirm_delete.lower() == 'yes' or confirm_delete.lower() == 'y':
                for state_machine in state_machines_to_purge:
                    try:
                        self.__delete_state_machine(state_machine.state_machine_arn)
                    except Exception as e:
                        print(f'Error in deleting State Machine {state_machine.state_machine_arn} {e}.')
            else:
                print(f'Confirm to NOT delete')
        else:
            print(f'No state machines found to delete.')

    def get_direct_lambda_functions_from_state_machine(self, state_machine_arn):
        function_arns = []
        try:
            # Get state machine definition
            response = self.__aws_sf_client.describe_state_machine(stateMachineArn=state_machine_arn)
            definition = json.loads(response['definition'])
            # get direct functions
            pattern = r"arn:aws:lambda:[\w\-]+:\d+:function:[\w\-]+"
            function_arns = re.findall(pattern, json.dumps(definition))
        except Exception as e:
            print(f"Error fetching state machine: {e}")
        finally:
            return function_arns

    # get the lambda functions recursively in case orch contains another orch
    def get_lambda_functions_from_state_machine_recursive(self, state_machine_arn, function_arns=set()):
        try:
            # Get state machine definition
            response = self.__aws_sf_client.describe_state_machine(stateMachineArn=state_machine_arn)
            definition = json.loads(response['definition'])

            # get direct functions
            function_pattern = r"arn:aws:lambda:[\w\-]+:\d+:function:[\w\-]+"
            function_arns_list = re.findall(function_pattern, json.dumps(definition))
            function_arns.update(function_arns_list)

            # get underneath state machines
            state_machine_pattern = r"arn:aws:states:[\w\-]+:\d+:stateMachine:[\w\-]+"
            state_machine_arns = re.findall(state_machine_pattern, json.dumps(definition))

            if state_machine_arns:
                for state_machine_arn in state_machine_arns:
                    self.get_lambda_functions_from_state_machine_recursive(state_machine_arn)

        except Exception as e:
            print(f"Error fetching state machine: {e}")
        finally:
            return function_arns

    # @time_logger
    def list_state_machine_lambda_function_status(self, state_machine_arn):
        lambda_functions = []
        function_arns = self.get_direct_lambda_functions_from_state_machine(state_machine_arn)

        if function_arns:
            for function_arn in function_arns:
                lambda_function = LambdaFunction(function_arn.split(':')[-1], function_arn)
                lambda_functions.append(lambda_function)
        else:
            return lambda_functions

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.get_lambda_function_last_execution, lambda_function.function_name) for lambda_function in lambda_functions]

        # update lambda_function last executed
        for future in as_completed(futures):
            try:
                function_name, function_last_executed = future.result()

                for lambda_function in lambda_functions:
                    if lambda_function.function_name == function_name:
                        lambda_function.function_last_executed = function_last_executed
            except Exception as e:
                print(e)

        return lambda_functions

    def get_broker_state_machine_lambda_function_status(self):
        lambda_functions = set()
        # get all the state machine for the given broker + branch
        state_machines = self.get_broker_state_machine_arn()
        # get all the functions under each state machines
        if state_machines:
            for state_machine in state_machines:
                lambda_functions_tmp = self.list_state_machine_lambda_function_status(state_machine.state_machine_arn)
                lambda_functions.update(lambda_functions_tmp)

        # get function last modified info through thread pool
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.__aws_lambda_client.get_function, FunctionName=lambda_function.function_name) for lambda_function in lambda_functions]

        for future in as_completed(futures):
            try:
                response = future.result()

                function_name = response['Configuration']['FunctionName']
                function_last_modified = parser.parse(response['Configuration']['LastModified']).replace(tzinfo=None, microsecond=0)
                function_version = response['Configuration']['Version']

                for lambda_function in lambda_functions:
                    if lambda_function.function_name == function_name:
                        lambda_function.function_version = function_version
                        lambda_function.function_last_modified = function_last_modified
                        lambda_function.active_within_three_months = 'Y' if lambda_function.function_last_executed > self.__three_months_ago or lambda_function.function_last_modified > self.__three_months_ago else 'N'
            except Exception as e:
                print(e)

        return lambda_functions

    def delete_broker_state_machines_lambda_function(self):
        lambda_functions = self.get_broker_state_machine_lambda_function_status()
        # stale_lambda_functions = [lambda_function for lambda_function in lambda_functions if lambda_function.active_within_three_months == 'N']
        stale_lambda_functions = []
        stale_lambda_function_table = PrettyTable(['Function_Name', 'Last_Executed_Time', 'Last_Modified_Time', 'Active_Within_Three_Months'])
        stale_lambda_function_table.align = 'l'

        if lambda_functions:
            for lambda_function in lambda_functions:
                if lambda_function.active_within_three_months == 'N':
                    stale_lambda_function_table.add_row([lambda_function.function_name, lambda_function.function_last_executed, lambda_function.function_last_modified, lambda_function.active_within_three_months])
                    stale_lambda_functions.append(lambda_function)
        else:
            print('No state machine functions found.')
            return stale_lambda_functions

        if stale_lambda_functions:
            print(f'Total {len(stale_lambda_functions)} stale functions to be purged: ')
            print(stale_lambda_function_table.get_string(sortby='Function_Name', reversesort=True))

            confirm_delete = input('CONFIRM TO DELETE?\n')
            if confirm_delete.lower() == 'yes' or confirm_delete.lower() == 'y':
                for lambda_function in stale_lambda_functions:
                    try:
                        self.__delete_lambda_functions(lambda_function)
                    except Exception as e:
                        print(f'Error deleting function: {lambda_function.function_name} {e}')
            else:
                print(f'Confirm to NOT delete')
        else:
            print(f'No stale state machine function found to delete.')

        return stale_lambda_functions

    # multi threads call
    # def list_lambda_functions(self, marker=None):
    #     """Fetch a batch of Lambda functions using pagination."""
    #     params = {}
    #     params['MaxItems'] = 50
    #     if marker:
    #         params['Marker'] = marker
    #
    #     response = self.__aws_lambda_client.list_functions(**params)
    #     functions = response.get('Functions')
    #     next_marker = response.get('NextMarker')
    #
    #     return functions, next_marker
    #
    # def get_all_lambda_functions(self):
    #     """Fetch all Lambda functions using parallel requests with ThreadPoolExecutor."""
    #     all_functions = []
    #     markers = set()
    #     three_months_ago = self.__check_time - relativedelta(months=3)
    #
    #     with ThreadPoolExecutor(max_workers=5) as executor:
    #         futures = []
    #         # Start the first request
    #         futures.append(executor.submit(self.list_lambda_functions))
    #
    #         while futures:
    #             for future in as_completed(futures):
    #                 try:
    #                     functions, next_marker = future.result()
    #                     for function in functions:
    #                         function_name = function['FunctionName']
    #                         function_arn = function['FunctionArn']
    #                         function_version = function['Version']
    #                         function_last_modified = function['LastModified']
    #                         if function_name.lower().startswith(f'{self.broker}-{self.branch}'):
    #                             print(function_name)
    #                             # function_last_executed = self.lambda_last_execution_time(function_name)
    #                             # executed_within_three_months = 'Y' if function_last_executed > three_months_ago else 'N'
    #                             # lambda_function = LambdaFunction(function_name, function_arn, function_version,
    #                             #                                  function_last_executed, function_last_modified,
    #                             #                                  executed_within_three_months)
    #                             # all_functions.append(lambda_function)
    #                         else:
    #                             pass
    #
    #                     # If there's more data, add a new task to the pool
    #                     if next_marker:
    #                         if next_marker not in markers:
    #                             print(next_marker)
    #                             markers.add(next_marker)
    #                             futures.append(executor.submit(self.list_lambda_functions, next_marker))
    #                 except Exception as e:
    #                     print(f"Error: {e}")
    #
    #     return all_functions



if __name__ == '__main__':
    # for broker_state_machine, broker_state_machine_arn in broker_state_machines.items():
    #     print(f'{broker_state_machine}: {broker_state_machine_arn}')

    # stateMachineExecutionResult = get_state_machine_last_run('arn:aws:states:ap-southeast-2:859004686855:stateMachine:tmgm-production-2nd-parallel-state')
    # print(stateMachineExecutionResult)

    broker = 'anzo'
    branch = 'dev-iad-1949-collection-orch'
    # get all state machine status
    aws_toolkits = AwsToolkits(broker, branch)
    # aws_sf_client = Boto3ClientSingleton('stepfunctions')
    start_time = time.time()
    # state_machines = aws_toolkits.list_all_state_machines()
    # state_machines = aws_toolkits.get_broker_state_machine_arn()
    # functions = aws_toolkits.list_broker_functions()
    # function_status = aws_toolkits.list_state_machine_lambda_function_status('arn:aws:states:ap-southeast-2:859004686855:stateMachine:tmgm-dev-iad-2721-calculation-orch')
    lambda_functions = aws_toolkits.delete_broker_state_machines_lambda_function()
    # for lambda_function in lambda_functions:
    #     print(lambda_function)
    # end_time = time.time()
    # print(f'time_elapsed: {end_time - start_time}')
    # print(len(function_status))
    # if functions:
    #     for function in functions:
    #         print(function)
    # else:
    #     print("No state machines found.")
