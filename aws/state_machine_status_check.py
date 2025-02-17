import sys
from datetime import datetime
from aws_toolkits import get_broker_all_state_machines_last_run_formatted, Boto3ClientSingleton, disable_broker_rules, \
    list_rules, delete_broker_rules, get_broker_rule_status, list_rule_target_ids

if __name__ == '__main__':
    broker = sys.argv[1]
    branch = sys.argv[2]

    # aws_sf_client = Boto3ClientSingleton("stepfunctions")
    # get_broker_all_state_machines_last_run_formatted(aws_sf_client, broker, branch=branch)

    aws_ev_client = Boto3ClientSingleton('events')
    # disable_broker_rules(aws_ev_client, broker)
    # event_bridge_rule_states = get_broker_rule_status(aws_ev_client, broker, branch)

    # delete rules
    broker_rules = list_rules(aws_ev_client, broker, branch)
    delete_broker_rules(aws_ev_client, broker_rules)

