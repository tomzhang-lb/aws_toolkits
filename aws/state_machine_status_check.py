import sys
from datetime import datetime
from aws_toolkits import get_broker_all_state_machines_last_run_formatted, Boto3ClientSingleton, disable_broker_rules

if __name__ == '__main__':
    broker = sys.argv[1]
    branch = sys.argv[2]

    aws_sf_client = Boto3ClientSingleton("stepfunctions")
    get_broker_all_state_machines_last_run_formatted(aws_sf_client, broker, branch=branch)

    aws_ev_client = Boto3ClientSingleton('events')
    # disable_broker_rules(aws_ev_client, broker)
