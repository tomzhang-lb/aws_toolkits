import sys
from aws_toolkits import aws_toolkits

if __name__ == '__main__':
    broker = sys.argv[1]
    branch = sys.argv[2]

    aws_client = aws_toolkits(broker, branch)

    # list broker step function latest execution status
    # aws_client.get_broker_all_state_machines_last_run_formatted()

    # broker rule status
    # aws_client.get_broker_rule_status()

    # broker branch rules
    # aws_client.list_broker_rules()

    # broker
    aws_client.disable_broker_rules()
