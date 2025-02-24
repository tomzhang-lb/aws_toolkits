import sys
from aws_toolkits import AwsToolkits

if __name__ == '__main__':
    broker = sys.argv[1]
    branch = sys.argv[2]

    aws_toolkits = AwsToolkits(broker, branch)
    # list broker step function latest execution status
    # aws_toolkits.get_broker_all_state_machines_last_run_formatted()

    # broker rule status
    # aws_toolkits.get_broker_rule_status()

    # broker branch rules
    # aws_toolkits.list_broker_rules()

    # broker
    # aws_toolkits.disable_broker_rules()

    # delete rules
    # aws_toolkits.describe_service()

    # list lambda executions
    # aws_toolkits.lambda_last_execution_time('tmgm-uat-master-crm-categories')

    # list functions
    # aws_toolkits.list_broker_functions()

    # delete functions
    # aws_toolkits.delete_broker_functions()
