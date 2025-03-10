import sys

from aws.aws_toolkits import AwsToolkits

if __name__ == '__main__':
    broker = sys.argv[1]
    branch = sys.argv[2]
    action = sys.argv[3]
    # broker = 'anzo'
    # branch = 'dev-iad-1821'

    aws_toolkits = AwsToolkits(broker, branch)
    # list broker step function latest execution status
    # aws_toolkits.get_broker_state_machines_for_release()

    # broker rule status
    # aws_toolkits.get_broker_rule_status()

    # broker branch rules
    # aws_toolkits.list_broker_rules()

    # broker
    # aws_toolkits.disable_broker_rules()

    # delete rules
    # aws_toolkits.delete_broker_rules()

    # list lambda executions
    # aws_toolkits.lambda_last_execution_time('tmgm-uat-master-crm-categories')

    # list functions
    # aws_toolkits.list_broker_functions()

    # delete functions
    # aws_toolkits.delete_broker_functions()

    # list state machines to delete
    # aws_toolkits.get_broker_state_machines_to_purge()

    # delete state machines
    aws_toolkits.delete_broker_state_machines()
