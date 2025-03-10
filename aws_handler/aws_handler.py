import os
import sys
admin_root = os.path.dirname(os.path.dirname(__file__))
sys.path.append(admin_root)

from aws.aws_toolkits import AwsToolkits

if __name__ == '__main__':
    broker = sys.argv[1]
    branch = sys.argv[2]
    action = sys.argv[3]
    # broker = 'anzo'
    # branch = 'dev-iad-1821'
    # action = 'delete_broker_state_machines'

    aws_toolkits = AwsToolkits(broker, branch)
    command = f'{action}'
    if hasattr(aws_toolkits, command):
        getattr(aws_toolkits, command)()