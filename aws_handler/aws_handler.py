import sys
import os
admin_root = os.path.dirname(os.path.dirname(__file__))
sys.path.append(admin_root)
from aws.aws_toolkits import AwsToolkits

if __name__ == '__main__':
    action = sys.argv[1]
    broker = sys.argv[2]
    branch = sys.argv[3]

    # action = 'delete_broker_state_machines'
    # broker = 'anzo'
    # branch = 'dev-iad-1821'


    aws_toolkits = AwsToolkits(broker, branch)
    command = f'{action}'
    if hasattr(aws_toolkits, command):
        getattr(aws_toolkits, command)()
