from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.config import Config
from boto3_client import Boto3ClientSingleton


class CloudformationStack:
    config = Config(
        retries={
            'max_attempts': 10,
            'mode': 'standard'
        }
    )

    def __init__(self):
        self.__aws_cf_client = Boto3ClientSingleton('cloudformation', config=self.config)

    def find_stacks(self, keyword: str, status: list, status_include_flag=True):
        """Return all stacks whose name contains the keyword."""
        matched = []
        if 'production' in keyword.lower():
            print('Production stack is not support!')
            return matched
        else:
            paginator = self.__aws_cf_client.get_paginator('list_stacks')

            for page in paginator.paginate():
                for s in page['StackSummaries']:
                    stack_name = s['StackName']
                    stack_status = s['StackStatus']

                    if status_include_flag:
                        if keyword.lower() in stack_name.lower() and stack_status in status:
                            matched.append(stack_name)
                    else:
                        if keyword.lower() in stack_name.lower() and stack_status not in status:
                            matched.append(stack_name)

            return matched

    def delete_stacks(self, stack_names, force_flag=False):
        """Delete stacks in the given list."""
        deletion_mode = 'STANDARD' if not force_flag else 'FORCE_DELETE_STACK'

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(self.__aws_cf_client.delete_stack, StackName=stack_name, DeletionMode=deletion_mode) for stack_name in stack_names]

            for future in as_completed(futures):
                try:
                    response = future.result()
                    print(f"Call status: {response.get('ResponseMetadata', {}).get('HTTPStatusCode')}")
                except Exception as e:
                    print(e)

        print('Delete requests submitted.')


if __name__ == '__main__':
    status_include_flag = True
    keyword = 'tmgm-uat-iad-794-create-bucket'
    # status = ['CREATE_COMPLETE', 'UPDATE_ROLLBACK_FAILED', 'CREATE_FAILED', 'UPDATE_COMPLETE', 'UPDATE_ROLLBACK_COMPLETE']
    status = ['DELETE_FAILED',]
    # force_flag = False
    force_flag = True


    cf = CloudformationStack()
    stacks = cf.find_stacks(keyword, status, status_include_flag)
    for stack in stacks:
        print(f'Stack to be deleted: {stack}')

    confirm_delete = input('CONFIRM TO DELETE?\n')
    if confirm_delete.lower() == 'yes' or confirm_delete.lower() == 'y':
        cf.delete_stacks(stacks, force_flag)
    else:
        print(f'Confirm to NOT delete')
    
