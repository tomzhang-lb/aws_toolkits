# aws_toolkits
code to interact with AWS programmatically 

# Example Usage
Try these out in this repo!

## Prepare the client
```python
from aws_toolkits import aws_toolkits
broker = '{broker}' # such as tmgm
branch = '{git_branch}' # such as production
aws_client = aws_toolkits(broker, branch)
```

## Returns the latest execution for given broker and git branch
```python
aws_client.get_broker_rule_status()
```

## Return rule status for given broker and git branch
```python
aws_client.list_broker_rules()
```

## Disable all rules for given broker and git branch
```python
aws_client.disable_broker_rules()
```

## Enable all rules for given broker and git branch, currently not working
```python
aws_client.disable_broker_rules()
```
