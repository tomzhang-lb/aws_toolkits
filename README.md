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

## 1.) Returns the latest execution for given broker and git branch
```python
aws_client.get_broker_rule_status()
```

## 2.) Return rule status for given broker and git branch
```python
aws_client.list_broker_rules()
```

## 3.) Disable all rules for given broker and git branch
```python
aws_client.disable_broker_rules()
```

## 4.) Enable all rules for given broker and git branch, currently not working
```python
aws_client.enable_broker_rules()
```

## 5.) List all lambda for given broker and git branch
```python
aws_client.list_broker_functions()
```

## 6.) Delete all lambda for given broker and git branch
```python
aws_client.delete_broker_functions()
```
## TBD
