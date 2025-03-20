from datetime import datetime


class LambdaFunction:
    def __init__(self, function_name, function_arn, function_version='$LATEST', function_last_executed=datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'), function_last_modified=datetime.strptime('1970-01-01 00:00:00', '%Y-%m-%d %H:%M:%S'), active_within_three_months='Y'):
        self.function_name = function_name
        self.function_arn = function_arn
        self.function_version = function_version
        self.function_last_executed = function_last_executed
        self.function_last_modified = function_last_modified
        self.active_within_three_months = active_within_three_months

    def __getattr__(self, attr):
        """Handles undefined attribute access."""
        return f"'{attr}' attribute not found"

    def __eq__(self, other):
        if isinstance(other, LambdaFunction):
            return self.function_name == other.function_name
        return False

    def __hash__(self):
        return hash(self.function_name)

    def __repr__(self):
        return f'{self.function_name}\t{self.function_arn}\t{self.function_version}\t{self.function_last_executed}\t{self.function_last_modified}\t{self.active_within_three_months}'
