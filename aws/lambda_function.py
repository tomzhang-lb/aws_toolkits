class LambdaFunction:
    def __init__(self, function_name, function_arn, function_version, function_last_executed, function_last_modified, executed_within_three_months):
        self.function_name = function_name
        self.function_arn = function_arn
        self.function_version = function_version
        self.function_last_executed = function_last_executed
        self.function_last_modified = function_last_modified
        self.executed_within_three_months = executed_within_three_months

    def __getattr__(self, attr):
        """Handles undefined attribute access."""
        return f"'{attr}' attribute not found"

    def __repr__(self):
        return f'{self.function_name}\t{self.function_arn}\t{self.function_version}\t{self.function_last_executed}\t{self.function_last_modified}\t{self.executed_within_three_months}'
