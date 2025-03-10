class StateMachine:
    def __init__(self, state_machine_name, state_machine_arn, state_machine_creation_time, state_machine_type, state_machine_last_executed,last_execution_status, executed_beyond_three_months):
        self.state_machine_name = state_machine_name
        self.state_machine_arn = state_machine_arn
        self.state_machine_creation_time = state_machine_creation_time
        self.state_machine_type = state_machine_type
        self.state_machine_last_executed = state_machine_last_executed
        self.last_execution_status = last_execution_status
        self.executed_beyond_three_months = executed_beyond_three_months

    def __getattr__(self, attr):
        """Handles undefined attribute access."""
        return f"'{attr}' attribute not found"

    def __repr__(self):
        return f'{self.state_machine_name}\t{self.state_machine_arn}\t{self.state_machine_creation_time}\t{self.state_machine_type}\t{self.state_machine_last_executed}\t{self.last_execution_status}\t{self.executed_beyond_three_months}'
