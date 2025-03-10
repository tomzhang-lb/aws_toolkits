# state machine execution result class
class StateMachineExecutionResult:
    def __init__(self, start_machine_arn, state_machine_name, last_start_time, status):
        """Constructor initialization"""
        self.start_machine_arn = start_machine_arn
        self.state_machine_name = state_machine_name
        self.last_start_time = last_start_time
        self.status = status

    def __getattr__(self, attr):
        """Handles undefined attribute access."""
        return f"'{attr}' attribute not found"

    def __str__(self):
        return f'{self.start_machine_arn}\t{self.state_machine_name}\t{self.last_start_time}\t{self.status}'
