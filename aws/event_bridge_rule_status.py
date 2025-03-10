class EventBridgeRuleState:
    def __init__(self, rule_name, state, rule_event_bus_name, managed_by, target_ids):
        """Constructor initialization"""
        self.rule_name = rule_name
        self.state = state
        self.rule_event_bus_name = rule_event_bus_name
        self.managed_by = managed_by
        self.target_ids = target_ids

    def __getattr__(self, attr):
        """Handles undefined attribute access."""
        return f"'{attr}' attribute not found"

    def __repr__(self):
        return f'{self.rule_name}\t{self.state}\t{self.rule_event_bus_name}\t{self.managed_by}\t{self.target_ids}'