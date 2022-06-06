class Battery:
    def __init__(self) -> None:
        self.max_capacity = 100
        self.current_capacity = 0
        self.over_under_charge = 0

    def charge(self, amount: float) -> None:
        """Charge battery

        Args:
            amount (float): amount
        """

        self.current_capacity += amount
        
        if self.current_capacity > self.max_capacity:
            yield_ = self.current_capacity - self.max_capacity
            self.current_capacity = self.max_capacity

            self.over_under_charge = yield_
        
    def discharge(self, amount: float) -> float:
        """Discharge battery
            
            Args:
                amount (float): amount
        """

        self.current_capacity += amount
        
        if self.current_capacity < 0:
            # Return the amount of energy that was not discharged
            yield_ = self.current_capacity
            self.current_capacity = 0
        
            self.over_under_charge = yield_
        
        # return amount

    def get_over_under_charge(self) -> float:
        return self.over_under_charge
    
    def get_current_capacity(self) -> float:
        return self.current_capacity
    
    def get_percentage(self) -> float:
        return self.current_capacity / self.max_capacity * 100


def logic():
    """_summary_
    """

    return None
