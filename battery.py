class Battery:
    def __init__(self) -> None:
        self.max_capacity = 100
        self.current_capacity = 0

    def charge(self, amount: int) -> None:
        """Charge battery

        Args:
            amount (int): amount
        """

        self.current_capacity += amount
        
        if self.current_capacity > self.max_capacity:
            self.current_capacity = self.max_capacity
        
    def discharge(self, amount: int) -> float:
        """Discharge battery
            
            Args:
                amount (int): amount
        """

        self.current_capacity -= amount
        
        if self.current_capacity < 0:
            # Return the amount of energy that was not discharged
            yield_ = self.current_capacity
            self.current_capacity = 0
        
            return yield_
        
        return amount
    
    def get_current_capacity(self) -> float:
        return self.current_capacity
    
    def get_percentage(self) -> float:
        return self.current_capacity / self.max_capacity * 100



