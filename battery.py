class Battery:
    def __init__(self, max_capacity = 13.5, current_capacity = 0, surplus_deficit = 0) -> None:
        self.max_capacity = max_capacity
        self.current_capacity = current_capacity
        self.surplus_deficit = surplus_deficit

    def charge(self, amount: float) -> float:

        self.current_capacity += amount
        self.surplus_deficit = 0
        
        # We charge the battery
        if self.current_capacity >= self.max_capacity:
            self.surplus_deficit = self.current_capacity - self.max_capacity
            self.current_capacity = self.max_capacity


        # We discharge the battery
        elif self.current_capacity <= 0:
            # Return the amount of energy that was not discharged
            self.surplus_deficit = self.current_capacity
            self.current_capacity = 0


    def get_surplus_deficit(self) -> float:
        return self.surplus_deficit
    
    def get_current_capacity(self) -> float:
        return self.current_capacity
    
    def get_percentage(self) -> float:
        return self.current_capacity / self.max_capacity * 100
    

if __name__ == "__main__":
    battery = Battery(max_capacity = 10, current_capacity=5)

    # Test battery charging and discharging
    print(battery.get_current_capacity(), battery.get_surplus_deficit())
    battery.charge(2)
    print(battery.get_current_capacity(), battery.get_surplus_deficit())
    battery.charge(5)
    print(battery.get_current_capacity(), battery.get_surplus_deficit())
    battery.charge(-9)
    print(battery.get_current_capacity(), battery.get_surplus_deficit())
    battery.charge(-10)
    print(battery.get_current_capacity(), battery.get_surplus_deficit())
