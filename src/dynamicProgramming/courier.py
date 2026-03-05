class Courier:
    """ Courier state container """

    def __init__(self, courier_id, start_time):

        self.courier_id = courier_id
        self.route = []
        self.current_time = start_time
    
    def __repr__(self):
        return f"Courier(id={self.courier_id}, route_len={len(self.route)})"
        
        
