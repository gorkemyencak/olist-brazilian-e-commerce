"""
State S(t):
    - current_time
    - active_jobs
    - courier routes
    - courier current locations
    - courier availability times
"""

class SystemState:
    """ State Container """

    def __init__(self, current_time):
        self.current_time = current_time
        self.active_jobs = []
        self.couriers = []
    
    
    def add_jobs(self, jobs):
        self.active_jobs.extend(jobs)
    

    def update_time(self, new_time):
        self.current_time = new_time
        

