import pandas as pd
from src.dynamicProgramming.state import SystemState

"""
1) new jobs -> add to state
2) remove finished jobs
3) apply insertion policy
4) update routes
5) advance time
"""

class DPScheduler:

    def __init__(self, insertion_policy):
        self.insertion_policy = insertion_policy
    
    def initialize_state(self, start_time):

        state = SystemState(current_time = start_time)

        # initialize empty couriers dictionary
        state.couriers = {}
        
        return state
    
    def update(self, state):

        # Step 1: remove completed jobs
        self._remove_completed_jobs(state)

        # Step 2: apply insertion policy
        self.insertion_policy.insert(state)
    
    def _remove_completed_jobs(self, state):

        for courier_id, courier in state.couriers.items():

            remaining_route = []

            for job in courier['route']:

                # completion
                if job['due_date'] > state.current_time:
                    remaining_route.append(job)
            
            courier['route'] = remaining_route


