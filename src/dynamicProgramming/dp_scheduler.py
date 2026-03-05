import pandas as pd
from src.dynamicProgramming.state import SystemState
from src.dynamicProgramming.courier import Courier

"""
1) arrival of new jobs -> add to state.active_jobs
2) remove finished jobs
3) apply greedy time-window aware VRP insertion policy
4) update routes
5) remove completed jobs
"""

class DPScheduler:

    def __init__(self, insertion_policy, n_couriers = 3):
        self.insertion_policy = insertion_policy
        self.n_couriers = n_couriers
    
    def initialize_state(self, start_time):

        state = SystemState(current_time = start_time)

        # initialize couriers
        for i in range(self.n_couriers):
            courier = Courier(
                courier_id = i,
                start_time = start_time
            )

            state.couriers.append(courier)
        
        return state
    
    def update(self, state):

        # Step 1: remove completed jobs
        self._remove_completed_jobs(state)

        # Step 2: assign active jobs
        self._assign_jobs(state)


    def _assign_jobs(self, state):

        remaining_jobs = []

        for job in state.active_jobs:

            courier, position = self.insertion_policy.select_courier(
                couriers = state.couriers,
                job = job
            )

            if courier is not None:
                courier.route.insert(position, job)
            else:
                # no feasible insertion
                remaining_jobs.append(job)
        
        state.active_jobs = remaining_jobs

    
    def _remove_completed_jobs(self, state):

        for courier in state.couriers:

            remaining_route = []

            for job in courier.route:

                # if job due_date not yet passed -> keep in remmaining route
                if job['due_date'] > state.current_time:
                    remaining_route.append(job)
            
            courier.route = remaining_route


