import pandas as pd
from src.dynamicProgramming.state import SystemState
from src.dynamicProgramming.courier import Courier

"""
1) arrival of new jobs -> add to state.active_jobs
2) assigning active jobs using insertion policy
3) update routes
4) remove completed jobs
"""

class DPScheduler:

    def __init__(self, insertion_policy, n_couriers = 3):
        self.insertion_policy = insertion_policy
        self.n_couriers = n_couriers
    

    def initialize_state(self, start_time):

        state = SystemState(current_time = start_time)

        # initialize courier fleet
        state.couriers = []

        for i in range(self.n_couriers):
            courier = Courier(
                courier_id = i,
                start_time = start_time,
                start_lat=-23.55,
                start_lng=-46.63
            )

            state.couriers.append(courier)
        
        return state
    

    def update(self, state):

        # Step 1: remove completed jobs
        self._remove_completed_jobs(state)

        # Step 2: assign active jobs
        self._assign_jobs(state)


    def _assign_jobs(self, state):

        if len(state.active_jobs) == 0:
            return

        remaining_jobs = []

        for job in state.active_jobs:

            # convert pandas row into dict if needed
            if hasattr(job, 'to_dict'):
                job = job.to_dict()

            courier, position = self.insertion_policy.select_courier(
                couriers = state.couriers,
                job = job
            )

            if courier is not None:
                # insert job into route
                courier.assign_job(
                    job,
                    position
                )
            else:
                # no feasible insertion
                remaining_jobs.append(job)
        
        # update active jobs
        state.active_jobs = remaining_jobs

    
    def _remove_completed_jobs(self, state):
        """ Move jobs that have been completed (arrival time <= current time) from route to completed jobs """

        for courier in state.couriers:
            # Execute jobs in the route sequentially if feasible
            
            while courier.route:
                next_job = courier.route[0]

                if next_job['ready_time'] <= state.current_time:
                    # if the job is ready and can be done now, execute it
                    courier.execute_next_job()
                else:
                    break
            