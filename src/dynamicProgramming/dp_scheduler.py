import pandas as pd
import copy
import random
from src.dynamicProgramming.state import SystemState
from src.dynamicProgramming.courier import Courier
from src.dynamicProgramming.feature_extractor import FeatureExtractor
from src.routing.route_utils import compute_insertion_cost

"""
1) arrival of new jobs -> add to state.active_jobs
2) assigning active jobs using insertion policy
3) update routes
4) remove completed jobs
"""

class DPScheduler:

    def __init__(self, insertion_policy, n_couriers = 3, gamma = 0.95, value_function = None):
        self.insertion_policy = insertion_policy
        self.n_couriers = n_couriers
        self.gamma = gamma
        self.value_function = value_function

        self.feature_extractor = FeatureExtractor()
    

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

            best_score = float("inf")
            best_action = None # (courier, position)

            # iterating over all couriers
            for courier in state.couriers:

                route = courier.route
                start_time = courier.current_time
                
                # iterating over all insertion positions
                for position in range(len(route) + 1):

                    cost = compute_insertion_cost(
                        route,
                        job,
                        position,
                        start_time,
                        courier.current_lat,
                        courier.current_lng
                    )

                    if cost == float("inf"):
                        continue

                    # DP evaluation
                    score = self._evaluate_action(
                        state,
                        courier,
                        job,
                        position,
                        cost
                    )

                    if score < best_score:
                        best_score = score
                        best_action = (courier, position)
            
            # applying best action
            if best_action is not None:
                courier, pos = best_action
                courier.assign_job(job, pos)
            else:
                remaining_jobs.append(job)
        
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

    
    def _evaluate_action(self, state, courier, job, position, route_cost):
        """
        Evaluate future decisions using:
        route_cost + gamma * V(next_state)
        """

        # if infeasible
        if route_cost == float("inf"):
            return float("inf")
        
        # if no value function 
        if self.value_function is None:
            return route_cost

        route_cost += self.insertion_policy.regularization_lambda * len(courier.route) + random.uniform(0, 1e-6)
        
        # simulate next state
        next_state = copy.deepcopy(state)
        next_courier = next_state.couriers[courier.courier_id]
        next_courier.route.insert(position, job)

        # extracting features
        features = self.feature_extractor.extract(next_state)
        future_value = self.value_function.predict(features)
        score = route_cost + self.gamma * future_value

        return score
            