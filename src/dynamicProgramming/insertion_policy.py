"""
Replacing the greedy earliest due-date insertion policy with time-window aware VRP greedy insertion policy:
    1) Inserting new job at every possible position
    2) Checking route feaibility (time-window aware)
    3) Computing new route duration 
    4) Selecting the position with minimum additional cost with regularized insertion cost
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path().resolve().parents[0]
sys.path.append(str(PROJECT_ROOT))
from src.routing.route_utils import route_duration, compute_insertion_cost

class GreedyInsertionPolicy:
    """
    Greedy insertion heuristic for courier assignment

    For each job:
        - try inserting each courier route
        - evaluate feasibility
        - choose minimum insertion cost
    """

    def __init__(self, regularization_lambda = 2.0):
        
        """ Lambda term controls how strongly we penalize long routes """
        self.regularization_lambda = regularization_lambda

    
    def select_courier(
            self,
            couriers,
            job
    ):
        """ 
        Interface used bu DPScheduler 
        Parameters:
            - couriers: list[Courier]
                list of courier containers
            - job: dict
                delivery job
        Returns:
            - courier_object
            - insertion_position
        """

        routes = {}
        courier_lookup = {}

        for courier in couriers:
            routes[courier.courier_id] = courier.route
            courier_lookup[courier.courier_id] = courier
        

        best_courier_id, best_position, best_cost = self.find_best_insertion(
            routes,
            job,
            couriers
        )

        if best_courier_id is None:
            return None, None

        courier_obj = courier_lookup[best_courier_id]

        return courier_obj, best_position


    def find_best_insertion(
            self,
            routes,
            job,
            couriers
    ):
        """ 
        Determining the best courier and position.
        Parameters:
            - routes: dict
                {courier_id: route_list}
            - job: dict
                delivery job
            - couriers: list[Courier]
        Returns:
            - best courier
            - best position
            - best cost        
        """

        best_cost = float('inf')
        best_courier = None
        best_position = None

        for courier_id, route in routes.items():
            courier_best_cost = float('inf')
            courier = next(c for c in couriers if c.courier_id == courier_id)
            start_time = courier.current_time

            for pos in range(len(route) + 1):

                cost = compute_insertion_cost(
                    route,
                    job,
                    pos,
                    start_time,
                    courier.current_lat,
                    courier.current_lng
                )

                if cost is None:
                    continue

                cost += self.regularization_lambda * len(route)

                if cost < courier_best_cost:
                    courier_best_cost = cost

                if courier_best_cost < best_cost:
                    best_cost = courier_best_cost
                    best_courier = courier_id
                    best_position = pos
        
        return best_courier, best_position, best_cost
    

    def assign_job(
            self,
            routes,
            job,
            couriers
    ):
        """
        Insert job into best courier route
        Parameters:
            - routes: dict
                {courier_id: route_list}
            - job: dict
                delivery job
            - couriers: list[Courier]
        Returns:
            - updated routes
            - assignment info
        """

        best_courier, best_position, best_cost = self.find_best_insertion(
            routes,
            job,
            couriers
        )

        if best_courier is None:
            return routes, None
        
        routes[best_courier].insert(
            best_position,
            job
        )

        assignment_info = {
            'job_id': job['job_id'],
            'courier_id': best_courier,
            'position': best_position,
            'cost': best_cost
        }

        return routes, assignment_info
    

    def assign_jobs_batch(
            self,
            routes,
            jobs,
            couriers
    ):
        
        """
        Assigning multiple jobs sequentially
        Parameters:
            - routes: dict
                courier routes
            - jobs: list
                list of job dictionaries
            - couriers: list[Courier]
        Returns:
            - routes
            - assignments
        """

        assignments = []

        for job in jobs:

            courier_routes, info = self.assign_job(
                routes,
                job,
                couriers
            )
        
            if info is not None:
                assignments.append(info)
        
        return courier_routes, assignments


    def compute_total_route_duration(
            self,
            routes,
            start_time            
    ):
        """ Computing route duration of all routes """

        total_duration = 0

        for route in routes.values():

            total_duration += route_duration(
                route,
                start_time
            )
        
        return total_duration
    