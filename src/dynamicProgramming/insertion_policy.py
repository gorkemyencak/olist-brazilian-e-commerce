"""
Replacing the greedy earliest due-date insertion policy with time-window aware VRP greedy insertion policy:
    1) Inserting new job at every possible position
    2) Checking route feaibility (time-window aware)
    3) Computing new route duration
    4) Selecting the position with minimum additional cost
"""
import sys
from pathlib import Path
PROJECT_ROOT = Path().resolve().parents[0]
sys.path.append(str(PROJECT_ROOT))
from src.routing.route_utils import route_feasibility, route_duration, compute_insertion_cost

class GreedyInsertionPolicy:
    """
    Greedy insertion heuristic for courier assignment

    For each job:
        - try inserting each courier route
        - evaluate feasibility
        - choose minimum insertion cost
    """

    def __init__(self):
        pass

    def find_best_insertion(
            self,
            routes,
            job,
            start_time
    ):
        """ 
        Determining the best courier and position.
        Parameters:
            - routes: dict
                {courier_id: route_list}
            - job: dict
                delivery job
            - start_time: timestamp
                courier shift start
        Returns:
            - best courier
            - best position
            - best cost        
        """

        best_cost = float('inf')
        best_courier = None
        best_position = None

        for courier_id, route in routes.items():

            for pos in range(len(route) + 1):

                cost = compute_insertion_cost(
                    route,
                    job,
                    pos,
                    start_time
                )

                if cost < best_cost:
                    best_cost = cost
                    best_courier = courier_id
                    best_position = pos
        
        return best_courier, best_position, best_cost
    

    def assign_job(
            self,
            routes,
            job,
            start_time
    ):
        """
        Insert job into best courier route
        Parameters:
            - routes: dict
                {courier_id: route_list}
            - job: dict
                delivery job
            - start_time: timestamp
                courier shift start
        Returns:
            - updated routes
            - assignment info
        """

        best_courier, best_position, best_cost = self.find_best_insertion(
            routes,
            job,
            start_time
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
            start_time
    ):
        
        """
        Assigning multiple jobs sequentially
        Parameters:
            - routes: dict
                courier routes
            - jobs: list
                list of job dictionaries
            - start_time: timestamp
                courier shift start
        Returns:
            - routes
            - assignments
        """

        assignments = []

        for job in jobs:

            courier_routes, info = self.assign_job(
                routes,
                job,
                start_time
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

        for route in route.values():

            total_duration += route_duration(
                routes,
                start_time
            )
        
        return total_duration




"""
class GreedyInsertionPolicy:


    def select_courier(
            self,
            couriers,
            job
    ):
        
        best_courier = None
        best_position = None
        best_cost_increase = float('inf')

        for c in couriers:
            current_route = c.route
            start_time = c.current_time

            # Current duration
            current_duration = route_duration(
                current_route,
                start_time
            )       

            # Enumerating all possible insertion positions
            for pos in range(len(current_route) + 1):
                
                new_route = (
                    current_route[:pos]
                    + [job]
                    + current_route[pos:]
                )

                if not route_feasibility(new_route, start_time):
                    continue

                new_duration = route_duration(
                    new_route, 
                    start_time
                )

                cost_increase = new_duration - current_duration

                if cost_increase < best_cost_increase:
                    best_cost_increase = cost_increase
                    best_courier = c
                    best_position = pos
        
        return best_courier, best_position """
