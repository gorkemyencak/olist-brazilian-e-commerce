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
from src.routing.route_utils import route_duration, route_feasibility

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
        
        return best_courier, best_position