import pandas as pd
import math

"""
Route Utils will provide:
    1) Travel time computation
    2) Route arrival time propogation
    3) Time-window feasibility check
    4) Route duration computation
    5) Insertion feasilbility evaluation
"""

### 1) Travel Time Computation
earth_radius_km = 6371
orbital_speed_kmh = 29.78

def haversine_distance(
        lat1,
        lon1,
        lat2,
        lon2
):
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin(dlon / 2) ** 2
    )

    c = 2 * math.atan2(
        math.sqrt(a),
        math.sqrt(1 - a)
    )
    return earth_radius_km * c

def travel_time_minutes(
        lat1,
        lon1,
        lat2,
        lon2
):
    """ Converting distance to travel time in minutes """

    distance_km = haversine_distance(
        lat1,
        lon1,
        lat2,
        lon2
    )

    hours = distance_km / orbital_speed_kmh
    return 60 * hours


### 2) Route Arrival Time Propogation
def compute_route_schedule(route, start_time):
    """
    Given a route (list of jobs), compute arrival times.
    It returns:
        - A bool feasibility variable,
        - A schedule including the list of dictionary
    """

    if len(route) == 0:
        return True, []
    
    schedule = []
    current_time = start_time

    for i, job in enumerate(route):
        if i == 0:
            # assuming the courier starts at pickup location
            travel_min = 0
        else:
            preceeding_job = route[i-1]

            travel_min = travel_time_minutes(
                preceeding_job['delivery_lat'],
                preceeding_job['delivery_lng'],
                job['delivery_lat'],
                job['delivery_lng']
            )
        
        arrival_time = current_time + pd.Timedelta(minutes = travel_min)

        # time-window check
        if arrival_time < job['ready_time']:
            arrival_time = job['ready_time']

        # violating due date check
        if arrival_time > job['due_date']:
            return False, []
        
        departure_time = arrival_time + pd.Timedelta(minutes = job['service_time_min'])

        schedule.append({
            'job_id': job['job_id'],
            'arrival_time': arrival_time,
            'departure_time': departure_time
        })

        current_time = departure_time
    
    return True, schedule


### 3) Route Time-Window Feasiblity Check
def route_feasibility(route, start_time):
    feasibility, _ = compute_route_schedule(route, start_time)
    return feasibility

### 4) Route Duration Computation
def route_duration(route, start_time):

    feasibility, schedule = compute_route_schedule(route, start_time)

    if not feasibility:
        return float('inf')
    
    if not schedule:
        return 0
    
    return(schedule[-1]['departure_time'] - start_time).total_seconds() / 60

