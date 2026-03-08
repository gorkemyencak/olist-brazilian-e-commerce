import pandas as pd
import math

"""
Route Utils Module

    1) Travel time computation
    2) Route arrival time propogation
    3) Time-window feasibility check
    4) Route duration computation
    5) Route travel distance computation
    6) Insertion feasilbility evaluation
    7) Marginal cost computation for insertion
"""

### 1) Travel Time Computation
earth_radius_km = 6371
courier_speed_in_city_kmh = 30.00

def haversine_distance(
        lat1,
        lon1,
        lat2,
        lon2
):
    """ Computing distance in km between two lat/lng points"""
    
    lat1, lon1, lat2, lon2 = map(
        math.radians, [lat1, lon1, lat2, lon2]
    )
    
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

    hours = distance_km / courier_speed_in_city_kmh
    return 60 * hours


### 2) Route Arrival Time Propogation
def compute_route_schedule(route, start_time):
    """
    Given a route (list of jobs), compute arrival/departure schedule for a courier route.
    It returns:
        - A bool feasibility variable,
        - A schedule including the list of dictionary with arrival_time, departure_time
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

        # time-window check -> wait if early
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
    """ Checking whether route satisfies all time windows """
    feasibility, _ = compute_route_schedule(route, start_time)
    return feasibility

### 4) Route Duration Computation
def route_duration(route, start_time):
    """ Computing route completion time in minutes """

    feasibility, schedule = compute_route_schedule(route, start_time)

    if not feasibility:
        return float('inf')
    
    if not schedule:
        return 0
    
    duration = (
        schedule[-1]['departure_time'] - start_time
    ).total_seconds() / 60
    
    return duration

### 5) Route Travel Distance Computation
def route_distance(route):
    """ Computing route travel distance for a route """

    if len(route) < 2:
        return 0
    
    distance = 0

    for i in range(len(route) - 1):

        job_1 = route[i]
        job_2 = route[i+1]

        distance += haversine_distance(
            lat1 = job_1['delivery_lat'],
            lon1 = job_1['delivery_lng'],
            lat2 = job_2['pickup_lat'],
            lon2 = job_2['pickup_lng']
        )

    return distance
    
### 6) Insertion feasibility
def check_insertion_feasible(
        route,
        new_job,
        position,
        start_time
):
    """ Checking if inserting job at a position will violate the route feasiblility or not """

    new_route = route.copy()

    new_route.insert(position, new_job)

    return route_feasibility(new_route, start_time)

### 7) Marginal Cost Computation for Insertion
def compute_insertion_cost(
        route,
        new_job,
        position,
        start_time,
        current_lat = None,
        current_lng = None
):
    """ 
    Compute additional route duration after inserting 'new job' at 'position' 
    If current_lat/current_lng provided, use it as route start
    Returns:
        - marginal cost
        - None if infeasble
    """

    if not route:
        # single job insertion
        travel_min = 0
        if current_lat is not None and current_lng is not None:
            travel_min = travel_time_minutes(
                current_lat,
                current_lng,
                new_job['pickup_lat'],
                new_job['pickup_lng']
            )
        
        return travel_min + new_job['service_time_min']

    cost = route_duration(route, start_time)

    new_route = route.copy()
    new_route.insert(position, new_job)

    new_cost = route_duration(new_route, start_time)
    if new_cost == float('inf'):
        return None
    
    marginal_cost = new_cost - cost
    return marginal_cost


