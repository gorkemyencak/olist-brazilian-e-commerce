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

