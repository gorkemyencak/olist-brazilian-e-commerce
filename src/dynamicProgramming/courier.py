from datetime import timedelta
import pandas as pd
from src.routing.route_utils import travel_time_minutes

class Courier:
    """ Courier state container with route and completed jobs tracking """

    def __init__(self, courier_id, start_time, start_lat = None, start_lng = None):

        self.courier_id = courier_id

        # route -> ordered list of jobs currently in the route
        self.route = []

        # ordered list of jobs already finished
        self.completed_jobs = []

        # starting simulation time
        self.start_time = start_time
        self.current_time = start_time

        # current courier location
        self.current_lat = start_lat
        self.current_lng = start_lng
    
    
    def __repr__(self):
        return f"Courier(id={self.courier_id}, route_len={len(self.route)}, completed={len(self.completed_jobs)})"
    
    ### Job Assignment Methods
    def assign_job(self, job, position = None):
        """ 
        Add a new job to the route 
        If position is None, append to end of route. Otherwise, insert at given index
        """
        
        if position is None:
            self.route.append(job)
        else:
            self.route.insert(position, job)

    def assign_jobs_batch(self, jobs):
        """ Add multiple jobs at the end of route sequentially """
        for job in jobs:
            self.assign_job(job)

    ### Job Execution Methods
    def execute_next_job(self):
        """
        Executing next job in the route
            - updating current time (arrical + service)
            - updating current location
            - moving job from the route to completed jobs
        """

        if not self.route:
            return None
        
        job = self.route.pop(0)

        # Compute travel time if courier has a current location
        if self.current_lat is not None and self.current_lng is not None:
            travel_min = travel_time_minutes(
                self.current_lat,
                self.current_lng,
                job['pickup_lat'],
                job['pickup_lng']
            )
        else:
            travel_min = 0

        # Update current time with travel + service
        arrival_time = self.current_time + timedelta(minutes = travel_min)

        # Respect job ready time
        if arrival_time < job['ready_time']:
            arrival_time = job['ready_time']

        # update current time (assumption -> service time = job duration)
        self.current_time = arrival_time + timedelta(
            minutes = job['service_time_min']
        )

        # update courier location to the delivery location
        self.current_lat = job['delivery_lat']
        self.current_lng = job['delivery_lng']

        # store completed job
        self.completed_jobs.append(job)

        return job
    

    def execute_all_jobs(self):
        """ Execute all jobs in the current route sequentially """
        while self.route:
            self.execute_next_job()

    ### Utility Methods
    def current_route_length(self):
        """ Return number of jobs currently in the route """
        return len(self.route)
    
    def total_completed_jobs(self):
        """ Return number of completed jobs """
        return len(self.completed_jobs)

    def get_route_job_ids(self):
        """ Return list of job IDs currently in the route """
        return [job['job_id'] for job in self.route]

    def get_completed_job_ids(self):
        """ Return list of completed job IDs """
        return [job['job_id'] for job in self.completed_jobs]        
        
