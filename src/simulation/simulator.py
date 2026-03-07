import pandas as pd

class Simulator:
    """ Dynamic time engine """

    def __init__(
            self,
            stream,
            scheduler
    ):
        
        self.stream = stream
        self.scheduler = scheduler
    
    def run(
            self,
            start_time,
            end_time,
            step_minutes = 30
    ):
        
        current_time = start_time
        state = self.scheduler.initialize_state(current_time)

        while current_time <= end_time:

            # updating system time
            state.current_time = current_time

            new_jobs = self.stream.get_new_jobs(current_time)
            state.add_jobs(new_jobs)

            self.scheduler.update(state)

            current_time += pd.Timedelta(minutes = step_minutes)
        
        return state
        
