import pandas as pd

class OrderStream:
    """ Arrival Generator -> Converting delivery dataset into time-ordered stream """

    def __init__(self, df_delivery):
        
        self.df = df_delivery.sort_values('ready_time')
        self.pointer = 0
    

    def get_new_jobs(self, current_time):

        new_jobs = []

        while self.pointer < len(self.df):

            row = self.df.iloc[self.pointer]

            if row['ready_time'] < current_time:
                new_jobs.append(row)
                self.pointer += 1
            
            else:
                break
        
        return new_jobs
    