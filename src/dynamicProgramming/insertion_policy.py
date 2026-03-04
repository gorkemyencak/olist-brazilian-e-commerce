class GreedyInsertionPolicy:
    """ Decision rule -> Greedy earliest due-date insertion implementation """

    def insert(self, state):

        # sort by due data
        state.active_jobs.sort(
            key = lambda x: x['due_date']
        )

        # assign to first available courier:
        for job in state.active_jobs:

            bool_assigned = False

            for courier_id, courier in state.courier.items():
                if courier['available_time'] <= state.current_time:
                    courier['route'].append(job)
                    courier['available_time'] = job['due_date']

                    bool_assigned = True
                    break
            
            if not bool_assigned:
                # create new courier
                new_courier_id = len(state.couriers) + 1
                state.courier[new_courier_id] = {
                    'route': [job],
                    'available_time': job['due_date']
                }


