import numpy as np

class FeatureExtractor:
    """ Converting system state into numerical features used by value function approximation """
    
    def extract(self, state):

        num_active_job = len(state.active_jobs)
        route_lengths = [len(c.route) for c in state.couriers]
        completed_jobs = [len(c.completed_jobs) for c in state.couriers]
        avg_route_length = np.mean(route_lengths) if route_lengths else 0
        avg_completed = np.mean(completed_jobs) if completed_jobs else 0
        max_route_length = np.max(route_lengths) if route_lengths else 0
        load_variance = np.var(route_lengths) if route_lengths else 0

        features = np.array([
            num_active_job,
            avg_route_length,
            max_route_length,
            load_variance,
            avg_completed
        ])

        return features
