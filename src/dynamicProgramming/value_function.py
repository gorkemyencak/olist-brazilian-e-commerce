import numpy as np

class ValueFunction:
    """ 
    Linear value function approximation (Bellmann)
    V(s) = w x features(s)
    """

    def __init__(self, num_features, learning_rate = 0.01):

        self.weights = np.zeros(num_features)
        self.learning_rate = learning_rate

    
    def predict(self, features):

        return np.dot(self.weights, features)
    

    def update(self, features, target):

        prediction = self.predict(features)
        
        error = target - prediction

        self.weights += self.learning_rate * error * features
