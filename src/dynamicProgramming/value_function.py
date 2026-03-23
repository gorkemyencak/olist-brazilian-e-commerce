import numpy as np
import xgboost as xgb

class ValueFunction:
    """ 
    XGBoost-based value function approximation 
    V(s) = model(features(s))
    """

    def __init__(self):
        self.model = xgb.XGBRegressor(
            n_estimators = 200,
            max_depth = 6,
            learning_rate = 0.05,
            subsample = 0.8,
            colsample_bytree = 0.8,
            random_state = 12
        )

        self.is_trained = False
    

    def fit(self, X, y):
        """ Train model on dataset """
        self.model.fit(X, y)
        self.is_trained = True
    

    def predict(self, features):
        """ Predicting the value of a state """
        if not self.is_trained:
            return 0.0
        
        try:
            features = np.array(features).reshape(1, -1)
            pred = self.model.predict(features)[0]

            if np.isnan(pred):
                return 0.0
            
            return float(pred)
        
        except Exception as e:
            print("Prediction error:", e)
            return 0.0