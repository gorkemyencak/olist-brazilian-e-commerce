import sys
from pathlib import Path
PROJECT_ROOT = Path().resolve().parents[0]
sys.path.append(str(PROJECT_ROOT))

from src.dynamicProgramming.feature_extractor import FeatureExtractor

class DPTrainer:

    def __init__(self, value_function):
        self.value_function = value_function
        self.feature_extractor = FeatureExtractor()

    
    def update(
            self,
            state,
            reward,
            next_state,
            gamma = 0.95
    ):
        
        features = self.feature_extractor.extract(state)
        next_features = self.feature_extractor.extract(next_state)

        next_value = self.value_function.predict(next_features)
        
        target = reward + gamma * next_value

        self.value_function.update(features, target)
