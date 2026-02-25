import pandas as pd

class DataValidator:
    def __init__(
            self,
            name: str
    ):
        self.name = name
    
    def validate_basic(
            self,
            df: pd.DataFrame
    ):
        print(f"\n-- Validating {self.name} --")
        print(f"Shape: {df.shape}")

        missing_vals = df.isna().mean().sort_values(ascending = False)
        print("\nMissing ratio:")
        print(missing_vals.head(10))

        duplicated_vals = df.duplicated().sum()
        print(f"\nDuplicates: {duplicated_vals}")
    
