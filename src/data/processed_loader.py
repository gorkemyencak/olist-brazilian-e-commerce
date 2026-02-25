from pathlib import Path
import pandas as pd

class ProcessedLoader:

    def __init__(
            self,
            key: str
    ):
        
        project_root = Path(__file__).resolve().parents[2]
        self.dir = project_root / "data" / "processed" / key
        self.dir.mkdir(parents = True, exist_ok = True)


    def load(
            self,
            name: str
    ):
        return pd.read_csv(
            self.dir / name, 
            parse_dates = True
        )
    

    def load_all(self):
        data = {}
        for f in self.dir.glob(".csv"):
            data[f.name] = pd.read_csv(
                f,
                parse_dates = True
            )
        return data


