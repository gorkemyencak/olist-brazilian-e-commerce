from pathlib import Path

class ProcessedSaver:

    def __init__(
            self,
            key: str
    ):
        project_root = Path(__file__).resolve().parents[2]
        self.dir = project_root / "data" / "processed" / "key"
        self.dir.mkdir(parents = True, exist_ok = True)

    def save(
            self,
            df,
            name
    ):
        df.to_csv(
            self.dir / name,
            index = True
        )

        print(f"Saved processed: {name}")