import os 
import pandas as pd
from pathlib import Path
from src.data.kaggle_downloader import KaggleDownloader

class KaggleCSVLoader:

    def __init__(
            self,
            dataset_name: str,
            data_dir: str = "data/raw"
    ):
        self.dataset_name = dataset_name

        project_root = Path(__file__).resolve().parents[2]
        self.data_dir = project_root / data_dir
        self.data_dir.mkdir(parents = True, exist_ok = True)

        self.dataset_downloader = KaggleDownloader(
            dataset_name = self.dataset_name,
            download_path = self.data_dir
        )
    
    def download_dataset(self):

        dataset_path = self.data_dir / self.dataset_name

        if not dataset_path.exists():
            self.dataset_downloader.download()
        else:
            print(f"Dataset {self.dataset_name} already exists! Skipping the data downloading step!")
    
    def load_local_csv(
            self,
            file_name: str
    ) -> pd.DataFrame:
        
        """ Loading CSV from local raw data directory """
        file_path = self.data_dir / file_name

        try:
            df = pd.read_csv(file_path)
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"File {file_name} not found in the local raw data directory!")
    
    def save_df(
            self,
            df: pd.DataFrame,
            file_name: str
    ):
        
        """ Saving dataframe into local raw data directory """
        file_path = self.data_dir / file_name
        df.to_csv(
            file_path,
            index = False
        )

        print(f"Dataframe {file_name} saved into local raw data directory..")
    
