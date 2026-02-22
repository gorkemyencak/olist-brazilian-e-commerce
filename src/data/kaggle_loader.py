import os 
import pandas as pd
from pathlib import Path
from src.data.kaggle_downloader import KaggleDownloader
from src.config.datasets_config import DATASETS

class KaggleCSVLoader:

    def __init__(
            self,
            dataset_key: str,
            data_dir: str = "data/raw"
    ):
        self.dataset_key = dataset_key
        self.dataset_config = DATASETS[dataset_key]

        project_root = Path(__file__).resolve().parents[2]
        self.base_data_dir = project_root / data_dir
        self.base_data_dir.mkdir(parents = True, exist_ok = True)

        self.dataset_dir = self.base_data_dir / self.dataset_key
        self.dataset_dir.mkdir(parents = True, exist_ok = True)

        self.dataset_downloader = KaggleDownloader(
            kaggle_id = self.dataset_config['kaggle_id'],
            download_path = self.dataset_dir
        )

    def download_dataset(self):
        file = self.dataset_config['files'][0]
        if not (self.dataset_dir / file).exists():
            self.dataset_downloader.download()
        else:
            print(f"{self.dataset_key} already downloaded..")
    
    def load_local_csv(
            self,
            file_name: str
    ) -> pd.DataFrame:
        
        """ Loading CSV from local raw data directory """
        file_path = self.dataset_dir / file_name

        if not file_path.exists():
            raise FileNotFoundError(f"{file_name} not found in {self.dataset_key}!")
        
        return pd.read_csv(file_path)

    def load_all(self) -> dict:
        data = {}

        for file in self.dataset_config['files']:
            data[file] = self.load_local_csv(file_name = file)

        return data

    def save_df(
            self,
            df: pd.DataFrame,
            file_name: str
    ):
        
        """ Saving dataframe into local raw data directory """
        file_path = self.dataset_dir / file_name
        df.to_csv(
            file_path,
            index = False
        )

        print(f"Dataframe {file_name} saved into local raw data directory..")
