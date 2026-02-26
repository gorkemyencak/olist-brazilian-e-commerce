

import pandas as pd
from pathlib import Path
from src.data.processed_loader import ProcessedLoader

class DeliveryDatasetBuilder:
    """ DeliveryDatasetBuilder class will convert relational e-commerce data into OR problem instance """

    def __init__(
            self,
            dataset_key: str = 'olist'
    ):
        self.dataset_key = dataset_key
        
        project_root = Path(__file__).resolve().parents[2]
        self.features_dir = project_root / 'data' / 'features' / self.dataset_key
        self.features_dir.mkdir(parents=True, exist_ok=True)

        self.loader = ProcessedLoader(self.dataset_key)

    def _load(self):
        # loading processed datasets
        data = self.loader.load_all()

        self.orders = data['processed_orders_dataset.csv']
        self.items = data['processed_items_dataset.csv']
        self.customers = data['processed_customers_dataset.csv']
        self.geolocation = data['processed_geolocation_dataset.csv']
        self.sellers = data['processed_sellers_dataset.csv']
    
    def _build_demand(self):
        # generating customer demand
        demand = (
            self.items
            .groupby('order_id')
            .size()
            .rename('demand')
            .reset_index()
        )

        return demand
    
    def _build_customer_geo(self, df: pd.DataFrame):
        # attaching customer geolocation data
        cust_data = self.customers[[
            'customer_id',
            'customer_zip_code_prefix'
        ]]

        df = df.merge(
            cust_data,
            on = 'customer_id',
            how = 'left'
        )

        df = df.merge(
            self.geolocation,
            left_on = 'customer_zip_code_prefix',
            right_on = 'geolocation_zip_code_prefix',
            how = 'left'
        )

        df = df.rename(
            columns= {
                'geolocation_lat': 'delivery_lat',
                'geolocation_lng': 'delivery_lng'
            }
        )

        return df
    
    def _build_seller_geo(self, df: pd.DataFrame):
        # attaching seller geolocation data
        seller_data = self.sellers[[
            'seller_id',
            'seller_zip_code_prefix'
        ]]

        seller_per_order = (
            self.items
            .groupby('order_id')['seller_id']
            .first()
            .reset_index()
        )

        df = df.merge(
            seller_per_order,
            on = 'order_id',
            how = 'left'
        )

        df = df.merge(
            seller_data,
            on = 'seller_id',
            how = 'left'
        )

        df = df.merge(
            self.geolocation,
            left_on = 'seller_zip_code_prefix',
            right_on = 'geolocation_zip_code_prefix',
            how = 'left'
        )

        df = df.rename(
            columns = {
                'geolocation_lat': 'pickup_lat',
                'geolocation_lng': 'pickup_lng'
            }
        )

        return df
        

    def _build_delivery(self):

        self._load()

        demand = self._build_demand()

        df = self.orders[[
            'order_id',
            'customer_id',
            'order_approved_at',
            'order_estimated_delivery_date'
        ]]

        df = df.merge(
            demand,
            on = 'order_id',
            how = 'left'
        )

        df = self._build_customer_geo(df)
        df = self._build_seller_geo(df)

        # DP-ready time-window
        df['ready_time'] = df['order_approved_at']
        df['due_date'] = df['order_estimated_delivery_date']

        # service time assumption (in minutes)
        df['service_time_min'] = 30

        # job id
        df['job_id'] = df['order_id']

        # OR cols
        cols = [
            'job_id',
            'order_id',
            'seller_id',
            'customer_id',
            'pickup_lat',
            'pickup_lng',
            'delivery_lat',
            'delivery_lng',
            'ready_time',
            'due_date',
            'service_time_min',
            'demand'
        ]

        df = df[cols].copy()

        return df
    

    def _save(self, df: pd.DataFrame):
        path = self.features_dir / "delivery_jobs_dataset.csv"
        df.to_csv(path, index = False)
        print(f"Saved delivery dataset: {path}")




