from pathlib import Path
import pandas as pd

class ProcessedLoader:

    def __init__(
            self,
            key: str
    ):
        
        project_root = Path(__file__).resolve().parents[2]
        self.dir = project_root / "data" / "processed" / key

        self.date_columns = {
            "processed_items_dataset.csv": [
                'shipping_limit_date'
            ],
            "processed_reviews_dataset.csv": [
                'review_creation_date',
                'review_answer_timestamp'
            ],
            "processed_orders_dataset.csv": [
                'order_purchase_timestamp',
                'order_approved_at',
                'order_delivered_carrier_date',
                'order_delivered_customer_date',
                'order_estimated_delivery_date'
            ]
        }


    def load(
            self,
            name: str
    ):
        parser = self.date_columns.get(name, None)

        return pd.read_csv(
            self.dir / name, 
            parse_dates = parser
        )
    

    def load_all(self):
        data = {}

        for f in self.dir.glob("*.csv"):
            parser = self.date_columns.get(f.name, None)

            data[f.name] = pd.read_csv(
                f,
                parse_dates = parser
            )
        return data


