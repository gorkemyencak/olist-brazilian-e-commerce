import pandas as pd

class DataPreprocessor:

    def preprocess_geolocation(
            self,
            df: pd.DataFrame
    ):
        df = df.copy()

        # aggregating duplicates on identical geo zip code
        df = (
            df.groupby('geolocation_zip_code_prefix')[['geolocation_lat', 'geolocation_lng']]
            .mean()
            .reset_index()
        )

        return df
    
    
    def preprocess_items(
            self,
            df: pd.DataFrame
    ):
        df = df.copy()

        # parsing timestamps
        time_columns = [
            'shipping_limit_date'
        ]

        for col in time_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col],
                    errors='coerce'
                )
        
        # item count
        df['item_count'] = 1
        
        return df
    
    
    def preprocess_reviews(
            self,
            df: pd.DataFrame
    ):
        df = df.copy()

        # parsing timestamps
        time_columns = [
            'review_creation_date',
            'review_answer_timestamp'
        ]

        for col in time_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col],
                    errors='coerce'
                )
        
        return df 


    def preprocess_orders(
            self,
            df: pd.DataFrame
    ):
        df = df.copy()

        # parsing timestamps
        time_columns = [
            'order_purchase_timestamp',
            'order_approved_at',
            'order_delivered_carrier_date',
            'order_delivered_customer_date',
            'order_estimated_delivery_date'
        ]

        for col in time_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(
                    df[col], 
                    errors='coerce'
                )
        
        # removing cancelled orders
        df = df.loc[
            df['order_status'] != 'canceled'
        ].reset_index(drop=True)

        # removing not approved orders
        df = df.loc[
            df['order_approved_at'].notna()
        ].reset_index(drop=True)

        return df