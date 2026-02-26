from src.features.delivery_dataset_builder import DeliveryDatasetBuilder

def run():
    builder = DeliveryDatasetBuilder()
    df_delivery = builder._build_delivery()
    builder._save(df = df_delivery)

    print(df_delivery.head())

if __name__ == "__main__":
    run()
