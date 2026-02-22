import os
from src.data.kaggle_loader import KaggleCSVLoader

def main():
    loader = KaggleCSVLoader(dataset_key = "olist")
    loader.download_dataset()

    data = loader.load_all()

    orders = data["olist_orders_dataset.csv"]
    print(orders.shape)

if __name__ == '__main__':
    main()
