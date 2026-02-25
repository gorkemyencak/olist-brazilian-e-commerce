from src.data.kaggle_loader import KaggleCSVLoader
from src.data.data_validator import DataValidator
from src.data.data_preprocessor import DataPreprocessor
from src.data.processed_saver import ProcessedSaver

def run():

    loader = KaggleCSVLoader(dataset_key = "olist")
    loader.download_dataset()

    raw_datasets = loader.load_all()

    # initialize preprocessor and saver class instances
    pre = DataPreprocessor()
    saver = ProcessedSaver(key = 'olist')

    ### customers
    customers = raw_datasets['olist_customers_dataset.csv']
    val = DataValidator(name = 'customers')
    val.validate_basic(df = customers)

    saver.save(
        df = customers,
        name = 'processed_customers_dataset.csv'
    )

    ### geolocation
    geolocation = raw_datasets['olist_geolocation_dataset.csv']
    val = DataValidator(name = 'geolocations')
    val.validate_basic(df = geolocation)

    geolocation_clean = pre.preprocess_geolocation(df = geolocation)

    saver.save(
        df = geolocation_clean,
        name = 'processed_geolocation_dataset.csv'
    )

    ### items
    items = raw_datasets['olist_order_items_dataset.csv']
    val = DataValidator(name = 'items')
    val.validate_basic(df = items)

    items_clean = pre.preprocess_items(df = items)

    saver.save(
        df = items_clean,
        name = 'processed_items_dataset.csv'
    )

    ### payments
    payments = raw_datasets['olist_order_payments_dataset.csv']
    val = DataValidator(name = 'payments')
    val.validate_basic(df = payments)

    saver.save(
        df = payments,
        name = 'processed_payments_dataset.csv'
    )

    ### reviews
    reviews = raw_datasets['olist_order_reviews_dataset.csv']
    val = DataValidator(name = 'reviews')
    val.validate_basic(df = reviews)

    reviews_clean = pre.preprocess_reviews(df = reviews)

    saver.save(
        df = reviews_clean,
        name = 'processed_reviews_dataset.csv'
    )

    ### orders
    orders = raw_datasets['olist_orders_dataset.csv']
    val = DataValidator(name = 'orders')
    val.validate_basic(df = orders)

    orders_clean = pre.preprocess_orders(df = orders)

    saver.save(
        df = orders_clean,
        name = 'processed_orders_dataset.csv'
    )     

    ### products
    products = raw_datasets['olist_products_dataset.csv']
    val = DataValidator(name = 'products')
    val.validate_basic(df = products)

    saver.save(
        df = products,
        name = 'processed_products_dataset.csv'
    )

    ### sellers
    sellers = raw_datasets['olist_sellers_dataset.csv']
    val = DataValidator(name = 'sellers')
    val.validate_basic(df = sellers)

    saver.save(
        df = sellers,
        name = 'processed_sellers_dataset.csv'
    )

    ### product category
    prod_cat = raw_datasets['product_category_name_translation.csv']
    val = DataValidator(name = 'product category')
    val.validate_basic(df = prod_cat)

    saver.save(
        df = prod_cat,
        name = 'processed_product_category_dataset.csv'
    )

if __name__ == "__main__":
    run()

