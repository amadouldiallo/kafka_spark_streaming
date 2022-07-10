import random

from faker import Faker
import pandas as pd

# Facker
fake = Faker()
Faker.seed(0)


def make_faker_customers(num):
    """

    :param num: number of customers to generate
    :return: customers
    """
    networks = ["Facebook", 'Twitter', 'Instagram','Yahoo','Telegram', 'Whatsapp','Affiliate']  # list pf sources
    faker_customers = []
    for x in range(num):
        position = fake.location_on_land()  # fake position
        zipcode = fake.zipcode()  # fake zipcode
        customer = {
            'customer_id': x+1,
            'name': fake.name(),
            'phone_number': fake.phone_number().replace(',', '_'),
            'address': fake.address().replace(',', '_').replace("\n", ' '),
            'email': fake.email(),
            'source': fake.random.choice(networks),
            'state': position[3],
            'longitude': position[1],
            'latitude': position[0],
            'zip': zipcode,
        }
        faker_customers.append(customer)

    random.shuffle(faker_customers)
    return faker_customers


def make_faker_products(num):
    """
    :param num: number of products
    :return: products
    """
    faker_products = [
        {
            'product_id': x+1,
            'name': 'Product '+ str(x+1),
            'price': fake.random.randint(1,1000),
            'barcode': fake.ean()
        } for x in range(num)
    ]
    random.shuffle(faker_products)
    return faker_products


def make_faker_orders(num):
    """
    :param num:  number of order to generate
    :return: list of ordres
    """
    customers = pd.read_csv('data/customers.csv')
    products = pd.read_csv('./data/products.csv')
    faker_orders = []
    for x in range(num):
        dt = details(fake.random.randint(1, len(products)))  # get details
        order = {
            'order_id': x+1,  # order
            'created_at': fake.date_time_between(start_date='-25d', end_date='now'),  # last 25 days
            'discount': dt[5],
            'product_id': dt[0],
            'quantity': dt[1],
            'subtotal': dt[2],
            'tax': dt[3],
            'total': dt[4],
            'customer_id': fake.random.randint(1, len(customers))
        }
        faker_orders.append(order)
        random.shuffle(faker_orders)
    return faker_orders


def details(product_id):
    """

    :param product_id: id of product
    :return: details of this product
    """
    products = pd.read_csv('./data/products.csv')
    quantity = fake.random.randint(1, 50)
    discount = fake.random.choice([0, 10, 15])
    price = products.loc[product_id-1, 'price']
    subtotal = round(quantity*price,2)
    tax = round(round(random.uniform(0,0.2),2) * subtotal, 2)
    return [product_id, quantity, subtotal, tax, round((subtotal+tax)*(1-discount/100),2), discount]


if __name__ == '__main__':
    # get 1000 customers
    customers_df = pd.DataFrame(make_faker_customers(num=1000))
    customers_df.to_csv('./data/customers.csv', index=False)
    # get 500 products
    products_df = pd.DataFrame(make_faker_products(num=500))
    products_df.to_csv('./data/products.csv', index=False)
    # get 30.000 orders
    orders_df = pd.DataFrame(make_faker_orders(num=30000))
    orders_df.to_csv('./data/orders.csv', index=False)




