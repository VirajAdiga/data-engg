import json
import pandas as pd


order_columns = ["order_id", "order_date", "order_customer_id", "order_status"]
orders = pd.read_csv("data/retail_db/orders/part-00000", names=order_columns)


# Orders whose status is 'COMPLETE' or 'CLOSED'
completed_or_closed_orders = orders.query('order_status == "COMPLETE" or order_status == "CLOSED"')
print(completed_or_closed_orders)
completed_or_closed_orders = orders.query('order_status == ("COMPLETE", "CLOSED")')
print(completed_or_closed_orders)


# Group orders by order_status and get the aggregated count
grouped_orders_count = orders.groupby('order_status')['order_id'].agg(grouped_count='count')
print(grouped_orders_count)


# Group orders by order_month , order_status and get the aggregated count
# Creating order_month column
orders['order_month'] = orders.apply(lambda order: order.order_date[:7], axis=1)
print(orders)
grouped_orders = orders.groupby(['order_month', 'order_status'])['order_id'].agg(grouped_count='count')
print(grouped_orders)


def get_column_names(schemas, data_set_name, sorting_key="column_position"):
    column_details = schemas[data_set_name]
    columns = sorted(column_details, key=lambda column: column[sorting_key])
    return [col['column_name'] for col in columns]


# Us dynamic column mapping using schemas
schemas = json.load(open("data/retail_db/schemas.json"))
customers_columns = get_column_names(schemas, 'customers')
customers = pd.read_csv("data/retail_db/customers/part-00000", names=customers_columns)
print(customers)


# Joining customers and orders dataframes
customers_orders = pd.merge(orders, customers, left_on='order_customer_id', right_on='customer_id', how='inner')
print(customers_orders)


# Grouping joined dataframe on number of customer orders and filter based on number of orders
grouped_customer_orders = customers_orders.groupby('order_customer_id')['order_customer_id'].agg(customer_order_count='count').query('customer_order_count >= 10')
print(grouped_customer_orders)


# Sorting on more than one column
sorted_grouped_customer_orders = grouped_customer_orders.sort_values(['customer_order_count', 'order_customer_id'], ascending=[False, True])
print(sorted_grouped_customer_orders)


# Writing dataframe to a file
sorted_grouped_customer_orders.to_json("pandas_sample.json", orient='records', lines=True)
