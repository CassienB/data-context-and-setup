import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
#orders[['order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date','order_approved_at']]\
#= orders[['order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date','order_approved_at']].apply(date, axis=1)
        if is_delivered == True:
            order_status_ = 'delivered'
        orders = self.data['orders']
        df = orders[orders['order_status'] == order_status_]
        df['order_delivered_carrier_date'] = pd.to_datetime(df.order_delivered_carrier_date)
        df['order_delivered_customer_date'] =pd.to_datetime(df.order_delivered_customer_date)
        df['order_estimated_delivery_date'] = pd.to_datetime(df.order_estimated_delivery_date)
        df['order_purchase_timestamp'] = pd.to_datetime(df.order_purchase_timestamp)
        df['wait_time'] = df['order_delivered_customer_date'] - df['order_purchase_timestamp']
        df['expected_wait_time'] = df['order_estimated_delivery_date'] - df['order_purchase_timestamp']
        df['delay_vs_expected'] = df['wait_time'] - df['expected_wait_time']
        dfa = df[['order_id','wait_time','expected_wait_time','delay_vs_expected','order_status']]
        return dfa

    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        reviews = self.data['order_reviews']
        def many_star(x,y):
            return y[x]
        five_star = {5:1,4:0,3:0,2:0,1:0}
        one_star = {5:0,4:0,3:0,2:0,1:1}

        reviews['dim_is_five_star'] = reviews['review_score'].apply(many_star,args=(five_star,))
        reviews['dim_is_one_star'] = reviews['review_score'].apply(many_star,args=(one_star,))
        review_count = reviews[['order_id','dim_is_five_star', 'dim_is_one_star','review_score']]
        return review_count

    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        products = self.data['order_items'].copy()
        prod = products[['order_id', 'product_id']]
        number_of_products = prod.groupby('order_id').count()[['product_id']]
        number_of_products.rename(columns = {'product_id':'number_of_products'}, inplace=True)
        number_of_products.reset_index(inplace=True)
        return number_of_products

    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        items = self.data['order_items'].copy()
        item = items[['order_id','seller_id']]

        number_of_sellers = item.groupby('order_id').count()
        number_of_sellers.rename(columns={'seller_id':'number_of_sellers'}, inplace = True)
        number_of_sellers.reset_index(inplace=True)
        return number_of_sellers

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        items = self.data['order_items'].copy()
        price = items[['order_id','price','freight_value']]
        price_ = price.groupby('order_id').sum()[['price','freight_value']]
        price_.reset_index(inplace=True)
        return price_

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        data = self.data
        orders = data['orders']
        order_items = data['order_items']
        sellers = data['sellers']
        customers = data['customers']

        # Since one zip code can map to multiple (lat, lng), take the first one
        geo = data['geolocation']
        geo = geo.groupby('geolocation_zip_code_prefix',
                          as_index=False).first()

        # Merge geo_location for sellers
        sellers_mask_columns = [
            'seller_id', 'seller_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'
        ]

        sellers_geo = sellers.merge(
            geo,
            how='left',
            left_on='seller_zip_code_prefix',
            right_on='geolocation_zip_code_prefix')[sellers_mask_columns]

        # Merge geo_location for customers
        customers_mask_columns = ['customer_id', 'customer_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']

        customers_geo = customers.merge(
            geo,
            how='left',
            left_on='customer_zip_code_prefix',
            right_on='geolocation_zip_code_prefix')[customers_mask_columns]

        # Match customers with sellers in one table
        customers_sellers = customers.merge(orders, on='customer_id')\
            .merge(order_items, on='order_id')\
            .merge(sellers, on='seller_id')\
            [['order_id', 'customer_id','customer_zip_code_prefix', 'seller_id', 'seller_zip_code_prefix']]

        # Add the geoloc
        matching_geo = customers_sellers.merge(sellers_geo,
                                            on='seller_id')\
            .merge(customers_geo,
                   on='customer_id',
                   suffixes=('_seller',
                             '_customer'))
        # Remove na()
        matching_geo = matching_geo.dropna()

        matching_geo.loc[:, 'distance_seller_customer'] =\
            matching_geo.apply(lambda row:
                               haversine_distance(row['geolocation_lng_seller'],
                                                  row['geolocation_lat_seller'],
                                                  row['geolocation_lng_customer'],
                                                  row['geolocation_lat_customer']),
                               axis=1)
        # Since an order can have multiple sellers,
        # return the average of the distance per order
        order_distance =\
            matching_geo.groupby('order_id',
                                 as_index=False).agg({'distance_seller_customer':
                                                      'mean'})

        return order_distance

    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        df1 = self.get_wait_time(is_delivered=True)
        df2 = self.get_review_score()
        df3 = self.get_number_products()
        df4 = self.get_number_sellers()
        df5 = self.get_price_and_freight()

        # Hint: make sure to re-use your instance methods defined above
        dfa = df1.merge(df2,on='order_id',how='inner').merge(df3,on='order_id',how='inner').merge(df4,on='order_id',how='inner').merge(df5,on='order_id',how='inner')
        dfa = dfa.dropna()

        if with_distance_seller_customer==True:
            df6 = self.get_distance_seller_customer()
            dfb = dfa.merge(df6, on='order_id',how='outer')
            dfb.dropna(inplace=True)
            return dfb

        return dfa
