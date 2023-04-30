import pandas as pd

class Transform:            
    def get_average_earning_per_day_of_week(self, orders, order_items):        
        full_df = orders.merge(order_items, left_on="order_id", right_on="order_item_order_id")
        full_df = full_df[
            ["order_date","order_status","order_item_product_id","order_item_subtotal"]
        ]
        
        full_df = full_df[full_df.order_status == 'COMPLETE']
        
        
        total_per_day = full_df.groupby("order_date")["order_item_subtotal"].sum().reset_index()
        
        total_per_day['day_of_week'] = pd.to_datetime(total_per_day.order_date).dt.dayofweek
        
        avg_per_weekday = total_per_day.groupby("day_of_week")["order_item_subtotal"].mean().reset_index()
        return avg_per_weekday
    
    def get_most_sold_category_per_department(self, orders, order_items, products, categories, departments):
        orders_merged_items = orders.merge(order_items, left_on="order_id", right_on="order_item_order_id")
        orders_merged_items = orders_merged_items[orders_merged_items.order_status == 'COMPLETE'][["order_id", "order_item_product_id", "order_item_quantity"]]
        
        departmentsxcategories = categories.merge(departments, left_on="category_department_id", right_on="department_id")
        productsxdepartmentsxcategories = departmentsxcategories.merge(products, left_on="category_id", right_on="product_category_id")
        
        productsxdepartmentsxcategories = productsxdepartmentsxcategories[["department_name", "category_name", "product_id"]]
        
        orders_per_department = orders_merged_items.merge(productsxdepartmentsxcategories, left_on="order_item_product_id", right_on="product_id")
        
        orders_per_category_per_department = orders_per_department.groupby(["department_name", "category_name"])["order_item_quantity"].sum().reset_index()
        
        best_category_per_department = orders_per_category_per_department.groupby('department_name', as_index=False).apply(lambda x: x.nlargest(1, 'order_item_quantity'))
        
        return best_category_per_department[["department_name", "category_name"]]
    
    def get_pending_orders_per_customer(self, orders, customers):
        pending_orders = orders[orders.order_status == 'PENDING']
        pending_ordersxcustomers = pending_orders.merge(customers, left_on="order_customer_id", right_on="customer_id")
        
        pending_ordersxcustomers['full_name'] = pending_ordersxcustomers.customer_fname + ' ' + pending_ordersxcustomers.customer_lname
        
        pending_ordersxcustomers = pending_ordersxcustomers.groupby('full_name')['order_id'].count().reset_index()
        
        return pending_ordersxcustomers
    
    def get_customer_favorite_department(self, customers, orders, order_items, products, categories, departments):
        customers['full_name'] = customers.customer_fname + ' ' + customers.customer_lname
        customers = customers[['customer_id', 'full_name']]
        
        orders = orders[orders.order_status == 'COMPLETE']
        
        orders = orders.merge(customers, left_on="order_customer_id", right_on="customer_id")
        orders_merged_items = orders.merge(order_items, left_on="order_id", right_on="order_item_order_id")
        orders_merged_items = orders_merged_items[["order_id", "full_name", "order_item_product_id", "order_item_quantity"]]
        
        departmentsxcategories = categories.merge(departments, left_on="category_department_id", right_on="department_id")
        productsxdepartmentsxcategories = departmentsxcategories.merge(products, left_on="category_id", right_on="product_category_id")
        
        productsxdepartmentsxcategories = productsxdepartmentsxcategories[["department_name", "category_name", "product_id"]]
        
        orders_per_department = orders_merged_items.merge(productsxdepartmentsxcategories, left_on="order_item_product_id", right_on="product_id")
        
        orders_per_customer_per_department = orders_per_department.groupby(["full_name", "department_name"])["order_item_quantity"].sum().reset_index()
        
        favorite_department_per_customer = orders_per_customer_per_department.groupby("full_name", as_index=False).apply(lambda x: x.nlargest(1, 'order_item_quantity'))
        
        return favorite_department_per_customer[["full_name", "department_name"]]
    
    def get_full_df(self, df_dict):
        full_product = df_dict['products'].merge(df_dict['categories'], left_on='product_category_id', right_on='category_id')
        full_product = full_product.merge(df_dict['departments'], left_on='category_department_id', right_on='department_id')
        
        full_df = df_dict['orders'].merge(df_dict['customers'], left_on='order_customer_id', right_on='customer_id')
        full_df = full_df.merge(df_dict['order_items'], left_on='order_id', right_on='order_item_order_id')
        full_df = full_df.merge(full_product, left_on='order_item_product_id', right_on='product_id')
        
        full_df = full_df[[
            'order_id', 'order_date', 'order_status',
             'customer_fname', 'customer_lname', 'customer_street', 'customer_city','customer_state', 'customer_zipcode',
             'department_name', 'category_name', 'product_name',
             'order_item_quantity','order_item_product_price', 'order_item_subtotal'
        ]]
        
        return full_df
        
        