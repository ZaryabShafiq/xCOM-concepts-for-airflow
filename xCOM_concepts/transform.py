# Transform function
import pandas as pd
import numpy as np
def transform(**kwargs):
    # Pull the dataframes from XComs
    ti = kwargs['ti']
    cust = ti.xcom_pull(key='cust', task_ids='extract')
    cust_supp = ti.xcom_pull(key='cust_supp', task_ids='extract')
    marketing = ti.xcom_pull(key='marketing', task_ids='extract')
    sales = ti.xcom_pull(key='sales', task_ids='extract')

    curr_date = pd.to_datetime(datetime.date.today())

    # CUSTOMER DATA
    cust['date_of_birth'] = pd.to_datetime(cust['date_of_birth'], format='%d/%m/%Y', errors='coerce')
    cust['age'] = (curr_date - cust['date_of_birth']).dt.days // 365
    cust['created_at'] = pd.to_datetime(cust['created_at'], format='%d/%m/%Y', errors='coerce')
    cust['ltd'] = (curr_date - cust['created_at']).dt.days // 365
    cust['last_updated_at'] = pd.to_datetime(cust['last_updated_at'], format='%d/%m/%Y', errors='coerce')
    cust['time_since_last_update'] = (curr_date - cust['last_updated_at']).dt.days // 365
    cust['full_name'] = cust['first_name'] + " " + cust['last_name']
    cust['full_address'] = cust['address'] + ', ' + cust['city'] + ', ' + cust['state'] + ', ' + cust['country']

    # SALES DATA
    sales_summary = sales.groupby('customer_id').agg(
        total_sales=('total_price', 'sum'),
        avg_sales=('total_price', 'mean')
    ).reset_index()

    cust = pd.merge(cust, sales_summary, on='customer_id', how='left')

    # SALES CHANNEL PERCENTAGE
    sales_count_for_each_channel = sales.groupby(['customer_id', 'sales_channel']).size().reset_index(
        name='channel_count')
    total_sales_per_cust = sales.groupby('customer_id')['sales_id'].count().reset_index(name='total_sales')
    sales_channel_percentage = pd.merge(sales_count_for_each_channel, total_sales_per_cust, on='customer_id')
    sales_channel_percentage['channel_percentage'] = 100 * (
                sales_channel_percentage['channel_count'] / sales_channel_percentage['total_sales'])

    # Add purchase frequency
    sales_count = sales.groupby('customer_id')['sales_id'].count().reset_index(name='total_purchases')
    cust = pd.merge(cust, sales_count, on='customer_id', how='left')
    cust['purchase_frequency'] = cust['total_purchases'] / cust['ltd']

    # PRODUCT POPULARITY
    product_popularity = sales.groupby('product_name')['quantity'].sum().reset_index(name='total_quantity_sold')
    sales = pd.merge(sales, product_popularity, on='product_name', how='left')

    # CUSTOMER SUPPORT DATA
    open_tickets = cust_supp[cust_supp['status'] == 'Open']
    ticket_freq = open_tickets.groupby('customer_id').size().reset_index(name='open_tickets')
    cust_supp = pd.merge(cust_supp, ticket_freq, on='customer_id', how='left')
    cust_supp['open_tickets'] = cust_supp['open_tickets'].fillna(0).astype(int)

    # MARKETING DATA
    marketing['engagement_rate'] = (marketing['clicks'] / marketing['impressions']) * 100
    conversion_rate_by_campaign = marketing.groupby('campaign_type').apply(
        lambda x: 100 * x['conversion'].sum() / x['customer_id'].count()
    ).reset_index(name='conversion_rate')

    avg_eng_score = marketing.groupby('campaign_type')['engagement_score'].mean().reset_index(
        name='avg_engagement_score')
    marketing = pd.merge(marketing, avg_eng_score, on='campaign_type', how='left')

    # CUMULATIVE INTERACTION
    cumul_interaction = marketing.groupby(['customer_id', 'campaign_type']).agg(
        total_clicks=('clicks', 'sum'),
        total_impressions=('impressions', 'sum')
    ).reset_index()
    marketing = pd.merge(marketing, cumul_interaction, on=['customer_id', 'campaign_type'], how='left')

    # Push transformed data to XCom for the next task or final use
    kwargs['ti'].xcom_push(key='cust', value=cust)
    kwargs['ti'].xcom_push(key='cust_supp', value=cust_supp)
    kwargs['ti'].xcom_push(key='marketing', value=marketing)
    kwargs['ti'].xcom_push(key='sales', value=sales)

