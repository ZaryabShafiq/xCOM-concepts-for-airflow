import pandas as pd
import numpy as np
def load(**kwargs):
    ti = kwargs['ti']
    cust = ti.xcom_pull(key='cust', task_ids='transform')
    cust_supp = ti.xcom_pull(key='cust_supp', task_ids='transform')
    marketing = ti.xcom_pull(key='marketing', task_ids='transform')
    sales = ti.xcom_pull(key='sales', task_ids='transform')

    # Here, you would insert the processed data into a database or write it to a file.
    print("Data loaded successfully.")
