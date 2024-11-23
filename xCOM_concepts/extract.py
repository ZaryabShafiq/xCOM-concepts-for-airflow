import pandas as pd
import numpy as np
# Extract function
def extract(**kwargs):
    path = r'your\file\path\here'
    cust = pd.read_csv(path)

    path1 = r'your\file\path\here'
    cust_supp = pd.read_csv(path1)

    path2 = r'your\file\path\here'
    marketing = pd.read_csv(path2)

    path3 = r'your\file\path\here'
    sales = pd.read_csv(path3)

    # Push the dataframes to XCom for the next task
    kwargs['ti'].xcom_push(key='cust', value=cust)
    kwargs['ti'].xcom_push(key='cust_supp', value=cust_supp)
    kwargs['ti'].xcom_push(key='marketing', value=marketing)
    kwargs['ti'].xcom_push(key='sales', value=sales)

