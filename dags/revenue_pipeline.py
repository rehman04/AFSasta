from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime 
from datetime import timedelta
import pandas as pd
# Define the ETL functions
def transform_data():
    # Load the data
    commission_df = pd.read_csv('/opt/airflow/dags/de_masked_data - de_masked_data.csv')
    
    orders_df = pd.read_csv('/opt/airflow/dags/de_test_rev_rules - de_test_rev_rules.csv')
    merged_df = commission_df.merge(orders_df,left_on='airline_id', right_on='operating_airline', how='left')
    
    # Apply transformations
    merged_df['gross_commission'] = merged_df['base_fare'] * merged_df['commission_percentage']
    merged_df['bf_net_commission'] = merged_df['base_fare'] - merged_df['gross_commission']
    merged_df['gross_income'] = merged_df['bf_net_commission'] * merged_df['incentive_percentage']
    merged_df['wht_commission'] = merged_df['gross_commission'] * merged_df['tax_percentage']
    merged_df['wht_income'] = merged_df['gross_income'] * merged_df['tax_percentage']
    merged_df['net_commission'] = merged_df['gross_commission'] - merged_df['wht_commission']
    merged_df['net_income'] = merged_df['gross_income'] - merged_df['wht_income']
    merged_df['revenue'] = merged_df['net_commission'] + merged_df['net_income']

    
    # Save the transformed data
    merged_df.to_csv('/opt/airflow/dags/transformed_data.csv', index=False)
    
def print_revenue():
    # Load the transformed data
    transformed_df = pd.read_csv('/opt/airflow/dags/transformed_data.csv')
    
    # Print the revenue for each order
    for _, row in transformed_df.iterrows():
        order_id = row['orders_id']
        revenue = row['revenue']
        print(f"Order ID: {order_id}, Revenue: {revenue}")

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('revenue_pipeline', default_args=default_args, schedule_interval=None)

# Define the tasks
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

print_task = PythonOperator(
    task_id='print_revenue',
    python_callable=print_revenue,
    dag=dag
)

# Set up task dependencies
transform_task >> print_task
