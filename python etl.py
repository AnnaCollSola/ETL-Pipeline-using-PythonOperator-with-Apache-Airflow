
'''
Apache Airflow DAG that collects road traffic data from multiple highway toll plazas. 
Each highway is operated by a different toll operator with a different IT setup that uses different file formats.
The data is collected and consolidated into a single file. 

DAG's tasks include:
Extract data from a csv file
Extract data from a tsv file
Extract data from a fixed-width file
Transform the data
Load the transformed data into the staging area
'''

# Import the libraries
from datetime import timedelta, datetime
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator


# Define the path for the input and output files
input_file = '/opt/airflow/dags/python-etl/tolldata.tgz'
extracted_file = '/opt/airflow/dags/python-etl/extracted-data'
transformed_file = 'transformed.txt'
output_file = 'data_for_analytics.csv'


# Define DAG arguments
default_args = {
    'owner': 'Anna',
    'start_date': datetime(2026,9,18),
    'email': ['anna@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data_python',
    default_args=default_args,
    description='ETL pipeline for toll data using PythonOperator',
    schedule=timedelta(days=1),
)

#--------------------------------------------------------------------------------

#FUNCTIONS

import tarfile
import os

#Function to untar the dataset
def untar_dataset(input_file, extracted_file):
    os.makedirs(extracted_file, exist_ok=True)

    with tarfile.open(input_file, 'r:gz') as tar_ref:
        tar_ref.extractall(extracted_file)


#Function to extract the fields Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type from the vehicle-data.csv 
#file and save them into a file named csv_data.csv
def extract_data_from_csv():
    input_file = os.path.join(extracted_file, 'vehicle-data.csv')
    output_file = os.path.join(extracted_file, 'csv_data.csv')

    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:

        # Write column headers
        outfile.write('Rowid,Timestamp,Anonymized Vehicle number,Vehicle type\n')

        for line in infile:
            fields = line.strip().split(',')

            if len(fields) >= 4:
                rowid = fields[0]
                timestamp = fields[1]
                vehicle_number = fields[2]
                vehicle_type = fields[3]

                outfile.write(
                    f"{rowid},{timestamp},{vehicle_number},{vehicle_type}\n"
                )


#Function named extract_data_from_tsv to extract the fields Number of axles, Tollplaza id, and Tollplaza code from the 
#tollplaza-data.tsv file and save it into a file named tsv_data.csv
#There are 7 columns separated by (\t) and we want the 5th (Number of axles), 6th (Tollplaza id) and 7th (Tollplaza code).

import csv

def extract_data_from_tsv(input_file, output_file):
    with open(input_file, 'r', newline='') as tsv_file:
        reader = csv.reader(tsv_file, delimiter='\t')

        with open(output_file, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)

            # Write the column names
            writer.writerow([
                'Number of axles',
                'Tollplaza id',
                'Tollplaza code'
            ])

            # Extract the required columns
            for row in reader:
                writer.writerow([row[4], row[5], row[6]])

#Function named extract_data_from_fixed_widthto extract the fields Type of Payment code and Vehicle Code 
#from the fixed width file payment-data.txt and save it into a file named fixed_width_data.csv

def extract_data_from_fixed_width(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        # Write the column names
        outfile.write('Type of Payment code,Vehicle Code\n')

        for line in infile:
            type_of_payment_code = line[58:61].strip()
            vehicle_code = line[62:67].strip()

            outfile.write(f"{type_of_payment_code},{vehicle_code}\n")


#Create a function named consolidate_data to create a single csv file named extracted_data.csv by combining 
#data from the following files csv_data.csv, tsv_data.csv and fixed_width_data.csv

def consolidate_data(csv_file, tsv_file, fixed_width_file, output_file):

    with open(csv_file, 'r', newline='') as f:
        csv_data = list(csv.reader(f))

    with open(tsv_file, 'r', newline='') as f:
        tsv_data = list(csv.reader(f))

    with open(fixed_width_file, 'r', newline='') as f:
        fixed_data = list(csv.reader(f))

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)

        writer.writerow(
            csv_data[0] + tsv_data[0] + fixed_data[0]
        )

        for i in range(1, len(csv_data)):
            writer.writerow(
                csv_data[i] + tsv_data[i] + fixed_data[i]
            )


#Create a function named transform_data to transform the vehicle_type field in extracted_data.csv into capital letters 
#and save it into a file named transformed_data.csv in the staging directory.
def transform_data(input_file, output_file):

    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(input_file, 'r', newline='') as infile:
        reader = csv.DictReader(infile)

        fieldnames = reader.fieldnames

        with open(output_file, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                row['Vehicle type'] = row['Vehicle type'].upper()
                writer.writerow(row)


#-------------------------------------------------------------------------


#TASKS

# Define the task named untar_dataset to call the untar_dataset function
untar_dataset = PythonOperator(
    task_id='untar_dataset',
    python_callable=untar_dataset,
    op_kwargs={
        'input_file': input_file,
        'extracted_file': extracted_file,
    },
    dag=dag,
)

#Define the task extract_data_from_csv to call the extract_data_from_csv function
extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

#Define the task extract_data_from_tsv to call the extract_data_from_tsv function
extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    op_kwargs={
        'input_file': os.path.join(extracted_file, 'tollplaza-data.tsv'),
        'output_file': os.path.join(extracted_file, 'tsv_data.csv'),
    },
    dag=dag,
)

#Define the task extract_data_from_fixed_width to call the extract_data_from_fixed_width function
extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    op_kwargs={
        'input_file': os.path.join(extracted_file, 'payment-data.txt'),
        'output_file': os.path.join(extracted_file, 'fixed_width_data.csv'),
    },
    dag=dag,
)

#Define the task consolidate_data to call the consolidate_data function
consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data,
    op_kwargs={
        'csv_file': os.path.join(extracted_file, 'csv_data.csv'),
        'tsv_file': os.path.join(extracted_file, 'tsv_data.csv'),
        'fixed_width_file': os.path.join(extracted_file, 'fixed_width_data.csv'),
        'output_file': os.path.join(extracted_file, 'extracted_data.csv'),
    },
    dag=dag,
)


transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={
        'input_file': os.path.join(
            extracted_file, 'extracted_data.csv'
        ),
        'output_file': os.path.join(
            extracted_file, 'transformed_data.csv'
        ),
    },
    dag=dag,
)


#----------------------------------------------------------------------------------

# Task pipeline
untar_dataset>>extract_data_from_csv>>extract_data_from_tsv>>extract_data_from_fixed_width>>consolidate_data>>transform_data

