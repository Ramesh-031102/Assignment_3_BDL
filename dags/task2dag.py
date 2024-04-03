# Import necessary libraries and modules
import os
import csv
from airflow import DAG
import requests
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import numpy as np
import apache_beam as beam
import re
import warnings

# Suppress warnings for cleaner output
warnings.filterwarnings("ignore")

# Import geospatial libraries for visualization
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt

# Import additional modules for data processing and visualization
import time
import json
import zipfile

# Define index list and fields for data processing
indexlist = ['LATITUDE', 'LONGITUDE', 'DATE']
fields = [
    'HourlyDewPointTemperature',
    'HourlyDryBulbTemperature',
    'HourlyPrecipitation',
    'HourlyPresentWeatherType',
    'HourlyPressureChange',
    'HourlyPressureTendency',
    'HourlyRelativeHumidity',
    'HourlySkyConditions',
    'HourlySeaLevelPressure',
    'HourlyStationPressure',
    'HourlyVisibility',
    'HourlyWetBulbTemperature',
    'HourlyWindGustSpeed',
    'HourlyWindSpeed'
]

heatmap_fields = ['HourlyDewPointTemperature', 'HourlyDryBulbTemperature', 'HourlyPrecipitation']

# Function definitions

# Function to check if data.zip file is present in the output directory after a delay
def filesensor():
    time.sleep(5)
    files = os.listdir('output')
    if ('data.zip' in files):
        return True
    return False

# Function to unzip files from a specified path to an extraction path
def unzip_files(file_path, extract_path):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)
    print(f"Extracted {file_path} to {extract_path}")

# Function to list all files in a directory recursively
def list_files(start_path):
    file_list = []
    for root, dirs, files in os.walk(start_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_list.append(file_path)
    return file_list

# Function to find all CSV file paths in a folder and save them to an index.json file
def find_all_file_paths(folder_path):
    files = list_files('/opt/airflow/extracted_data/')
    files = [file for file in files if file.endswith('.csv')]
    
    with open('extracted_data/index.json', 'w') as f:
        json.dump(files, f)
    
    return files

# Functions to extract month and year from a date string 
def get_date_month(date):
    try:
        return int(date.split('-')[1])
    except:
        return np.nan

def get_date_year(date):
    try:
        return int(date.split('-')[0])
    except:
        return np.nan

# Apache Beam DoFn class to read and clean CSV files for processing pipeline 
class ReadandClean_files(beam.DoFn):
    
    # Process function to read and clean CSV data 
    def process(self, file_path, **kwargs):
        field_cols = kwargs['field_cols']
        df = pd.read_csv(file_path, engine='python', encoding='utf-8', on_bad_lines='skip')
        
        df['MONTH'] = df['DATE'].apply(get_date_month)
        df['YEAR'] = df['DATE'].apply(get_date_year)
        
        for col in field_cols:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[^0-9.+-]', ''), errors='coerce')
        
        index_cols = ['LATITUDE', 'LONGITUDE', 'MONTH', 'YEAR']
        df = df[index_cols + field_cols]

        return [(tuple(x[0:4]), tuple(x[4:])) for x in df.itertuples(index=False, name=None)]

# Apache Beam DoFn class to compute mean values for processed data 
class ComputeMean(beam.DoFn):
    
    # Process function to compute mean values by key 
    def process(self, element, **kwargs):
        key, values = element
        
        value_element = []
        
        for value in values:
            row = []
            for item in value:
                try:
                    row.append(float(item))
                except ValueError:
                    row.append(np.nan)
            value_element.append(row)
        
        if value_element:
            mean_values = np.nanmean(value_element, axis=0)
            
            if hasattr(mean_values, 'tolist'):
                mean_list = mean_values.tolist()
            else:
                mean_list = mean_values
            
            return [(key, tuple(mean_list))]
        else:
            return []

# Function to format processed data into CSV format 
def format_files(element):
    key, mean_values = element
    return ','.join(map(str, key + tuple(mean_values)))

# Function to run the processing pipeline using Apache Beam 
def run_processing_pipeline(field_cols, output_path):
    
    with open('extracted_data/index.json') as f:
        files = json.load(f)
    
    with beam.Pipeline() as pipeline:
        records=(
            pipeline
            | 'Create FilePaths' >> beam.Create(files)
            | 'Read and Clean CSVs' >> beam.ParDo(ReadandClean_files(), field_cols=fields)
        )   

        grouped_records=(
            records
            | 'Group by key' >> beam.GroupByKey()
            | 'Compute mean by key' >> beam.ParDo(ComputeMean())
        )

        header=','.join(['LATITUDE', 'LONGITUDE', 'MONTH', 'YEAR'] + field_cols)

        output=(
            grouped_records
            | 'Format CSV' >> beam.Map(format_files)
            | 'Write to CSV' >> beam.io.WriteToText(output_path, file_name_suffix='.csv', shard_name_template='', header=header)
        )

# Define a function to create plots for a specific field based on geographical data
def make_plots_by_field(gdf, field):
    # Group data by latitude, longitude, month, and year and calculate the mean for the specified field
    grouped_data = gdf.groupby(['LATITUDE', 'LONGITUDE', 'MONTH', 'YEAR'])[field].mean().reset_index()
    
    # Create an output folder for the field if it doesn't exist
    output_folder_field = os.path.join('/opt/airflow/vizs', field)
    os.makedirs(output_folder_field, exist_ok=True)

    # Iterate over grouped data by month and year to create individual plots
    for index, group in grouped_data.groupby(['MONTH', 'YEAR']):
        month, year = index
        print(f"{month}, {year} month and year")
        
        # Format month and year for file naming
        month_year = f"{month:02d}_{year}"
        
        # Create a scatter plot with geographical data colored by the specified field
        fig, ax = plt.subplots(1, 1, figsize=(12, 10))
        group.plot(ax=ax, kind='scatter', x='LONGITUDE', y='LATITUDE', c=field, cmap='YlOrRd', legend=True,
                     s=30, edgecolor='black', linewidth=0.8)
        
        # Add world map background for context
        world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
        world.plot(ax=ax, color='lightgray', edgecolor='black')
        
        # Set plot title
        ax.set_title(f'Heatmap: {field} - {month_year}')
        ax.set_axis_off()

        # Create output folder for each year within the field folder
        output_folder_year = os.path.join(output_folder_field, str(year))
        os.makedirs(output_folder_year, exist_ok=True)

        # Save the plot as a PNG file in the specified folder structure
        output_file_path = os.path.join(output_folder_year, f"{month:02d}.png")
        
        plt.savefig(output_file_path, bbox_inches='tight')
        plt.close()

# Define a pipeline function to visualize heatmap fields using processed data
def visualisation_pipeline(heatmap_fields):
    df = pd.read_csv('/opt/airflow/output/processed.csv')
    df['LATITUDE'] = pd.to_numeric(df['LATITUDE'], errors='coerce')
    df['LONGITUDE'] = pd.to_numeric(df['LONGITUDE'], errors='coerce')
    
    # Create Point geometry from latitude and longitude coordinates
    geometry = [Point(xy) for xy in zip(df['LONGITUDE'], df['LATITUDE'])]
    GDF = gpd.GeoDataFrame(df, geometry=geometry)
    
    # Generate plots for each heatmap field
    for field in heatmap_fields:
        make_plots_by_field(GDF, field)
    
    print('Heatmap visualization completed')

# Check if filesensor returns True before proceeding with DAG creation
status = filesensor()
if status == False:
    exit()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Define the Airflow DAG with necessary details
dag = DAG(
    dag_id='task2dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Define tasks within the DAG

unzip_task = PythonOperator(
    task_id='unzip_task',
    python_callable=unzip_files,
    op_kwargs={'file_path': '/opt/airflow/output/data.zip', 'extract_path': '/opt/airflow/extracted_data/'},
    dag=dag,
)

path_find_pipeline = PythonOperator(
    task_id='file_path_find',
    python_callable=find_all_file_paths,
    op_kwargs={'folder_path': 'opt/airflow/extracted_data/'},
    dag=dag,
)

processing_pipeline = PythonOperator(
    task_id='data_processing_pipe',
    python_callable=run_processing_pipeline,
    op_kwargs={'field_cols': fields, 'output_path': 'output/processed'},
    dag=dag,
)

visualisation_pipeline_task = PythonOperator(
    task_id='data_visualization_pipeline',
    python_callable=visualisation_pipeline,
    op_kwargs={'heatmap_fields': heatmap_fields},
    dag=dag,
)

delete_data = BashOperator(
    task_id='delete_csv',
    bash_command='rm -r extracted_data/',
    dag=dag
)

# Define task dependencies within the DAG
unzip_task >> path_find_pipeline >> processing_pipeline >> visualisation_pipeline_task >> delete_data