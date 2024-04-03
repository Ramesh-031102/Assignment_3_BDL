import os
import zipfile
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime

# Function to scrape data for a specific year and download CSV files
def scrape_data(ds, year, num_files, **kwargs):
    # Print current working directory
    print('cwd:', os.getcwd())
    
    # Base URL for NOAA data access
    base_url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}'
    
    # Create directory to store downloaded data for the year
    os.makedirs(f'/opt/airflow/data/{year}', exist_ok=True)
    
    # Construct URL for the specific year
    url = base_url.format(year=year)
    
    # Make a request to the URL and parse the HTML content using BeautifulSoup
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    # Find the table containing links to CSV files
    table = soup.find('table')
    anchors = table.find_all('a')
    
    # Filter out links that point to CSV files
    anchors = [a for a in anchors if 'csv' in a.text]
    
    files_downloaded = 0
    crashflag = 0
    
    # Loop through each CSV file link and download the file
    for anchor in anchors:
        if crashflag > 2:
            break
        
        if files_downloaded >= num_files:
            break
        
        file = anchor.text
        file_url = f'{url}/{file}'
        
        # Check if the file URL is valid before downloading
        if requests.get(file_url) == None:
            crashflag += 1
            continue
        
        res = requests.get(file_url)
        csv = res.text
        
        # Save the downloaded CSV file to the specified directory
        with open(f'/opt/airflow/data/{year}/{file}', 'w') as f:
            f.write(csv)
        
        files_downloaded += 1

# Function to zip the downloaded data directory
def zip_directory():
    os.makedirs('/opt/airflow/output', exist_ok=True)
    source_dir = '/opt/airflow/data'
    zip_filename = '/opt/airflow/output/data.zip'
    
    # Create a zip file and add all files from the data directory to it
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname=arcname)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
}

# Create a new DAG named 'task1dag'
dag = DAG(
    'task1dag',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

# Define a DummyOperator task to gather scraped files
gather_task = DummyOperator(
    task_id='gather_scraped_files',
    dag=dag,
)

# Define a PythonOperator task to archive the downloaded data into a zip file
archive_task = PythonOperator(
    task_id='archive_data',
    python_callable=zip_directory,
    provide_context=True,
    dag=dag,
)

# Loop through years from 2010 to 2022 and create scrape_data tasks for each year
for year in range(2010, 2023):
    task_id = f'scrape_data_{year}'
    
    scrape_task = PythonOperator(
        task_id=task_id,
        python_callable=scrape_data,
        op_kwargs={'year': year, 'num_files': 10},
        provide_context=True,
        dag=dag,
    )
    
    # Set task dependencies: scrape_task -> gather_task 
    scrape_task >> gather_task

# Set task dependency: gather_task -> archive_task 
gather_task >> archive_task