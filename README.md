# COVID-19 Daily State-case India

An Airflow-BigQuery Python Application which runs daily and uploads the daily change in covid-19 cases in India state-wise.

___
## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites
Things required to get the application up and running.
* Python 3
* pip3
* PyCharm CE 2020

### Installation
A step by step series of examples that tell you how to get a development environment running.

Run the below commands :-

1. To install Apache-Airflow, Google BigQuery Library and other dependencies :    

```
    $ sudo pip3 install -r requirements.txt
``` 
    
2. Initialize Airflow Database :
```
    $ airflow initdb
    $ mkdir -p ~/airflow/dags
```
    
3. Google BigQuery Console  :

    1. Start a new project on [BigQuery](https://console.cloud.google.com/).
    2. Create a new Service Account.
    3. Download the JSON file.
    

4.  Open the `covid_daily.py` and change the `service_account_creds_filepath` to path of the downloaded application credentials. 

5.  Create a directory to store the generated csv :
```
    $ mkdir -p ~/airflow_assignment/csv
```
6. Copy and paste the `covid_daily.py` file into the `dags` folder inside your `~/airflow`.

### Steps to run the application:

1. Open a terminal, say T1 and start the webserver using :
```
    $ airflow webserver -p 8080
```   
2. Open another terminal, say T2 and start the scheduler : 
```
    $ airflow scheduler
```
3. Open the browser and navigate to localhost:8080 to monitor the DAG.
4. Find the DAG with ID as `covid19_daily` and switch it `ON`
5. Monitor and see the Logs.

---
### License
This project is licensed under Nineleaps Technology Solutions Pvt Ltd. © 2020.