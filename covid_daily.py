import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

project_dir = "/home/abhijeet/airflow_assignment"

config = {
    "STATES": {
        "an": "Andaman and Nicobar Islands",
        "ap": "Andhra Pradesh",
        "ar": "Arunachal Pradesh",
        "as": "Assam",
        "br": "Bihar",
        "ch": "Chandigarh",
        "ct": "Chhattisgarh",
        "dl": "Delhi",
        "dn": "Dadra and Nagar Haveli and Daman and Diu",
        "ga": "Goa",
        "gj": "Gujarat",
        "hp": "Himachal Pradesh",
        "hr": "Haryana",
        "jh": "Jharkhand",
        "jk": "Jammu and Kashmir",
        "ka": "Karnataka",
        "kl": "Kerala",
        "la": "Ladakh",
        "ld": "Lakshadweep",
        "mh": "Maharashtra",
        "ml": "Meghalaya",
        "mn": "Manipur",
        "mp": "Madhya Pradesh",
        "mz": "Mizoram",
        "nl": "Nagaland",
        "or": "Odisha",
        "pb": "Punjab",
        "py": "Puducherry",
        "rj": "Rajasthan",
        "sk": "Sikkim",
        "tg": "Telangana",
        "tn": "Tamil Nadu",
        "tr": "Tripura",
        "tt": "Total",
        "un": "State Unassigned",
        "up": "Uttar Pradesh",
        "ut": "Uttarakhand",
        "wb": "West Bengal"
    }
}


current_date = (datetime.now().date() - timedelta(days=1)).strftime("%d-%b-%y")


def get_data():
    try:
        response = requests.get("https://api.covid19india.org/states_daily.json").json()["states_daily"]
        new_cases_today = next(
            (item for item in response if item['status'] == "Confirmed" and item["date"] == current_date), None)
        return new_cases_today
    except:
        print(f"API Error")


def format_data(covid_cases_today):
    final_list = list()
    for k, v in config["STATES"].items():
        state_dict = dict()
        state_dict["Date"] = current_date
        state_dict["State_UnionTerritory"] = v
        state_dict["Count"] = covid_cases_today.get(k, 0)
        final_list.append(state_dict)
    return final_list


def export_csv(covid_data_list):
    covidDF = pd.DataFrame(covid_data_list)
    covidDF.to_csv(f"{project_dir}/csv/covid_india_{current_date}.csv", index=False)
    print(f"CSV exported to {project_dir}/csv/")


def task2():
    covid_api_response = get_data()
    state_wise_data_list = format_data(covid_api_response)
    export_csv(state_wise_data_list)


def get_create_dataset_bigquery(client):
    dataset_ref = client.dataset("covid_19_india_state_wise_dataset")
    dataset = bigquery.Dataset(dataset_ref)
    dataset.description = "COVID-19 state-wise cases Big Query"
    dataset = client.create_dataset(dataset, exists_ok=True)
    return dataset


def get_create_table_bigquery(client, dataset):
    table_ref = dataset.table("covid_19_india_state_wise")
    schema = [
        bigquery.SchemaField("Date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("State_UnionTerritory", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("Count", "INTEGER", mode="NULLABLE"),
    ]
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table, exists_ok=True)
    return table


def load_csv_file_to_bigquery(client, csv_filename, dataset_name, table_name):
    table_ref = client.dataset(dataset_name).table(table_name)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    with open(csv_filename, "rb") as file:
        load_job = client.load_table_from_file(file, table_ref, job_config=job_config)
        print("Starting job {}".format(load_job.job_id))

    load_job.result()
    print("Job finished.")

    covidDF = pd.read_csv(csv_filename)
    csv_length = len(covidDF)
    print(f"Percentage uploaded: {load_job.output_rows * 100 / csv_length}%")

    destination_table = client.get_table(table_ref)
    print("Loaded {} rows into {}:{}.".format(destination_table.num_rows, dataset_name, table_name))


def task3():
    service_account_creds_filepath = "/home/abhijeet/Downloads/wfhubertraining-9f6b34fd408e.json"
    client = bigquery.Client.from_service_account_json(service_account_creds_filepath)

    csv_filename = f"{project_dir}/csv/covid_india_{current_date}.csv"

    dataset_fn = get_create_dataset_bigquery(client)
    dataset_name = dataset_fn.dataset_id
    print(f"Dataset name : {dataset_name}")

    table_fn = get_create_table_bigquery(client, dataset_fn)
    table_name = table_fn.table_id
    print(f"Table name : {table_name}")

    load_csv_file_to_bigquery(client, csv_filename, dataset_name, table_name)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(year=2020, month=6, day=2),
    'email': ['abhijeet.shah@nineleaps.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'covid19_daily',
    default_args=default_args,
    description='A Pipeline to record the state-wise change in corona virus case everyday in India',
    schedule_interval='@daily',
)

t1 = BashOperator(
    task_id='Dag_Date',
    bash_command="date",
    dag=dag,
)

t2 = PythonOperator(
    task_id='Fetch_Data_and_Create_CSV',
    python_callable=task2,
    retries=3,
    dag=dag,
)

t3 = PythonOperator(
    task_id='Upload_Big_Query',
    python_callable=task3,
    retries=3,
    dag=dag,
)

dag.doc_md = __doc__

t1.doc_md = """\
            #### Task 1 : Install Requirements.
            Install requirements present in requirements.txt
            """

t2.doc_md = """\
            #### Task 2 : Fetch data from API & create a local csv.
            The API provides the change in Covid-19 Cases state-wise everyday
            """

t1 >> t2 >> t3
