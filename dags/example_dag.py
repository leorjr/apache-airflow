import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

def consumir_api_pokemon():
    url = "https://pokeapi.co/api/v2/pokemon"
    response = requests.get(url)

    if response.status_code == 200:
        print(response.json())
    else:
        print("Falha ao consumir a API")

def log_inicial():
    print("################")
    print("log inicial do serviço")
    print("################")

@dag(schedule_interval="@daily", start_date=days_ago(1), catchup=False)
def example_pokemon_dag():

    @task
    def log_inicial_service():
        log_inicial()

    @task
    def fetch_and_log_pokemon_info():
        consumir_api_pokemon()

    
    # definindo a ordem das tasks
    log_inicial_service() >> fetch_and_log_pokemon_info()

# Instanciando o DAG
dag = example_pokemon_dag()
