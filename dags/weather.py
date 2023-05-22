# stuff to make the airflow, 1Password integration work
import os
import pendulum
import docker
import onepasswordconnectsdk
from airflow.decorators import dag, task
from onepasswordconnectsdk.client import Client, new_client

# libs for the dag portion, not the boilerplate
import json
import urllib.request

AIRFLOW_CHECKOUT_PATH = os.getenv("AIRFLOW_CHECKOUT_PATH")  			# The path to the airflow checkout, as seen from the host
DEPLOYMENT_ENVIRONMENT = os.environ.get("ENVIRONMENT", 'development')   # our current environment from ['production', 'development']
ONEPASSWORD_CONNECT_TOKEN = os.getenv("OP_API_TOKEN")       			# our secret to get secrets 🤐
ONEPASSWORD_CONNECT_HOST = os.getenv("OP_CONNECT")          			# where we get our secrets
VAULT_ID = os.getenv("OP_VAULT_ID")

REQUIRED_SECRETS = {
    "secret_value": {
        "opitem": "Diagnostic ETL (Weather)",
        "opfield": f"{DEPLOYMENT_ENVIRONMENT}.demonstration secret",
        "opvault": VAULT_ID,
    }
}

# instantiate a 1Password client
client: Client = new_client(ONEPASSWORD_CONNECT_HOST, ONEPASSWORD_CONNECT_TOKEN)
# get the requested secrets from 1Password
SECRETS = onepasswordconnectsdk.load_dict(client, REQUIRED_SECRETS)

# define the parameters of the DAG
@dag(
    dag_id="weather-checker",
    description="A DAG which checks the weather and writes out an HTML file",
    schedule="0/5 * * * *",
    start_date=pendulum.datetime(2023, 4, 10, tz="UTC"),
    catchup=False,
    tags=["weather"],
)

# 🥘 Boilerplate ends here; the rest is the DAG itself

def etl_weather():

    # A task which is encapsulated in a docker image
    @task()
    def download_image_and_annotate_with_docker():
        client = docker.from_env()
        docker_image = "frankinaustin/signal-annotate:latest"
        # pull at run time; remember to publish a amd64 image to docker hub
        client.images.pull(docker_image)
        logs = client.containers.run(
            image=docker_image, 
            # this docker image needs a place to write its output. the path is as seen from the host
            # server, because we've passed down the docker socket into the container running the flow.
            volumes=[AIRFLOW_CHECKOUT_PATH + '/weather:/opt/weather'],
            auto_remove=True,
            )
        return str(logs)

    # An extract task to get the time in Austin
    @task()
    def get_time_in_austin_tx():
        current_time = pendulum.now("America/Chicago") 
        return current_time.strftime("%m/%d/%Y, %H:%M:%S")

    # An extract task which pulls the weather forecast from the NOAA API
    @task()
    def get_weather():
        # open a URL and parse the JSON it returns
        with urllib.request.urlopen("https://api.weather.gov/gridpoints/EWX/156,91/forecast") as url:
            data = json.loads(url.read().decode())
            return data

    # A transform task which formats the weather forecast
    @task()
    def get_forecast(weather: dict):
        details = weather["properties"]["periods"][0]["detailedForecast"]
        period = weather["properties"]["periods"][0]["name"]
        return(f"{period}: {details}")

    # A load task which writes the forecast to an HTML file
    @task()
    def write_out_html_file(forecast: str, time: str, logs: str):
        f = open("/opt/airflow/weather/index.html", "w")
        f.write(f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>ATX Weather Report</title>
                <style>
                body {{
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    background-color: lightgreen;
                }}
                .weather {{
                    font-weight: bold;
                    font-size: 24px;
                }}
                .date {{
                    font-size: 16px;
                    margin-top: 24px; /* equal to the height of the first p element */
                    text-align: right;
                    
                }}
                img {{
                    justify-content: center;
                    margin: 0 20px 0 20px;
                }}
                </style>
            </head>
            <body>
                    <img src='annotated-image.jpg' alt='image manipulated with docker' />
                <div>
                    <p class="weather">{forecast}</p>
                    <p class="smaller">{time}</p>
                    <p class='smaller'>{DEPLOYMENT_ENVIRONMENT} secret: {SECRETS["secret_value"]}</p>
                </div>
            </body>
            </html>
        """)
        f.close()


    # End of tasks, start of DAG

    # Define the DAG/flow of the tasks. There are a handful of other ways to do this, also.
    time = get_time_in_austin_tx()
    weather = get_weather()
    forecast = get_forecast(weather)
    logs = download_image_and_annotate_with_docker()
    write_out_html_file(forecast=forecast, time=time, logs=logs)

etl_weather()
