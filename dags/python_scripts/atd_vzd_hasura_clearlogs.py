import json
import os
import requests
import time


def make_update() -> dict:
    """
    Runs a SQL query against Hasura and tries to dissociate the location
    for any CR3 crashes that fall in the main-lane polygon.
    :return dict:
    """
    response = requests.post(
        url=os.getenv("HASURA_ENDPOINT"),
        headers={
            "Accept": "*/*",
            "content-type": "application/json",
            "x-hasura-admin-secret": os.getenv("HASURA_ADMIN_KEY")
        },
        json={
            "type": "run_sql",
            "args": {
                "sql": """
                    /*
                        This query should clear event data:
                        https://hasura.io/docs/1.0/graphql/manual/event-triggers/clean-up.html
                    */
                    DELETE FROM hdb_catalog.event_invocation_logs;
                    DELETE FROM hdb_catalog.event_log WHERE delivered = true OR error = true;
                """
            }
        }
    )
    response.encoding = "utf-8"
    return response.json()


def main():
    print("Clearing Hasura event logs from the database")
    start = time.time()

    try:
        response = make_update()
    except Exception as e:
        response = {
            "errors": str(e)
        }

    response = json.dumps(response)
    print("Response: ")
    print(response)
    if "errors" in response:
        print("Error detected, exiting...")
        exit(1)

    # Stop timer and print duration
    end = time.time()
    hours, rem = divmod(end - start, 3600)
    minutes, seconds = divmod(rem, 60)
    print("Finished in: {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))


if __name__ == "__main__":
    main()
