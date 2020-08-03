import json
import os
import requests
import time


def make_update() -> dict:
    """
    Runs a SQL query against Hasura and tries to assign a
    location to any Non-CR3s that do not have location yet.
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
                    UPDATE atd_txdot_crashes
                        SET location_id = (SELECT location_id FROM find_location_for_cr3_collision(case_id) LIMIT 1)::text, updated_by = 'SYSTEM'
                        WHERE 1=1
                          AND location_id IS NULL
                          AND (SELECT location_id FROM find_location_for_cr3_collision(crash_id) LIMIT 1) IS NOT NULL;
                """.strip()
            }
        }
    )
    response.encoding = "utf-8"
    return response.json()


def main():
    endpoint = os.getenv("HASURA_ENDPOINT")
    print("Updating non-cr3 records in the database")
    print(f"Hasura Endpoint: {endpoint}")
    print(f"Please wait, this could take a minute or two.")
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
