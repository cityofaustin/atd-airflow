"""
Pulls the latest production docker images used by our dockerized ETLs.
"""

import logging
import subprocess
from os import getenv

from airflow.sdk import Param, chain, dag, task
from pendulum import datetime, duration

from utils.slack_operator import task_fail_slack_alert

DEPLOYMENT_ENVIRONMENT = getenv("ENVIRONMENT")
logger = logging.getLogger(__name__)

# Docker images used by our DAGs, kept warm so task runs don't have to pull on demand
DOCKER_IMAGES = [
    "atddocker/atd-airflow:production",

     # atd_cost_of_service_fees.py
    "atddocker/atd-cost-of-service:production",

    # atd_finance_data_fdu_program_tagging.py, atd_finance_data_fdus.py,
    # atd_finance_data_master_agreements.py, atd_finance_data_objects.py, 
    # atd_finance_data_subprojects.py, atd_finance_data_task_orders.py
    # atd_finance_data_units.py
    "atddocker/atd-finance-data:production", 

    # atd_kits_dms_message.py, atd_kits_sig_stat_pub.py 
    "atddocker/atd-kits:production",

    # atd_knack_311.py
    "atddocker/atd-knack-311:production",

    # atd_knack_banner.py
    "atddocker/atd-knack-banner:production",

    # atd_knack_amd_preventative_maintenance.py, atd_knack_artbox_signals.py, atd_knack_arterial_managment_locations.py
    # atd_knack_cctv_cameras.py, atd_knack_corridor_retiming.py, atd_knack_data_collection_requests.py,
    # atd_knack_data_tracker_location_updater.py, atd_knack_data_tracker_sr_asset_assign.py
    # atd_knack_data_tracker_street_segment_updater.py, atd_knack_detectors.py, atd_knack_development_services.py
    # atd_knack_dms.py, atd_knack_employees_tpw_hire.py, atd_knack_flashing_beacons.py
    # atd_knack_inventory_items_finance_to_data_tracker.py, atd_knack_inventory_items_nightly_snapshot.py
    # atd_knack_inventory_transactions.py, atd_knack_markings_attachments.py, atd_knack_markings_materials.py
    # atd_knack_markings_specifications.py, atd_knack_markings_work_orders_jobs.py, atd_knack_mmc_activities_to_socrata.py
    # atd_knack_mmc_issues.py, atd_knack_purchase_request_copier.py, atd_knack_school_beacons.py
    # atd_knack_school_zone_beacon_zones.py, atd_knack_secondary_signals.py, atd_knack_signal_cabinets.py
    # atd_knack_signal_detection_status_log.py, atd_knack_signal_requests.py, atd_knack_signal_studies.py
    # atd_knack_signal_work_orders.py, atd_knack_signals.py, atd_knack_signs_markings_reimbursements.py
    # atd_knack_signs_markings_time_logs.py, atd_knack_signs_materials.py, atd_knack_signs_work_order_attachments.py
    # atd_knack_signs_work_order_specifications.py, atd_knack_signs_work_orders.py, atd_knack_smd_311_csr.py
    # atd_knack_tcp_submissions.py, atd_knack_traffic_detectors_weekly_snapshot.py, atd_knack_work_orders_markings_contractors.py
    # atd_knack_work_orders_markings.py, dts_row_reporting.py
    "atddocker/atd-knack-services:production",

    # atd_moped_components_to_agol.py
    "atddocker/atd-moped-etl-arcgis:production",
    
    # atd_moped_data_tracker_sync.py
    "atddocker/atd-moped-etl-data-tracker-sync:production",

    # atd_moped_ecapris_funding_sync.py
    "atddocker/atd-moped-etl-ecapris-funding:production",
    
    # atd_moped_ecapris_status_sync.py
    "atddocker/atd-moped-etl-ecapris-statuses:production",
    
    # atd_parking_data.py
    "atddocker/atd-parking-data-meters:production",
    
    # atd_road_conditions_socrata.py
    "atddocker/atd-road-conditions:production",

    # atd_service_bot_index_issues_to_dts_portal.py, atd_service_bot_intake.py, atd_service_bot_issues_to_socrata.py
    "atddocker/atd-service-bot:production",

    # atd_signal_comms.py
    "atddocker/atd-signal-comms:production",
    
    # dts_public_safety_incident_reports.py
    "atddocker/atd-traffic-incident-reports:production",

    # dts_311_report_publishing.py, dts_open_311_scrape.py
    "atddocker/dts-311-reporting:production",
    
    # dts_finances_report_publishing.py
    "atddocker/dts-finance-reporting:production",

    # dts_maximo_reporting.py
    "atddocker/dts-maximo-reporting:production",
    
    # dts_pavement_ops_reporting.py
    "atddocker/dts-pavement-ops-reporting:production",

    # dts_row_reporting.py
    "atddocker/dts-right-of-way-reporting:production",

    # dts_traffic_signal_metrics.py
    "atddocker/dts-traffic-signal-metrics:production",

    # dts_work_zone_data_feed.py, dts_work_zone_segment_updater.py
    "atddocker/dts-work-zone-data-feed:production",
    
    # maximo_geo_emergency_mgmt_email_parser.py
    "atddocker/maximo-geo-emergency-mgmt:production",
    
    # vz-afd-ems-import.py
    "atddocker/vz-afd-ems-import:production",
    
    # vz_cad_import.py
    "atddocker/vz-cad-incidents-import:production",

    # vz_cris_import.py
    "atddocker/vz-cris-import:production",

    # vz_cris_import.py
    "atddocker/vz-ems-person-match:production",
    
    # vz_moped_component_crashes.py
    "atddocker/vz-moped-join:production",
    
    # vz_refresh_location_crashes.py
    "atddocker/vz-run-sql:production",
    
    # vz_moped_component_crashes.py, vz_socrata_export.py
    "atddocker/vz-socrata-export:production",
]


@dag(
    dag_id="airflow_docker_image_pull",
    schedule="0 */6 * * *" if DEPLOYMENT_ENVIRONMENT == "production" else None,
    start_date=datetime(2015, 12, 1, tz="America/Chicago"),
    catchup=False,
    tags=["repo:atd-airflow"],
    default_args={
        "owner": "airflow",
        "retries": 0,
        "on_failure_callback": task_fail_slack_alert,
        "execution_timeout": duration(minutes=30),
    },
    description="Pull docker images used by our ETLs to keep them up to date",
    params={
        "dry_run": Param(
            title="Dry run",
            default=False,
            type="boolean",
            description_md="Log images that would be pulled without running docker pull.",
        ),
    },
)
def airflow_docker_image_pull():
    """
    Pulls the docker images used by our ETLs.

    This DAG runs every 6 hours in production to keep local docker images
    current, so DAG runs don't need to pull on demand.
    """

    @task
    def pull_image(image: str, params):
        """Pull a single docker image, or log what would be pulled in dry-run mode."""
        if bool(params["dry_run"]):
            logger.info("Would pull %s", image)
            return

        logger.info("Pulling %s", image)
        subprocess.run(["docker", "pull", image], check=True)

    pull_tasks = []
    for image in DOCKER_IMAGES:
        # atddocker/atd-airflow:production -> pull_atd-airflow
        image_name = image.split("/")[-1].split(":")[0]
        pull_tasks.append(
            pull_image.override(
                task_id=f"pull_{image_name}",
                # Continue the chain even if an upstream pull failed
                trigger_rule="all_done",
            )(image)
        )

    chain(*pull_tasks)


# Instantiate the DAG
airflow_docker_image_pull()
