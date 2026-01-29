from __future__ import annotations

from datetime import datetime, timedelta
import os
from typing import Any

import random
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client

CLICKHOUSE_CONN_ID = "clickhouse_default"
CRM_CONN_ID = "crm_postgres"
TELEMETRY_CONN_ID = "telemetry_postgres"

def get_clickhouse_client(database: str | None = None) -> Client:
    return Client(
        host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.getenv("CLICKHOUSE_PORT", "9000")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=database or os.getenv("CLICKHOUSE_DB", "reports"),
    )


def create_tables() -> None:
    bootstrap_client = get_clickhouse_client(database="default")
    bootstrap_client.execute("CREATE DATABASE IF NOT EXISTS reports")

    client = get_clickhouse_client(database="reports")
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS reports.crm_customers
        (
            customer_id String,
            customer_name String,
            customer_email String
        )
        ENGINE = MergeTree
        ORDER BY customer_id
        """
    )

    client.execute(
        """
        CREATE TABLE IF NOT EXISTS reports.crm_prostheses
        (
            prosthesis_id String,
            customer_id String,
            device_type String
        )
        ENGINE = MergeTree
        ORDER BY (customer_id, prosthesis_id)
        """
    )

    client.execute(
        """
        CREATE TABLE IF NOT EXISTS reports.telemetry_events
        (
            event_id String,
            prosthesis_id String,
            event_time DateTime,
            response_time_ms Float64,
            signal_strength Float64,
            noise_level Float64,
            battery_level Float64,
            gestures_count UInt32
        )
        ENGINE = MergeTree
        ORDER BY (prosthesis_id, event_time)
        """
    )

    client.execute(
        """
        CREATE TABLE IF NOT EXISTS reports.report_mart
        (
            user_id String,
            prosthesis_id String,
            customer_name String,
            customer_email String,
            device_type String,
            total_events UInt64,
            avg_response_time_ms Float64,
            max_response_time_ms Float64,
            avg_signal_strength Float64,
            avg_noise_level Float64,
            avg_battery_level Float64,
            min_battery_level Float64,
            total_gestures UInt64,
            last_seen_at DateTime
        )
        ENGINE = MergeTree
        ORDER BY (user_id, prosthesis_id)
        """
    )


def load_crm_data() -> None:
    crm_hook = PostgresHook(postgres_conn_id=CRM_CONN_ID)
    customer_rows = crm_hook.get_records("SELECT id, name, email FROM customers")
    prosthesis_rows = crm_hook.get_records(
        "SELECT id, customer_id, device_type FROM prostheses"
    )

    client = get_clickhouse_client()
    client.execute("TRUNCATE TABLE reports.crm_customers")
    if customer_rows:
        client.execute(
            "INSERT INTO reports.crm_customers (customer_id, customer_name, customer_email) VALUES",
            customer_rows,
        )

    client.execute("TRUNCATE TABLE reports.crm_prostheses")
    if prosthesis_rows:
        client.execute(
            "INSERT INTO reports.crm_prostheses (prosthesis_id, customer_id, device_type) VALUES",
            prosthesis_rows,
        )


def seed_crm_data() -> None:
    crm_hook = PostgresHook(postgres_conn_id=CRM_CONN_ID)
    crm_hook.run(
        """
        CREATE TABLE IF NOT EXISTS customers (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        );
        """
    )
    crm_hook.run(
        """
        CREATE TABLE IF NOT EXISTS prostheses (
            id UUID PRIMARY KEY,
            customer_id TEXT NOT NULL REFERENCES customers(id),
            device_type TEXT NOT NULL
        );
        """
    )

    # Customer IDs are Keycloak usernames so that report_mart.user_id
    # matches the JWT "preferred_username" claim for each logged-in user.
    keycloak_users = [
        ("user1", "User One", "user1@example.com"),
        ("user2", "User Two", "user2@example.com"),
        ("admin1", "Admin One", "admin1@example.com"),
        ("prothetic1", "Prothetic One", "prothetic1@example.com"),
        ("prothetic2", "Prothetic Two", "prothetic2@example.com"),
        ("prothetic3", "Prothetic Three", "prothetic3@example.com"),
    ]

    customer_rows = []
    prosthesis_rows = []
    for customer_id, name, email in keycloak_users:
        customer_rows.append((customer_id, name, email))
        for device_index in range(1, 3):
            prosthesis_rows.append(
                (str(uuid.uuid4()), customer_id, f"Model-{device_index}")
            )

    crm_hook.insert_rows(
        table="customers",
        rows=customer_rows,
        target_fields=["id", "name", "email"],
        replace=True,
        replace_index=["id"],
    )
    crm_hook.insert_rows(
        table="prostheses",
        rows=prosthesis_rows,
        target_fields=["id", "customer_id", "device_type"],
        replace=True,
        replace_index=["id"],
    )


def seed_telemetry_data() -> None:
    telemetry_hook = PostgresHook(postgres_conn_id=TELEMETRY_CONN_ID)
    crm_hook = PostgresHook(postgres_conn_id=CRM_CONN_ID)
    telemetry_hook.run(
        """
        CREATE TABLE IF NOT EXISTS sensor_events (
            id UUID PRIMARY KEY,
            prosthesis_id UUID NOT NULL,
            event_time TIMESTAMP NOT NULL,
            response_time_ms DOUBLE PRECISION NOT NULL,
            signal_strength DOUBLE PRECISION NOT NULL,
            noise_level DOUBLE PRECISION NOT NULL,
            battery_level DOUBLE PRECISION NOT NULL,
            gestures_count INTEGER NOT NULL
        );
        """
    )
    prosthesis_ids = crm_hook.get_records("SELECT id FROM prostheses")
    if not prosthesis_ids:
        return

    now = datetime.utcnow()
    events = []
    for prosthesis_id, in prosthesis_ids:
        # Simulate battery draining over 24 hours
        battery_start = random.uniform(85.0, 100.0)
        for offset in range(0, 24):
            battery = max(5.0, battery_start - offset * random.uniform(1.5, 3.5))
            events.append(
                (
                    str(uuid.uuid4()),
                    str(prosthesis_id),
                    now - timedelta(hours=offset),
                    random.uniform(40.0, 130.0),   # response_time_ms
                    random.uniform(0.3, 1.0),       # signal_strength (mV)
                    random.uniform(0.01, 0.15),     # noise_level (mV)
                    round(battery, 1),              # battery_level (%)
                    random.randint(5, 60),          # gestures_count
                )
            )

    telemetry_hook.insert_rows(
        table="sensor_events",
        rows=events,
        target_fields=[
            "id", "prosthesis_id", "event_time",
            "response_time_ms", "signal_strength", "noise_level",
            "battery_level", "gestures_count",
        ],
        replace=True,
        replace_index=["id"],
    )


def load_telemetry_data(**context: Any) -> None:
    data_interval_start = context.get("data_interval_start")
    data_interval_end = context.get("data_interval_end")
    telemetry_hook = PostgresHook(postgres_conn_id=TELEMETRY_CONN_ID)
    query = (
        "SELECT id, prosthesis_id, event_time, "
        "response_time_ms, signal_strength, noise_level, "
        "battery_level, gestures_count "
        "FROM sensor_events WHERE event_time >= %s AND event_time < %s"
    )
    telemetry_rows = telemetry_hook.get_records(
        query,
        parameters=(data_interval_start, data_interval_end),
    )

    client = get_clickhouse_client()
    if telemetry_rows:
        client.execute(
            "INSERT INTO reports.telemetry_events "
            "(event_id, prosthesis_id, event_time, "
            "response_time_ms, signal_strength, noise_level, "
            "battery_level, gestures_count) VALUES",
            telemetry_rows,
        )


def build_report_mart() -> None:
    client = get_clickhouse_client()
    client.execute("TRUNCATE TABLE reports.report_mart")
    client.execute(
        """
        INSERT INTO reports.report_mart
        SELECT
            customers.customer_id AS user_id,
            prostheses.prosthesis_id AS prosthesis_id,
            customers.customer_name AS customer_name,
            customers.customer_email AS customer_email,
            prostheses.device_type AS device_type,
            COUNT(telemetry.event_id) AS total_events,
            AVG(telemetry.response_time_ms) AS avg_response_time_ms,
            MAX(telemetry.response_time_ms) AS max_response_time_ms,
            AVG(telemetry.signal_strength) AS avg_signal_strength,
            AVG(telemetry.noise_level) AS avg_noise_level,
            AVG(telemetry.battery_level) AS avg_battery_level,
            MIN(telemetry.battery_level) AS min_battery_level,
            SUM(telemetry.gestures_count) AS total_gestures,
            MAX(telemetry.event_time) AS last_seen_at
        FROM reports.crm_customers AS customers
        INNER JOIN reports.crm_prostheses AS prostheses
            ON customers.customer_id = prostheses.customer_id
        LEFT JOIN reports.telemetry_events AS telemetry
            ON prostheses.prosthesis_id = telemetry.prosthesis_id
        GROUP BY
            customers.customer_id,
            prostheses.prosthesis_id,
            customers.customer_name,
            customers.customer_email,
            prostheses.device_type
        """
    )


with DAG(
    dag_id="crm_telemetry_reporting",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    default_args={"owner": "airflow"},
    tags=["reports", "etl"],
) as dag:
    create_tables_task = PythonOperator(
        task_id="create_clickhouse_tables",
        python_callable=create_tables,
    )

    seed_crm_task = PythonOperator(
        task_id="seed_crm_data",
        python_callable=seed_crm_data,
    )

    seed_telemetry_task = PythonOperator(
        task_id="seed_telemetry_data",
        python_callable=seed_telemetry_data,
    )

    load_crm_task = PythonOperator(
        task_id="load_crm_data",
        python_callable=load_crm_data,
    )

    load_telemetry_task = PythonOperator(
        task_id="load_telemetry_data",
        python_callable=load_telemetry_data,
    )

    build_mart_task = PythonOperator(
        task_id="build_report_mart",
        python_callable=build_report_mart,
    )

    create_tables_task >> seed_crm_task >> seed_telemetry_task
    seed_crm_task >> load_crm_task
    seed_telemetry_task >> load_telemetry_task
    [load_crm_task, load_telemetry_task] >> build_mart_task
