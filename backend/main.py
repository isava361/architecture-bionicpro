import os
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
from typing import Any

import requests
from clickhouse_driver import Client
from fastapi import Depends, FastAPI, Header, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from jose import jwt

app = FastAPI(title="Reports API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Settings:
    def __init__(self) -> None:
        self.clickhouse_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        self.clickhouse_port = int(os.getenv("CLICKHOUSE_PORT", "9000"))
        self.clickhouse_user = os.getenv("CLICKHOUSE_USER", "default")
        self.clickhouse_password = os.getenv("CLICKHOUSE_PASSWORD", "")
        self.clickhouse_database = os.getenv("CLICKHOUSE_DB", "reports")
        self.keycloak_url = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
        self.keycloak_realm = os.getenv("KEYCLOAK_REALM", "reports-realm")
        self.keycloak_audience = os.getenv("KEYCLOAK_AUDIENCE", "reports-api")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def get_clickhouse_client(settings: Settings) -> Client:
    return Client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        user=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
    )


def fetch_jwks(settings: Settings) -> dict[str, Any]:
    url = (
        f"{settings.keycloak_url}/realms/{settings.keycloak_realm}"
        "/protocol/openid-connect/certs"
    )
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


def decode_token(token: str, settings: Settings) -> dict[str, Any]:
    try:
        unverified_header = jwt.get_unverified_header(token)
    except jwt.JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token header",
        ) from exc

    jwks = fetch_jwks(settings)
    key = next((item for item in jwks.get("keys", []) if item.get("kid") == unverified_header.get("kid")), None)
    if not key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Signing key not found",
        )

    try:
        return jwt.decode(
            token,
            key,
            algorithms=[key.get("alg", "RS256")],
            audience=settings.keycloak_audience,
            options={"verify_aud": True},
        )
    except jwt.JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token validation failed",
        ) from exc


def get_current_user(authorization: str | None = Header(None), settings: Settings = Depends(get_settings)) -> dict[str, Any]:
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
        )

    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Authorization header",
        )

    claims = decode_token(token, settings)
    if "sub" not in claims:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token missing subject",
        )
    return claims


def parse_date_range(start: str | None, end: str | None, max_available: datetime) -> tuple[datetime, datetime]:
    if end:
        end_dt = parse_datetime_input(end)
    else:
        end_dt = max_available

    if start:
        start_dt = parse_datetime_input(start)
    else:
        start_dt = end_dt - timedelta(days=30)

    if start_dt > end_dt:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start must be before end",
        )

    if end_dt > max_available:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Requested period is not available yet",
        )

    return start_dt, end_dt


def parse_datetime_input(value: str) -> datetime:
    try:
        if "T" in value:
            parsed = datetime.fromisoformat(value)
        else:
            parsed_date = date.fromisoformat(value)
            parsed = datetime.combine(parsed_date, time.min)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format, use YYYY-MM-DD or ISO-8601 datetime",
        ) from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


@app.get("/reports")
async def get_report(
    start: str | None = Query(None, description="Report start date (YYYY-MM-DD or ISO-8601)"),
    end: str | None = Query(None, description="Report end date (YYYY-MM-DD or ISO-8601)"),
    user: dict[str, Any] = Depends(get_current_user),
    settings: Settings = Depends(get_settings),
) -> dict[str, Any]:
    user_id = user["preferred_username"]
    client = get_clickhouse_client(settings)
    max_row = client.execute(
        "SELECT max(last_seen_at) FROM reports.report_mart",
    )
    max_available = max_row[0][0] if max_row and max_row[0] else None
    if not max_available:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Report data is not available yet",
        )

    start_dt, end_dt = parse_date_range(start, end, max_available)
    rows = client.execute(
        """
        SELECT
            user_id,
            prosthesis_id,
            customer_name,
            customer_email,
            device_type,
            total_events,
            avg_response_time_ms,
            max_response_time_ms,
            avg_signal_strength,
            avg_noise_level,
            avg_battery_level,
            min_battery_level,
            total_gestures,
            last_seen_at
        FROM reports.report_mart
        WHERE user_id = %(user_id)s
          AND last_seen_at >= %(start_date)s
          AND last_seen_at <= %(end_date)s
        ORDER BY last_seen_at DESC
        """,
        {"user_id": user_id, "start_date": start_dt, "end_date": end_dt},
    )

    report = [
        {
            "user_id": row[0],
            "prosthesis_id": row[1],
            "customer_name": row[2],
            "customer_email": row[3],
            "device_type": row[4],
            "total_events": row[5],
            "avg_response_time_ms": round(row[6], 2),
            "max_response_time_ms": round(row[7], 2),
            "avg_signal_strength": round(row[8], 4),
            "avg_noise_level": round(row[9], 4),
            "avg_battery_level": round(row[10], 1),
            "min_battery_level": round(row[11], 1),
            "total_gestures": row[12],
            "last_seen_at": row[13].isoformat() if row[13] else None,
        }
        for row in rows
    ]

    return {
        "user_id": user_id,
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
        "report": report,
    }
