import requests
import json
import sys
import os

GRAFANA_URL = os.getenv("GRAFANA_URL", "http://grafana:3000")
API_KEY = sys.argv[1]
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "BICO")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {API_KEY}"
}

# Define PostgreSQL data source
data_source_payload = {
    "name": "PostgreSQL",
    "type": "postgres",
    "url": f"{POSTGRES_HOST}:{POSTGRES_PORT}",
    "access": "proxy",
    "database": POSTGRES_DB,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "basicAuth": False,
    "isDefault": True,
    "jsonData": {
        "sslmode": "disable"
    }
}

# Create data source
response = requests.post(f"{GRAFANA_URL}/api/datasources", headers=HEADERS, data=json.dumps(data_source_payload))

if response.status_code == 200:
    print("Data source created successfully.")
else:
    print(f"Failed to create data source: {response.content}")

# Define dashboard with timeseries and bar chart panels
dashboard_payload = {
    "dashboard": {
        "id": None,
        "title": "User Operation Dashboard",
        "tags": ["automated"],
        "timezone": "browser",
        "schemaVersion": 16,
        "version": 0,
        "refresh": "5s",
        "panels": [
            {
                "title": "User Operations Per Hour",
                "type": "barchart",
                "gridPos": {"x": 0, "y": 0, "w": 24, "h": 8},
                "datasource": "PostgreSQL",
                "targets": [
                    {
                        "refId": "A",
                        "rawSql": '''
                            SELECT 
                              date_trunc('hour', snapshot_timestamp) AS time,
                              COALESCE(beneficiary_bundler.entity_name, tx_bundler.entity_name, 'Other') AS bundler,
                              COUNT(*) AS value 
                              FROM pipeline.raw_user_operations rao
                              LEFT JOIN pipeline.bundlers beneficiary_bundler ON lower(beneficiary_bundler.address) = lower(rao.beneficiary)
                              LEFT JOIN pipeline.bundlers tx_bundler ON lower(tx_bundler.address) = lower(rao.bundler)
                              GROUP BY 1, 2 ORDER BY 1                           
                            ''',
                        "format": "time_series"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "mappings": [],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [{"color": "blue", "value": None}]
                        }
                    }
                },
                "options": {
                    "stacked": True,
                    "tooltip": {
                        "mode": "single"
                    },
                    "legend": {
                        "displayMode": "table",
                        "placement": "bottom"
                    }
                }
            },
            {
                "title": "Sponsorship Mix Per Minute",
                "type": "barchart",
                "gridPos": {"x": 0, "y": 8, "w": 24, "h": 8},
                "datasource": "PostgreSQL",
                "targets": [
                    {
                        "refId": "B",
                        "rawSql": '''
                            SELECT 
                              date_trunc('minute', snapshot_timestamp) AS time,
                              CASE
                                WHEN paymaster IS NOT NULL AND lower(paymaster) <> '0x0000000000000000000000000000000000000000' THEN 'Paymaster Sponsored'
                                ELSE 'Wallet Funded'
                              END AS sponsor_type,
                              COUNT(*) AS value 
                              FROM pipeline.raw_user_operations rao
                              GROUP BY 1, 2 ORDER BY 1                           
                            ''',
                        "format": "time_series"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "mappings": [],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [{"color": "blue", "value": None}]
                        }
                    }
                },
                "options": {
                    "stacked": True,
                    "tooltip": {
                        "mode": "single"
                    },
                    "legend": {
                        "displayMode": "table",
                        "placement": "bottom"
                    }
                }
            },
            {
                "title": "Average Prefund vs Actual Gas Cost",
                "type": "timeseries",
                "gridPos": {"x": 0, "y": 16, "w": 24, "h": 8},
                "datasource": "PostgreSQL",
                "targets": [
                    {
                        "refId": "C",
                        "rawSql": '''
                            SELECT
                              date_trunc('minute', snapshot_timestamp) AS time,
                              'Required Prefund' AS metric,
                              AVG(required_prefund) / 1e18 AS value
                            FROM pipeline.raw_user_operations
                            WHERE required_prefund IS NOT NULL
                            GROUP BY 1
                            UNION ALL
                            SELECT
                              date_trunc('minute', snapshot_timestamp) AS time,
                              'Actual Gas Cost' AS metric,
                              AVG(actual_gas_cost) / 1e18 AS value
                            FROM pipeline.raw_user_operations
                            WHERE actual_gas_cost IS NOT NULL
                            GROUP BY 1
                            ORDER BY 1
                            ''',
                        "format": "time_series"
                    }
                ],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "mappings": [],
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [{"color": "blue", "value": None}]
                        }
                    }
                },
                "options": {
                    "tooltip": {
                        "mode": "single"
                    },
                    "legend": {
                        "displayMode": "table",
                        "placement": "bottom"
                    }
                }
            },
        ],
        "time": {
            "from": "now-5m",
            "to": "now"
        },
        "timepicker": {
            "refresh_intervals": ["5s", "10s", "30s", "1m", "5m", "15m", "30m", "1h", "2h", "1d"]
        }
    },
    "overwrite": True
}

# Create dashboard
response = requests.post(f"{GRAFANA_URL}/api/dashboards/db", headers=HEADERS, data=json.dumps(dashboard_payload))

if response.status_code == 200:
    print("Dashboard created successfully.")
else:
    print(f"Failed to create dashboard: {response.content}")
