import json
from datetime import datetime, timezone
from typing import Any

from superset import db, security_manager
from superset.app import create_app


DASHBOARD_TITLE = "ShopFlow CDC Lakehouse"
DATABASE_NAME = "ShopFlow Iceberg"
PALETTE = [
    "#7fad91",
    "#b8a9d9",
    "#a63a54",
    "#ef7f82",
    "#c9a9c0",
    "#44364f",
    "#e8748a",
    "#34293f",
]


DATASETS: dict[str, dict[str, Any]] = {
    "shopflow_cdc_operations": {
        "schema": "bronze",
        "sql": """
SELECT
  CASE json_extract_scalar(raw_payload, '$.op')
    WHEN 'c' THEN 'INSERT'
    WHEN 'r' THEN 'SNAPSHOT'
    WHEN 'u' THEN 'UPDATE'
    WHEN 'd' THEN 'DELETE'
    ELSE COALESCE(json_extract_scalar(raw_payload, '$.op'), 'UNKNOWN')
  END AS operation,
  COUNT(*) AS total_events
FROM bronze.cdc_events
WHERE raw_payload IS NOT NULL
GROUP BY 1
""",
        "columns": [("operation", "STRING", True), ("total_events", "BIGINT", False)],
        "metrics": [("events", "SUM(total_events)")],
    },
    "shopflow_events_timeline": {
        "schema": "bronze",
        "sql": """
SELECT
  date_trunc('minute', kafka_timestamp) AS event_minute,
  COUNT(*) AS total_events
FROM bronze.cdc_events
WHERE kafka_timestamp >= current_timestamp - INTERVAL '60' MINUTE
GROUP BY 1
""",
        "columns": [("event_minute", "TIMESTAMP", False), ("total_events", "BIGINT", False)],
        "metrics": [("events", "SUM(total_events)")],
        "main_dttm_col": "event_minute",
    },
    "shopflow_orders_by_status": {
        "schema": "gold",
        "sql": """
SELECT order_status, total_orders, gold_updated_at
FROM gold.orders_by_status
""",
        "columns": [
            ("order_status", "STRING", True),
            ("total_orders", "BIGINT", False),
            ("gold_updated_at", "TIMESTAMP", False),
        ],
        "metrics": [("orders", "SUM(total_orders)")],
    },
    "shopflow_orders_by_channel": {
        "schema": "gold",
        "sql": """
SELECT sales_channel, total_orders, gold_updated_at
FROM gold.orders_by_channel
""",
        "columns": [
            ("sales_channel", "STRING", True),
            ("total_orders", "BIGINT", False),
            ("gold_updated_at", "TIMESTAMP", False),
        ],
        "metrics": [("orders", "SUM(total_orders)")],
    },
    "shopflow_customer_segments": {
        "schema": "gold",
        "sql": """
SELECT customer_segment, total_customers, gold_updated_at
FROM gold.customer_segments
""",
        "columns": [
            ("customer_segment", "STRING", True),
            ("total_customers", "BIGINT", False),
            ("gold_updated_at", "TIMESTAMP", False),
        ],
        "metrics": [("customers", "SUM(total_customers)")],
    },
    "shopflow_returns_summary": {
        "schema": "gold",
        "sql": """
SELECT return_status, return_reason, total_returns, gold_updated_at
FROM gold.returns_summary
""",
        "columns": [
            ("return_status", "STRING", True),
            ("return_reason", "STRING", True),
            ("total_returns", "BIGINT", False),
            ("gold_updated_at", "TIMESTAMP", False),
        ],
        "metrics": [("returns", "SUM(total_returns)")],
    },
    "shopflow_payments_by_period": {
        "schema": "gold",
        "sql": """
SELECT payment_period, payment_type, total_payments, total_value, gold_updated_at
FROM gold.payments_by_period
""",
        "columns": [
            ("payment_period", "TIMESTAMP", False),
            ("payment_type", "STRING", True),
            ("total_payments", "BIGINT", False),
            ("total_value", "DOUBLE", False),
            ("gold_updated_at", "TIMESTAMP", False),
        ],
        "metrics": [
            ("payments", "SUM(total_payments)"),
            ("payment_value", "SUM(total_value)"),
        ],
        "main_dttm_col": "payment_period",
    },
    "shopflow_pipeline_latency": {
        "schema": "gold",
        "sql": """
SELECT table_name, avg_latency_seconds, max_latency_seconds, events_measured, gold_updated_at
FROM gold.pipeline_latency
""",
        "columns": [
            ("table_name", "STRING", True),
            ("avg_latency_seconds", "DOUBLE", False),
            ("max_latency_seconds", "DOUBLE", False),
            ("events_measured", "BIGINT", False),
            ("gold_updated_at", "TIMESTAMP", False),
        ],
        "metrics": [
            ("avg_latency", "AVG(avg_latency_seconds)"),
            ("max_latency", "MAX(max_latency_seconds)"),
            ("events_measured", "SUM(events_measured)"),
        ],
    },
    "shopflow_recent_cdc": {
        "schema": "bronze",
        "sql": """
SELECT
  source_table,
  CASE json_extract_scalar(raw_payload, '$.op')
    WHEN 'c' THEN 'INSERT'
    WHEN 'r' THEN 'SNAPSHOT'
    WHEN 'u' THEN 'UPDATE'
    WHEN 'd' THEN 'DELETE'
    ELSE COALESCE(json_extract_scalar(raw_payload, '$.op'), 'UNKNOWN')
  END AS operation,
  kafka_timestamp,
  bronze_ingested_at
FROM bronze.cdc_events
WHERE raw_payload IS NOT NULL
""",
        "columns": [
            ("source_table", "STRING", True),
            ("operation", "STRING", True),
            ("kafka_timestamp", "TIMESTAMP", False),
            ("bronze_ingested_at", "TIMESTAMP", False),
        ],
        "metrics": [("events", "COUNT(*)")],
        "main_dttm_col": "kafka_timestamp",
    },
}


def metric_ref(metric_name: str) -> dict[str, str]:
    return {"expressionType": "SIMPLE", "column": {"column_name": metric_name}, "aggregate": None, "label": metric_name}


def dataset_ref(dataset: Any) -> str:
    return f"{dataset.id}__table"


def table_params(dataset: Any, columns: list[str], metrics: list[str] | None = None) -> dict[str, Any]:
    return {
        "datasource": dataset_ref(dataset),
        "viz_type": "table",
        "query_mode": "aggregate",
        "groupby": columns,
        "metrics": metrics or [],
        "adhoc_filters": [],
        "row_limit": 100,
        "order_desc": True,
        "color_pn": True,
        "show_cell_bars": True,
        "table_timestamp_format": "smart_date",
    }


def bar_params(dataset: Any, dimension: str, metric: str) -> dict[str, Any]:
    return {
        "datasource": dataset_ref(dataset),
        "viz_type": "dist_bar",
        "metrics": [metric],
        "groupby": [dimension],
        "adhoc_filters": [],
        "row_limit": 50,
        "order_desc": True,
        "color_scheme": "shopflow_react_palette",
        "show_legend": True,
        "show_bar_value": True,
        "y_axis_format": "SMART_NUMBER",
    }


def big_number_params(dataset: Any, metric: str, subtitle: str) -> dict[str, Any]:
    return {
        "datasource": dataset_ref(dataset),
        "viz_type": "big_number_total",
        "metric": metric,
        "adhoc_filters": [],
        "subheader": subtitle,
        "y_axis_format": "SMART_NUMBER",
        "header_font_size": 0.45,
        "subheader_font_size": 0.15,
    }


def upsert_dataset(database: Any, name: str, config: dict[str, Any], owner: Any) -> Any:
    from superset.connectors.sqla.models import SqlaTable, SqlMetric, TableColumn

    dataset = (
        db.session.query(SqlaTable)
        .filter(SqlaTable.table_name == name, SqlaTable.database_id == database.id)
        .one_or_none()
    )
    if dataset is None:
        dataset = SqlaTable(table_name=name, database=database)
        db.session.add(dataset)

    dataset.schema = config["schema"]
    dataset.sql = config["sql"].strip()
    dataset.is_sqllab_view = True
    dataset.main_dttm_col = config.get("main_dttm_col")
    dataset.params = json.dumps({"remote_id": name})
    dataset.owners = [owner]

    existing_columns = {column.column_name: column for column in dataset.columns}
    wanted_columns = set()
    for column_name, column_type, groupby in config["columns"]:
        wanted_columns.add(column_name)
        column = existing_columns.get(column_name) or TableColumn(column_name=column_name)
        column.type = column_type
        column.groupby = groupby
        column.filterable = True
        column.is_dttm = "TIMESTAMP" in column_type
        if column not in dataset.columns:
            dataset.columns.append(column)

    dataset.columns = [column for column in dataset.columns if column.column_name in wanted_columns]

    existing_metrics = {metric.metric_name: metric for metric in dataset.metrics}
    wanted_metrics = set()
    for metric_name, expression in config["metrics"]:
        wanted_metrics.add(metric_name)
        metric = existing_metrics.get(metric_name) or SqlMetric(metric_name=metric_name)
        metric.expression = expression
        metric.metric_type = "sum"
        metric.d3format = "SMART_NUMBER"
        metric.verbose_name = metric_name.replace("_", " ").title()
        if metric not in dataset.metrics:
            dataset.metrics.append(metric)

    dataset.metrics = [metric for metric in dataset.metrics if metric.metric_name in wanted_metrics]
    return dataset


def upsert_chart(name: str, dataset: Any, viz_type: str, params: dict[str, Any], owner: Any) -> Any:
    from superset.models.slice import Slice

    chart = db.session.query(Slice).filter(Slice.slice_name == name).one_or_none()
    if chart is None:
        chart = Slice(slice_name=name)
        db.session.add(chart)

    chart.datasource_id = dataset.id
    chart.datasource_type = "table"
    chart.datasource_name = dataset.table_name
    chart.viz_type = viz_type
    chart.params = json.dumps(params, indent=2)
    chart.query_context = None
    chart.owners = [owner]
    chart.last_saved_at = datetime.now(timezone.utc)
    chart.last_saved_by = owner
    return chart


def dashboard_position(charts: list[Any]) -> str:
    children = []
    position: dict[str, Any] = {
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": children},
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": DASHBOARD_TITLE},
        },
    }

    for index in range(0, len(charts), 3):
        row_id = f"ROW-{index // 3}"
        row_children = []
        children.append(row_id)
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": row_children,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for chart in charts[index : index + 3]:
            chart_id = f"CHART-{chart.id}"
            row_children.append(chart_id)
            position[chart_id] = {
                "type": "CHART",
                "id": chart_id,
                "children": [],
                "meta": {
                    "chartId": chart.id,
                    "height": 50,
                    "width": 4,
                    "sliceName": chart.slice_name,
                },
            }

    return json.dumps(position, indent=2)


app = create_app()

with app.app_context():
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard

    admin = security_manager.find_user(username="admin")
    if admin is None:
        raise RuntimeError("Admin user does not exist. Start Superset once before creating dashboards.")

    database = db.session.query(Database).filter(Database.database_name == DATABASE_NAME).one_or_none()
    if database is None:
        raise RuntimeError(f"Superset database {DATABASE_NAME!r} was not found.")

    datasets = {
        name: upsert_dataset(database, name, config, admin)
        for name, config in DATASETS.items()
    }
    db.session.flush()

    charts = [
        upsert_chart(
            "CDC Events by Operation",
            datasets["shopflow_cdc_operations"],
            "dist_bar",
            bar_params(datasets["shopflow_cdc_operations"], "operation", "events"),
            admin,
        ),
        upsert_chart(
            "Total CDC Events",
            datasets["shopflow_cdc_operations"],
            "big_number_total",
            big_number_params(datasets["shopflow_cdc_operations"], "events", "Captured by Debezium"),
            admin,
        ),
        upsert_chart(
            "Orders by Status",
            datasets["shopflow_orders_by_status"],
            "dist_bar",
            bar_params(datasets["shopflow_orders_by_status"], "order_status", "orders"),
            admin,
        ),
        upsert_chart(
            "Pipeline Latency by Table",
            datasets["shopflow_pipeline_latency"],
            "table",
            table_params(
                datasets["shopflow_pipeline_latency"],
                ["table_name"],
                ["avg_latency", "max_latency", "events_measured"],
            ),
            admin,
        ),
        upsert_chart(
            "Orders by Sales Channel",
            datasets["shopflow_orders_by_channel"],
            "dist_bar",
            bar_params(datasets["shopflow_orders_by_channel"], "sales_channel", "orders"),
            admin,
        ),
        upsert_chart(
            "Customer Segments",
            datasets["shopflow_customer_segments"],
            "dist_bar",
            bar_params(datasets["shopflow_customer_segments"], "customer_segment", "customers"),
            admin,
        ),
        upsert_chart(
            "Returns and Cancellations",
            datasets["shopflow_returns_summary"],
            "table",
            table_params(
                datasets["shopflow_returns_summary"],
                ["return_status", "return_reason"],
                ["returns"],
            ),
            admin,
        ),
        upsert_chart(
            "Payments by Period",
            datasets["shopflow_payments_by_period"],
            "table",
            table_params(
                datasets["shopflow_payments_by_period"],
                ["payment_period", "payment_type"],
                ["payments", "payment_value"],
            ),
            admin,
        ),
        upsert_chart(
            "Recent CDC Events",
            datasets["shopflow_recent_cdc"],
            "table",
            table_params(
                datasets["shopflow_recent_cdc"],
                ["source_table", "operation", "kafka_timestamp", "bronze_ingested_at"],
                [],
            ),
            admin,
        ),
    ]
    db.session.flush()

    dashboard = (
        db.session.query(Dashboard)
        .filter(Dashboard.dashboard_title == DASHBOARD_TITLE)
        .one_or_none()
    )
    if dashboard is None:
        dashboard = Dashboard(dashboard_title=DASHBOARD_TITLE)
        db.session.add(dashboard)

    dashboard.slug = "shopflow-cdc-lakehouse"
    dashboard.published = True
    dashboard.owners = [admin]
    dashboard.slices = charts
    dashboard.position_json = dashboard_position(charts)
    dashboard.json_metadata = json.dumps(
        {
            "color_namespace": "shopflow",
            "label_colors": {},
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "refresh_frequency": 0,
            "default_filters": "{}",
        },
        indent=2,
    )
    dashboard.css = f"""
.dashboard-content {{
  background: #fff7fc;
}}
.dashboard-component-chart-holder {{
  border-radius: 8px;
  border: 1px solid #f0dce7;
  box-shadow: 0 18px 40px rgba(81, 38, 70, 0.08);
}}
.dashboard-header, .dashboard-component-header {{
  color: #2d2437;
}}
a, .ant-tabs-tab-active, .ant-btn-primary {{
  color: {PALETTE[2]};
}}
"""

    db.session.commit()
    print(f"Created/updated dashboard: {DASHBOARD_TITLE}")
    print("URL: /superset/dashboard/shopflow-cdc-lakehouse/")
