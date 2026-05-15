FROM apache/superset:4.1.1

USER root

RUN pip install --no-cache-dir trino sqlalchemy-trino

COPY superset_config.py /app/pythonpath/superset_config.py
COPY register_trino.py /app/pythonpath/register_trino.py
COPY create_shopflow_dashboard.py /app/pythonpath/create_shopflow_dashboard.py
COPY create_shopflow_saved_queries.py /app/pythonpath/create_shopflow_saved_queries.py
COPY docker-bootstrap.sh /app/docker-bootstrap.sh

RUN chmod +x /app/docker-bootstrap.sh

USER superset

CMD ["/app/docker-bootstrap.sh"]
