FROM apache/airflow:2.5.2
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install apache-airflow-providers-snowflake
RUN pip install pyarrow
RUN pip install snowflake-connector-python
RUN pip install snowflake-sqlalchemy
RUN pip install SQLAlchemy