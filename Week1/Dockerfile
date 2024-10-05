FROM python:latest

RUN pip install pandas argparse sqlalchemy pyarrow fastparquet psycopg2
RUN apt-get install wget

WORKDIR /app

COPY DataIngestion.py DataIngestion.py

ENTRYPOINT ["python", "DataIngestion.py"]