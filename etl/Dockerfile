FROM python:latest
WORKDIR /etl
ADD data data
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY etl.py etl.py
CMD ["python", "-u", "etl.py"]

