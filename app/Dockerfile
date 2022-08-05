FROM python:latest
WORKDIR /app
ADD data data
ADD wait-for-postgres.sh wait-for-postgres.sh
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY app.py app.py
# RUN ["chmod", "+x", "wait-for-postgres.sh"]
CMD ["python", "-u", "app.py"]

