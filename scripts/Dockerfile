FROM python:3.8.5-buster
WORKDIR /opt
COPY main.py .
COPY requirements.txt .
RUN apt-get update && apt-get install python3-dev gcc -y && \
    pip install -r requirements.txt

ENTRYPOINT [ "uvicorn", "main:app", "--host", "0.0.0.0"]
