# Pull base image
FROM python:3.8-alpine3.15
ENV PYBASE /pybase
ENV PYTHONUSERBASE $PYBASE
ENV PATH $PYBASE/bin:$PATH
RUN apk update && apk add bash

WORKDIR /weather_app

# Pip installations
COPY ./requirements.txt requirements.txt
RUN pip install pipenv && pip install -r requirements.txt

# App variables
ENV OPEN_API_KEY e9f153eff4f68eb3705d5a9077b58f1a

# App data
COPY ./weather_app .

# App execution
ENTRYPOINT ["python3", "open_weather.py", "/data/raw"]