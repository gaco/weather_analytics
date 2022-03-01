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
ENV OPEN_API_KEY ce6a01afec4d8d72c298b47d530c37a8

# App data
COPY ./weather_app .

# App execution
ENTRYPOINT ["python3", "open_weather.py", "/data/raw"]