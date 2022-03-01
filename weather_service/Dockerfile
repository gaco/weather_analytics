# Pull base image
FROM python:3.8-alpine3.15
ENV PYBASE /pybase
ENV PYTHONUSERBASE $PYBASE
ENV PATH $PYBASE/bin:$PATH

# Pip Installations
WORKDIR /weather_app
COPY ./requirements.txt requirements.txt
RUN pip install pipenv && pip install -r requirements.txt

# App Variables
ENV OPEN_API_KEY ce6a01afec4d8d72c298b47d530c37a8

# App data
COPY ./weather_app .
ENTRYPOINT ["python3", "open_weather.py", "/data/raw"]