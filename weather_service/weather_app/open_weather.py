import calendar
import datetime
import os
import sys
import time
from urllib import request
from geopy import geocoders
from geopy.exc import GeocoderTimedOut

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_cities_coordenates(cities):
    geocoders.options.default_timeout = 10
    geolocator = geocoders.Nominatim(user_agent="Weather Analtyics")

    city_coords = {}
    try:
        for city in cities:
            location = geolocator.geocode(city)
            city_coords[city] = [location.latitude,
                                 location.longitude, location.address]

    except GeocoderTimedOut:
        logger.error("The call to the geocoding service in order to get coordenates for the locations was aborted because no response has been received within 10 seconds.")
        raise
    except Exception:
        logger.error(
            "Some unkown excection happened during the call to the geocoding service in order to get coordenates for the locations.")
        raise
    return city_coords


def call_weather_api(coordenate, output_name, time):
    api = "https://api.openweathermap.org/data/2.5/onecall/timemachine?"
    api_key = os.getenv("OPEN_API_KEY")
    lat, lon = coordenate[0], coordenate[1]
    url = api + \
        f"lat={lat}&lon={lon}&dt={time}&appid={api_key}"
    logger.debug(f"{url}")
    request.urlretrieve(url, output_name)


if __name__ == "__main__":

    # Extract the last 5 days of data from the free API: https://api.openweathermap.org/data/2.5/onecall/timemachine (Historical weather data) from 10 different locations to choose by the candidate.
    locations = ["São Paulo", "Araraquara", "São Carlos", "Ribeirão Bonito",
                 "Ribeirão Preto", "Rio de Janeiro", "Sertãozinho", "Campinas", "Ibaté", "Matão"]

    timestamp = calendar.timegm(time.gmtime())
    utc_datetime = datetime.datetime.fromtimestamp(timestamp)
    raw_data_dir = sys.argv[1]

    location_coords = {}
    try:
        location_coords = get_cities_coordenates(locations)
    except:
        logger.error("Unexpected error:", sys.exc_info()[0])
        exit(1)

    days = 5  # more than 5 days is not allowed by openweather-onecall api
    for t in range(days):
        previous_day = utc_datetime - datetime.timedelta(days=t+1)

        execution_time = utc_datetime.strftime('%Y%m%d_%H%M%S')
        output_dir = f"{raw_data_dir}/{execution_time}/{previous_day.strftime('%Y%m%d_%H%M%S')}/"
        os.makedirs(output_dir, exist_ok=True)

        for location, coordenate in location_coords.items():
            # calling API
            logger.info(
                f"Calling API for location = {location}, day = {previous_day}...")

            # formatted_location = unidecode.unidecode(location.lower().strip().replace(" ", "_"))
            output_name = output_dir + location + ".json"
            call_weather_api(coordenate, output_name,
                             time=int(datetime.datetime.timestamp(previous_day)))
            logger.info(f"Raw data saved into {output_name}")

    logger.info(f"Open Weather APP ended successfuly.")
