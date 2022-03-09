# weather_analytics

Read Weather Data from last 5 days and generate temperature metrics from it.

1. Extract the last 5 days of data from the free API: https://api.openweathermap.org/data/2.5/onecall/timemachine (Historical weather data) from 10 different locations.

2. Build a repository of data where we will keep the data extracted from the API. This repository should only have deduplicated data. Idempotency should also be guaranteed.

3. Build another repository of data that will contain the results of the following calculations from the data stored in step 2.

    - A dataset containing the location, date and temperature of the highest temperatures reported by location and month.
    - A dataset containing the average temperature, min temperature, location of min temperature, and location of max temperature per day.
