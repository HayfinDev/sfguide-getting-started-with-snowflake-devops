# Views to transform marketplace data in pipeline

import os

from snowflake.core import Root, CreateMode
from snowflake.snowpark import Session
from snowflake.core.user_defined_function import (
    Argument,
    ReturnDataType,
    PythonFunction,
    UserDefinedFunction,
)
from snowflake.core.view import View, ViewColumn


"""
To join the flight and location focused tables
we need to cross the gap between the airport and cities domains.
For this we make use of a Snowpark Python UDF.
What's really cool is that Snowpark allows us to define a vectorized UDF
making the processing super efficient as we don’t have to invoke the
function on each row individually!

To compute the mapping between airports and cities,
we use SnowflakeFile to read a JSON list from the pyairports package.
The SnowflakeFile class provides dynamic file access, to stream files of any size.
"""
map_city_to_airport = UserDefinedFunction(
    name="get_city_for_airport",
    arguments=[Argument(name="iata", datatype="VARCHAR")],
    return_type=ReturnDataType(datatype="VARCHAR"),
    language_config=PythonFunction(
        runtime_version="3.11", packages=["snowflake-snowpark-python"], handler="main"
    ),
    body="""
from snowflake.snowpark.files import SnowflakeFile
from _snowflake import vectorized
import pandas
import json

@vectorized(input=pandas.DataFrame)
def main(df):
    airport_list = json.loads(
        SnowflakeFile.open("@bronze.raw/airport_list.json", "r", require_scoped_url=False).read()
    )
    airports = {airport[3]: airport[1] for airport in airport_list}
    return df[0].apply(lambda iata: airports.get(iata.upper()))
""",
)


"""
To mangle the data into a more usable form,
we make use of views to not materialize the marketplace data
and avoid the corresponding storage costs.
"""

pipeline = [
    View(
        name="flight_emissions",
        columns=[
            ViewColumn(name="departure_airport"),
            ViewColumn(name="arrival_airport"),
            ViewColumn(name="co2_emissions_kg_per_person"),
        ],
        query="""
        select
            departure_airport,
            arrival_airport,
            avg(estimated_co2_total_tonnes / seats) * 1000 as co2_emissions_kg_per_person
        from oag_flight_emissions_data_sample.public.estimated_emissions_schedules_sample
        where seats != 0 and estimated_co2_total_tonnes is not null
        group by departure_airport, arrival_airport
        """,
    ),
    View(
        name="flight_punctuality",
        columns=[
            ViewColumn(name="departure_iata_airport_code"),
            ViewColumn(name="arrival_iata_airport_code"),
            ViewColumn(name="punctual_pct"),
        ],
        query="""
        select
            departure_iata_airport_code,
            arrival_iata_airport_code,
            count(case when arrival_actual_ingate_timeliness in ('OnTime', 'Early') then 1 end) / count(*) * 100 as punctual_pct
        from oag_flight_status_data_sample.public.flight_status_latest_sample
        where arrival_actual_ingate_timeliness is not null
        group by departure_iata_airport_code, arrival_iata_airport_code
        """,
    ),
    View(
        name="flights_from_home",
        columns=[
            ViewColumn(name="departure_airport"),
            ViewColumn(name="arrival_airport"),
            ViewColumn(name="arrival_city"),
            ViewColumn(name="co2_emissions_kg_per_person"),
            ViewColumn(name="punctual_pct"),
        ],
        query="""
        select
            departure_airport,
            arrival_airport,
            get_city_for_airport(arrival_airport) as arrival_city,
            co2_emissions_kg_per_person,
            punctual_pct
        from flight_emissions
        join flight_punctuality
            on departure_airport = departure_iata_airport_code
           and arrival_airport = arrival_iata_airport_code
        where departure_airport = (
            select $1:airport
            from @quickstart_common.public.quickstart_repo/branches/main/data/home.json
                (file_format => bronze.json_format)
        )
        """,
    ),
    View(
        name="weather_forecast",
        columns=[
            ViewColumn(name="postal_code"),
            ViewColumn(name="avg_temperature_air_f"),
            ViewColumn(name="avg_relative_humidity_pct"),
            ViewColumn(name="avg_cloud_cover_pct"),
            ViewColumn(name="precipitation_probability_pct"),
        ],
        query="""
        select
            postal_code,
            avg(avg_temperature_air_2m_f) as avg_temperature_air_f,
            avg(avg_humidity_relative_2m_pct) as avg_relative_humidity_pct,
            avg(avg_cloud_cover_tot_pct) as avg_cloud_cover_pct,
            avg(probability_of_precipitation_pct) as precipitation_probability_pct
        from global_weather__climate_data_for_bi.standard_tile.forecast_day
        where country = 'US'
        group by postal_code
        """,
    ),
    View(
        name="major_us_cities",
        columns=[
            ViewColumn(name="geo_id"),
            ViewColumn(name="geo_name"),
            ViewColumn(name="total_population"),
        ],
        query="""
        select
            geo.geo_id,
            geo.geo_name,
            max(ts.value) as total_population
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.DATACOMMONS_TIMESERIES ts
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_INDEX geo
            on ts.geo_id = geo.geo_id
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS geo_rel
            on geo_rel.related_geo_id = geo.geo_id
        where true
            and ts.variable_name = 'Total Population, census.gov'
            and date >= '2020-01-01'
            and geo.level = 'City'
            and geo_rel.geo_id = 'country/USA'
            and value > 100000
        group by geo.geo_id, geo.geo_name
        order by total_population desc
        """,
    ),

    # NEW: Attractions view (integrated into pipeline)
    View(
        name="attractions",
        columns=[
            ViewColumn(name="geo_id"),
            ViewColumn(name="geo_name"),
            ViewColumn(name="aquarium_cnt"),
            ViewColumn(name="zoo_cnt"),
            ViewColumn(name="korean_restaurant_cnt"),
        ],
        query="""
        select
            city.geo_id,
            city.geo_name,
            count(case when poi.category_main = 'Aquarium' then 1 end) as aquarium_cnt,
            count(case when poi.category_main = 'Zoo' then 1 end) as zoo_cnt,
            count(case when poi.category_main = 'Korean Restaurant' then 1 end) as korean_restaurant_cnt
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.POINT_OF_INTEREST_INDEX poi
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.POINT_OF_INTEREST_ADDRESSES_RELATIONSHIPS poi_add
            on poi_add.poi_id = poi.poi_id
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.US_ADDRESSES address
            on address.address_id = poi_add.address_id
        join major_us_cities city
            on city.geo_id = address.id_city
        where true
            and poi.category_main in ('Aquarium', 'Zoo', 'Korean Restaurant')
            and address.id_country = 'country/USA'
        group by city.geo_id, city.geo_name
        """,
    ),

    View(
        name="zip_codes_in_city",
        columns=[
            ViewColumn(name="city_geo_id"),
            ViewColumn(name="city_geo_name"),
            ViewColumn(name="zip_geo_id"),
            ViewColumn(name="zip_geo_name"),
        ],
        query="""
        select
            city.geo_id as city_geo_id,
            city.geo_name as city_geo_name,
            city.related_geo_id as zip_geo_id,
            city.related_geo_name as zip_geo_name
        from SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS country
        join SNOWFLAKE_PUBLIC_DATA_FREE.PUBLIC_DATA_FREE.GEOGRAPHY_RELATIONSHIPS city
            on country.related_geo_id = city.geo_id
        where true
            and country.geo_id = 'country/USA'
            and city.level = 'City'
            and city.related_level = 'CensusZipCodeTabulationArea'
        order by city_geo_id
        """,
    ),
    View(
        name="weather_joined_with_major_cities",
        columns=[
            ViewColumn(name="geo_id"),
            ViewColumn(name="geo_name"),
            ViewColumn(name="total_population"),
            ViewColumn(name="avg_temperature_air_f"),
            ViewColumn(name="avg_relative_humidity_pct"),
            ViewColumn(name="avg_cloud_cover_pct"),
            ViewColumn(name="precipitation_probability_pct"),
        ],
        query="""
        select
            city.geo_id,
            city.geo_name,
            city.total_population,
            avg(weather.avg_temperature_air_f) as avg_temperature_air_f,
            avg(weather.avg_relative_humidity_pct) as avg_relative_humidity_pct,
            avg(weather.avg_cloud_cover_pct) as avg_cloud_cover_pct,
            avg(weather.precipitation_probability_pct) as precipitation_probability_pct
        from major_us_cities city
        join zip_codes_in_city zip
            on city.geo_id = zip.city_geo_id
        join weather_forecast weather
            on zip.zip_geo_name = weather.postal_code
        group by city.geo_id, city.geo_name, city.total_population
        """,
    ),
    # Placeholder: Add new view definition here
]


def deploy(session: Session) -> None:
    """
    Entry point for Snowflake-executed environments (procedures / SnowCLI temp procedures).
    IMPORTANT: Do NOT create a new Session here. Reuse the provided one.
    """
    root = Root(session)

    env = os.environ.get("environment")
    if not env:
        raise ValueError("Missing required environment variable: 'environment'")

    silver_schema = root.databases[f"quickstart_{env}"].schemas["silver"]

    silver_schema.user_defined_functions.create(
        map_city_to_airport, mode=CreateMode.or_replace
    )

    for view in pipeline:
        silver_schema.views.create(view, mode=CreateMode.or_replace)


# Local/CI execution (optional)
if __name__ == "__main__":
    session = Session.builder.create()
    try:
        deploy(session)
    finally:
        session.close()
