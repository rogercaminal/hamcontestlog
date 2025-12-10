import pandas as pd
from datetime import datetime
from pyhamtools.locator import latlong_to_locator, calculate_sunrise_sunset
from hamcontestlog.utils import get_call_info


call_info = get_call_info()


def get_geo(df: pd.DataFrame, call_column_name: str) -> pd.DataFrame:
    assert call_column_name in df.columns
    geo_df = (
        df
        .apply(lambda x: call_info.get_all(x["call"]), axis=1)
        .apply(pd.Series)
    )
    return geo_df


def get_locator(df: pd.DataFrame, call_column_name: str) -> pd.DataFrame:
    assert call_column_name in df.columns
    locator_df = (
        df
        .apply(lambda x: latlong_to_locator(**call_info.get_lat_long(x["call"])), axis=1)
        .apply(pd.Series)
        .rename(columns={0: "locator"})
    )
    return locator_df


def get_sunrise_sunset(df: pd.DataFrame, reference_date: datetime) -> pd.DataFrame:
    assert "locator" in df.columns
    sunrise_sunset_df = (
        df
        .apply(lambda x: calculate_sunrise_sunset(x["locator"], reference_date), axis=1)
        .apply(pd.Series)
    )
    return sunrise_sunset_df
