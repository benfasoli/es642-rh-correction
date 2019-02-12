#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import pandas as pd
import requests


SYNOPTIC_API_BASE_URI = 'https://api.synopticdata.com/v2/stations/timeseries'
SYNOPTIC_API_TOKEN = os.getenv('SYNOPTIC_API_TOKEN')


def synoptic_api_get(stid: list,
                     start: pd.Timestamp,
                     end: pd.Timestamp,
                     api_vars: list = None) -> pd.DataFrame:
    """Returns `pd.DataFrame` containing observations

    For API reference, see
    https://developers.synopticdata.com/mesonet/v2/getting-started/

    Args:
        stid: unique station identifier(s)
        start: timeseries start timestamp or str formatted `YYYYMMDDHHmm`
        end: timeseries end timestamp or str formatted `YYYYMMDDHHmm`
        api_vars: variable codes for desired data, defaults to all

    Returns:
        pd.DataFrame: flattened time, stid, lat/lon, and relevant readings

    Examples:
        >>> synoptic_api_get('kslc', '201901010000', '201901010010', 'air_temp')
                               date  stid  latitude   longitude  air_temp_set_1
        0 2019-01-01 00:00:00+00:00  KSLC  40.77069  -111.96503            -5.0
        1 2019-01-01 00:05:00+00:00  KSLC  40.77069  -111.96503            -4.0
        2 2019-01-01 00:10:00+00:00  KSLC  40.77069  -111.96503            -4.0
    """
    if type(stid) is list:
        stid = ','.join(stid)

    if type(api_vars) is list:
        api_vars = ','.join(api_vars)

    if type(start) is pd.Timestamp:
        start = start.strftime('%Y%m%d%H%M')

    if type(end) is pd.Timestamp:
        end = end.strftime('%Y%m%d%H%M')

    params = {
        'token': SYNOPTIC_API_TOKEN,
        'stid': stid,
        'start': start,
        'end': end,
        'vars': api_vars
    }
    res = requests.get(SYNOPTIC_API_BASE_URI, params=params).json()

    # SynopticLabs API returns a response code in the response body rather than
    # including standard status codes in the http header. The API always
    # returns a 200 code rather than 5xx server error codes and instead uses a
    # field in the response body to signal a successful operation.
    if res['SUMMARY']['RESPONSE_CODE'] in [-1, 200]:
        raise OSError('SYNOPTIC_API_TOKEN environment variable invalid.')
    if res['SUMMARY']['RESPONSE_CODE'] == 2:
        raise ValueError('No data found. Try a different stid or time range.')

    df_list = []
    for station in res['STATION']:
        obs = station['OBSERVATIONS']
        date = pd.to_datetime(obs['date_time'], utc=True)
        _ = obs.pop('date_time', None)
        df_list.append(
            pd.DataFrame({
                'date': date,
                'stid': str(station['STID']),
                'latitude': float(station['LATITUDE']),
                'longitude': float(station['LONGITUDE']),
                **obs
            })
        )
    return pd.concat(df_list, ignore_index=True).set_index('date')
