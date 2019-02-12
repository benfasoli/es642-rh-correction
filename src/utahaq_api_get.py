#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import pandas as pd
import requests


UTAHAQ_API_BASE_URI = 'http://meso2.chpc.utah.edu/aq/cgi-bin/download_mobile_archive.cgi'
UTAHAQ_API_TOKEN = os.getenv('UTAHAQ_API_TOKEN')


def _utahaq_batch_get(stid: str,
                      yr: int,
                      mo: int,
                      datatype: str) -> pd.DataFrame:
    """Queries UtahAQ API endpoint for single month of data

    For API reference, see
    http://utahaq.chpc.utah.edu/aq/cgi-bin/mobile_archive.cgi

    Args:
        stid: unique station identifier
        yr: year desired
        mo: month desired
        datatype: measurement dataset identifier, see reference

    Returns:
        pd.DataFrame: flattened time, stid, lat/lon, and relevant readings
    """

    yr = str(yr).zfill(4)
    mo = str(mo).zfill(2)
    stid = stid.upper()
    datatype = datatype.lower()

    uri = (
        f'{UTAHAQ_API_BASE_URI}'
        f'?accesskey={UTAHAQ_API_TOKEN}'
        f'&stid={stid}'
        f'&yr={yr}'
        f'&mo={mo}'
        f'&datatype={datatype}'
    )

    try:
        res = pd.read_csv(uri, skiprows=True)
    except pd.errors.EmptyDataError:
        return None

    res = res[res.esampler_error_code == 0]
    res.index = pd.to_datetime(res.Date + ' ' + res.TimeUTC, utc=True)
    res = res.rename(columns={
        'esampler_pm25_ugm3': 'pm25_ugm3',
        'esampler_rh_pcent': 'rh_pct'
    })

    return res[['pm25_ugm3', 'rh_pct']]


def utahaq_api_get(stid: list,
                   start: pd.Timestamp,
                   end: pd.Timestamp,
                   datatype: list) -> pd.DataFrame:
    """Returns `pd.DataFrame` containing observations

    For API reference, see
    http://utahaq.chpc.utah.edu/aq/cgi-bin/mobile_archive.cgi

    Args:
        stid: unique station identifier
        start: start timestamp for returned data
        end: end timestamp for returned data
        datatype: measurement dataset identifier, see reference

    Returns:
        pd.DataFrame: flattened time, stid, lat/lon, and relevant readings

    Examples:
        >>> utahaq_api_get(
                'hawth',
                pd.Timestamp('2019-01-02 00:00:00'),
                pd.Timestamp('2019-01-02 00:00:30'),
                'pm'
            )
                                   pm25_ugm3  rh_pct
        2019-01-02 00:00:00+00:00        3.0    28.0
        2019-01-02 00:00:10+00:00        3.0    28.0
        2019-01-02 00:00:20+00:00        3.0    28.0
        2019-01-02 00:00:30+00:00        2.0    28.0
    """
    query_dates = pd.date_range(start=start, end=end, freq='MS')
    if len(query_dates) == 0:
        query_dates = [start]
    
    df_list = []
    for date in query_dates:
        df_list.append(
            _utahaq_batch_get(
                stid=stid,
                yr=date.year,
                mo=date.month,
                datatype=datatype
            )
        )

    df = pd.concat(df_list)
    return df[(df.index >= start) & (df.index <= end)]

