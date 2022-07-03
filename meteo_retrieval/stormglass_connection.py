import pandas as pd
import numpy as np
import math
from datetime import datetime, timedelta
import arrow
import requests
import pvlib

def get_solar_radiation(date_start, last_api_date, lng, lat, pres, elevation):
    # obj = TimezoneFinder()
    # tz_location = obj.timezone_at(lng=lng, lat=lat)
    tz_location = 'UTC'
    times = pd.date_range(start=date_start, end=last_api_date, freq='1h', tz=tz_location)
    # clear sky solar irradiation calculation
    solpos = pvlib.solarposition.get_solarposition(times, lat, lng)
    airmass = pvlib.atmosphere.get_relative_airmass(solpos['apparent_zenith'])
    dni_extra = pvlib.irradiance.get_extra_radiation(times)
    am_abs = pvlib.atmosphere.get_absolute_airmass(airmass, pres)
    tl = pvlib.clearsky.lookup_linke_turbidity(times, lat, lng)
    cs = pvlib.clearsky.ineichen(solpos['apparent_zenith'], am_abs, tl,
                                 dni_extra=dni_extra, altitude=elevation)
    cs = cs.reset_index()
    return cs


def get_meteo_sg(date_start='2021-06-01',
                 date_end='2021-10-01',
                 lat=-33.40,
                 lng=-70.80,
                 elevation=100.0,
                 api_key='abe46d92-2575-11ec-9b0e-0242ac130002-abe46e14-2575-11ec-9b0e-0242ac130002'):
    """
    API	data from
    Weather	2017-01-01
    Tide	2018-03-01
    Solar	2021-06-01
    :param date_start: YYYY-mm-dd (str)
    :param date_end:  YYYYY-mm-dd (str)
    :param lat: -xx.xx (float)
    :param lng: -xx.xx (float)
    :param api_key: (str)
    :return:
    variables
    ● Temperatura promedio aire
    ● Precipitación horaria
    ● Humedad relativa promedio
    ● Radiación solar máxima
    ● Velocidad máxima viento
    ● Temperatura mínima
    ● Temperatura máxima
    ● Grados día (base 10)
    ● Horas frio (base 7)
    """
    start = arrow.get(date_start)
    end = arrow.get(date_end)
    response = requests.get(
        'https://api.stormglass.io/v2/weather/point',
        params={
            'lat': lat,
            'lng': lng,
            'start': start.to('UTC').timestamp(),
            'end': end.to('UTC').timestamp(),
            'params': ','.join(['airTemperature',
                                'precipitation',
                                'humidity',
                                'gust',
                                'windSpeed',
                                'windDirection',
                                'pressure'])
        },
        headers={
            'Authorization': api_key
        }
    )

    response_solar = requests.get(
        'https://api.stormglass.io/v2/solar/point',
        params={
            'lat': lat,
            'lng': lng,
            'start': start.to('UTC').timestamp(),
            'end': end.to('UTC').timestamp(),
            'params': ','.join(['downwardShortWaveRadiationFlux'])
        },
        headers={
            'Authorization': api_key
        })

    json_data = response.json()
    json_data_solar = response_solar.json()
    try:
        json_data_hourly = json_data['hours']
        json_data_solar_hourly = json_data_solar['hours']
        last_api_date = json_data['hours'][len(json_data['hours']) - 1]['time'][0:10]
    except KeyError:
        #logger = prefect.context.get("logger")
        #logger.warning('Error in json retrievals, the message is: '+str(json_data))
        return

    meteovars = list(json_data_hourly[0].keys())
    meteovars.append('downwardShortWaveRadiationFlux')
    # create a df of solar radiation backup for dates when sg doesn't
    # have available information
    try:
        pres = json_data_hourly[0]['pressure']['sg']
    except KeyError:
        pres = 1030.0
    solar_rad_backup = get_solar_radiation(
        date_start,
        last_api_date,
        lng,
        lat,
        pres,
        elevation)
    if len(json_data_hourly) != len(solar_rad_backup):
        print('cambio de horario detectado')
        solar_rad_backup.loc[len(solar_rad_backup)] = [0, 0, 0, 0]
    i = -1
    for dates in json_data_hourly:
        i = i + 1
        l = []
        for meteovar in meteovars:
            if meteovar == 'time':
                l.append(dates[meteovar])
            elif meteovar == 'downwardShortWaveRadiationFlux':
                try:
                    l.append(float(json_data_solar_hourly[i][meteovar]['sg']))
                except (KeyError, IndexError):
                    l.append(solar_rad_backup['ghi'][i])

            else:
                try:
                    l.append(float(dates[meteovar]['sg']))
                except KeyError:
                    # print('no data on sg found')
                    l.append(np.nan)
        array = np.array([l])
        try:
            df
            df0 = pd.DataFrame(array, columns=meteovars)
            df = df.append(df0)
        except NameError:
            df = pd.DataFrame(array, columns=meteovars)
    # json_data_solar_hourly = json_data_solar['hours']
    df['time'] = pd.to_datetime(df['time'])
    return df


def get_elevation(lat, lng, api_key='abe46d92-2575-11ec-9b0e-0242ac130002-abe46e14-2575-11ec-9b0e-0242ac130002'):
    response = requests.get(
        'https://api.stormglass.io/v2/elevation/point',
        params={
            'lat': lat,
            'lng': lng,
        },
        headers={
            'Authorization': api_key
        }
    )
    json_data = response.json()
    try:
        elevation = json_data['data']['elevation']
    except KeyError:
        #logger = prefect.context.get("logger")
        #logger.warning('Error in json retrievals, the message is: '+str(json_data))
        return
    return elevation


def get_full_df(lat_lng, station):
    #last_df_gbq = query_lastdatetime_station(station, table='virtual_stations')
    last_df_gbq = pd.DataFrame(data={'timestamp':[datetime.strptime('2019-01-01','%Y-%m-%d')]})

    if len(last_df_gbq) == 1:
        date_start = last_df_gbq['timestamp'][0] + timedelta(hours=1)
        print('farm '+station+' DETECTED, last datetime is: ' + str(date_start - timedelta(hours=1)))
    elif len(last_df_gbq) == 0:
        date_start = pd.to_datetime('2019-01-01')
        print('farm '+station+' DONT DETECTED, date_start is: ' + str(date_start))

    date_end = pd.to_datetime(datetime.utcnow()).floor(freq='H')
    last_date = date_start
    elevation = get_elevation(lat_lng[0], lat_lng[1])

    l_last_date = []
    i = -1
    while last_date < date_end:
        i += 1
        print(last_date)
        try:
            df
            df0 = get_meteo_sg(last_date, date_end, lat_lng[0], lat_lng[1], elevation)
            if df0 is None:
                break
            df = df.append(df0)
        except NameError:
            df = get_meteo_sg(last_date, date_end, lat_lng[0], lat_lng[1], elevation)
            if df is None:
                break
        except IndexError:
            last_date = last_date + timedelta(days=1)
            l_last_date.append(last_date)
            print('Data not detected in ' + str(last_date)+' API')
            continue

        last_date = pd.to_datetime(df['time'].max().strftime('%Y-%m-%d %H'))
        l_last_date.append(last_date)
        if len(l_last_date) > 1:
            if l_last_date[i] == l_last_date[i-1]:
                last_date = last_date + timedelta(days=1)
                print('Data not detected in '+str(last_date))

        # time.sleep(1)

    try:
        df
        df = df.rename(columns={'time': 'timestamp',
                                'airTemperature': 'TA',
                                'precipitation': 'PP',
                                'humidity': 'HR',
                                'downwardShortWaveRadiationFlux': 'RG',
                                'windSpeed': 'WS',
                                'windDirection': 'WD',
                                'gust': 'WS_MAX'})
        df['TA'] = df['TA'].astype(np.float64)
        df['PP'] = df['PP'].astype(np.float64)
        df['HR'] = df['HR'].astype(np.float64)
        df['RG'] = df['RG'].astype(np.float64)
        df['WS'] = df['WS'].astype(np.float64)
        df['WD'] = df['WD'].astype(np.float64)
        df['WS_MAX'] = df['WS_MAX'].astype(np.float64)
        df['ingestion_time'] = datetime.now()
        df['source'] = 'sg'
        df['station'] = station
        df = df.drop(columns=['pressure'])
    except (NameError, AttributeError):
        df = pd.DataFrame()
        #logger = prefect.context.get("logger")
        #logger.warning("StormGlass API quota exceeded.")
    return df

def rn_estimation(df2, lat, long):
    """
    This function is to estimate RN with PM equation. It can change according to the sensors that each station has.
    :param df2:
    :param lat:
    :param long:
    :param z:
    :return:
    """
    z = get_elevation(lat, long)
    RHD = df2['HR']
    TaK = df2['TA'] + 273.15
    es = 6.122*np.exp((17.67*(TaK-273.15))/((TaK-273.15)+243.5)) #[HPa]
    df2['VPS'] = es/10 #[Kpa]
    df2['VP'] = (es*RHD)/100 #[Kpa]
    df2['VPD'] = df2['VP'] - df2['VPS']
    # RN
    boltz = 2.043 * 10 ** (-10)  # MJ m-2 hr-1
    LATi = lat * math.pi / 180
    df2['DOY'] = df2['timestamp'].dt.strftime('%j').astype('float64')
    dr = 1 + 0.033 * (np.cos((2 * math.pi * df2['DOY']) / 365))
    delt = 0.409 * np.sin(((2 * math.pi * df2['DOY']) / 356) - 1.39)
    #df2['timestamp_chile'] = df2.timestamp - datetime.timedelta(hours=5)
    df2['t'] = df2.timestamp.dt.strftime('%H').astype('float64') - 0.5
    Lz = 75  # longitud central de la zona horaria
    b = (2 * math.pi * (df2['DOY'] - 81)) / 364
    Sc = 0.1645 * np.sin(2 * b) - 0.1255 * np.cos(b) - 0.025 * np.sin(b)
    w = (math.pi / 12) * (df2.t + 0.06667 * (Lz - round(-long) + Sc) - 12)
    w1 = w - (math.pi / 24)
    w2 = w + (math.pi / 24)
    ra = ((12 * 60) / math.pi) * 0.082 * dr * ((w2 - w1) * np.sin(LATi) * np.sin(delt) + np.cos(LATi) * np.cos(delt) * (np.sin(w2) - np.sin(w1)))
    df2['sup_mj'] = df2.RG * 0.0036  # SUP en MJ/hr
    df2['rso'] = (0.75 + (z * 0.00002)) * ra
    df2.loc[df2.rso < 0, 'rso'] = 0
    df2['RN'] = (1 - 0.23) * df2.sup_mj - (boltz * ((df2.TA + 273.16) ** 4) * (0.34 - 0.14 * (df2['VP'] ** (1 / 2))) * (1.35 * (df2.sup_mj / df2.rso - 0.35)))
    df2['RN'] = df2['RN']/0.0036 # [W/m2]
    df2 = df2.drop(['VP', 'VPD', 'DOY', 't', 'VPS', 'sup_mj', 'rso'], axis=1)
    df2.reset_index(drop=True, inplace=True)
    df2.replace([np.inf, -np.inf], np.nan, inplace=True)
    df2['RN'] = df2['RN'].interpolate(method='slinear')#,order=5)
    #df2['RN'] = np.where(df2.RN < 0, 0, df2.RN)
    return df2

def calculate_daily_stats(df):
    df = df.set_index(df['timestamp'])
    df_d_mean = df.resample('1D').mean()
    df_d_max = df.resample('1D').max()
    df_d_min = df.resample('1D').min()
    df_d_sd = df.resample('1D').std()

    df_d_mean['TX'] = df_d_min['TA']
    df_d_mean['TN'] = df_d_min['TA']
    df_d_mean['RG_SD'] = df_d_sd['RG']
    df_d_mean['RN_SD'] = df_d_sd['RN']
    df_d_mean = df_d_mean.drop(['WS_MAX', 'PP'], axis=1)
    df_d_mean = df_d_mean.reset_index()
    df_d_mean['station'] = df['station'][df['station'].index[0]]
    return df_d_mean