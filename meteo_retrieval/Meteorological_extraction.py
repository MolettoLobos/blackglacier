from meteo_retrieval.stormglass_connection import get_full_df, rn_estimation, calculate_daily_stats
from areas.geometries import Areas
import os
import pandas as pd

la_paloma_za = Areas.la_paloma_za.centroid().getInfo()
la_paloma_zb = Areas.la_paloma_zb.centroid().getInfo()

juncal_za = Areas.juncal_za.centroid().getInfo()
juncal_zb = Areas.juncal_zb.centroid().getInfo()

olivares_za = Areas.olivares_za.centroid().getInfo()
olivares_zb = Areas.olivares_zb.centroid().getInfo()

lat_lon = (la_paloma_za['coordinates'][1],\
          la_paloma_za['coordinates'][0])

df_paloma_za = get_full_df(lat_lon,'La Paloma ZA')
df_paloma_za_rn = rn_estimation(df_paloma_za,lat_lon[0],lat_lon[1])
df_paloma_za_daily = calculate_daily_stats(df_paloma_za_rn)

lat_lon = (la_paloma_zb['coordinates'][1], \
           la_paloma_zb['coordinates'][0])

#df_paloma_zb = get_full_df(lat_lon,'La Paloma ZB')
#df_paloma_zb_rn = rn_estimation(df_paloma_zb,lat_lon[0],lat_lon[1])
#df_paloma_zb_daily = calculate_daily_stats(df_paloma_zb_rn)


lat_lon = (juncal_za['coordinates'][1], \
           juncal_za['coordinates'][0])

df_juncal_za = get_full_df(lat_lon,'Juncal ZA')
df_juncal_za_rn = rn_estimation(df_juncal_za,lat_lon[0],lat_lon[1])
df_juncal_za_daily = calculate_daily_stats(df_juncal_za_rn)

"""
lat_lon = (juncal_zb['coordinates'][1], \
           juncal_zb['coordinates'][0])

df_juncal_zb = get_full_df(lat_lon,'Juncal ZB')
df_juncal_zb_rn = rn_estimation(df_juncal_zb,lat_lon[0],lat_lon[1])
df_juncal_zb_daily = calculate_daily_stats(df_juncal_zb_rn)
"""

lat_lon = (olivares_zb['coordinates'][1], \
           olivares_zb['coordinates'][0])

df_olivares_za = get_full_df(lat_lon,'Olivares ZA')
df_olivares_za_rn = rn_estimation(df_olivares_za,lat_lon[0],lat_lon[1])
df_olivares_za_daily = calculate_daily_stats(df_olivares_za_rn)

"""
lat_lon = (olivares_zb['coordinates'][1], \
           olivares_zb['coordinates'][0])

df_olivares_zb = get_full_df(lat_lon,'Olivares ZB')
df_olivares_zb_rn = rn_estimation(df_olivares_zb,lat_lon[0],lat_lon[1])
df_olivares_zb_daily = calculate_daily_stats(df_olivares_zb_rn)
"""
df = pd.concat([df_paloma_za_rn,
                    df_juncal_za_rn,
                   df_olivares_za_rn])

df_daily = pd.concat([df_paloma_za_daily,
                df_juncal_za_daily,
                df_olivares_za_daily])


df.to_csv(os.getcwd()+'/TS_METEO_BG_HOURLY.csv',index=False)
df_daily.to_csv(os.getcwd()+'/TS_METEO_BG_DAILY.csv',index=False)