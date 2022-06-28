import ee
import pandas as pd
from datetime import datetime

from satellite_retrieval.cloudless import get_s2_sr_cld_col
from areas.geometries import Areas

aoi = Areas.ae
start_date = '2018-12-31'
end_date = '2022-06-20'
collection = get_s2_sr_cld_col(aoi, start_date, end_date)#.filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10))
n_img = 5000
colList = collection.toList(n_img)
n = colList.size().getInfo()
#n=0
df = pd.DataFrame(columns=['timestamp','ingestion_time','B01','B02','B03','B04','B05','B06','B07',
                           'B08','B8A','B09','B11','B12','NDSI','ID'])
for i in range(0,n):
    print('percentage processed: '+str(round(  (i/n)*100,2)))

    now = datetime.now()

    S2_SR_img = ee.Image(colList.get(i))

    id_image = S2_SR_img.get('PRODUCT_ID').getInfo()
    date = id_image.split('_')[2]
    date_yyyymmdd = date[0:4]+'-'+date[4:6]+'-'+date[6:8]
    date_datetime = datetime.strptime(date_yyyymmdd,'%Y-%m-%d')

    stats_paloma_za_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.la_paloma_za).getInfo()

    stats_paloma_zb_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.la_paloma_zb).getInfo()

    stats_juncal_za_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.juncal_za).getInfo()

    stats_juncal_zb_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.juncal_zb).getInfo()

    stats_olivares_za_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.olivares_za).getInfo()

    stats_olivares_zb_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.olivares_zb).getInfo()

    dict_paloma_za_mean = {'ingestion_time':now,'timestamp':date_datetime,'B01':stats_paloma_za_mean['B1'],
        'B02':stats_paloma_za_mean['B2'],'B03':stats_paloma_za_mean['B3'],
        'B04':stats_paloma_za_mean['B4'],'B05':stats_paloma_za_mean['B5'],
        'B06':stats_paloma_za_mean['B6'],'B07':stats_paloma_za_mean['B7'],
        'B08':stats_paloma_za_mean['B8'],'B8A':stats_paloma_za_mean['B8A'],
        'B09':stats_paloma_za_mean['B9'],'B11':stats_paloma_za_mean['B11'],
        'B12':stats_paloma_za_mean['B12'],'NDSI':stats_paloma_za_mean['NDSI'],
        'NDSI_MASK':stats_paloma_za_mean['NDSI_MASK'],'ID':'La Paloma ZA'}

    dict_paloma_zb_mean = {'ingestion_time':now,'timestamp':date_datetime,'B01':stats_paloma_zb_mean['B1'],
                           'B02':stats_paloma_zb_mean['B2'],'B03':stats_paloma_zb_mean['B3'],
                           'B04':stats_paloma_zb_mean['B4'],'B05':stats_paloma_zb_mean['B5'],
                           'B06':stats_paloma_zb_mean['B6'],'B07':stats_paloma_zb_mean['B7'],
                           'B08':stats_paloma_zb_mean['B8'],'B8A':stats_paloma_zb_mean['B8A'],
                           'B09':stats_paloma_zb_mean['B9'],'B11':stats_paloma_zb_mean['B11'],
                           'B12':stats_paloma_zb_mean['B12'],'NDSI':stats_paloma_zb_mean['NDSI'],
                           'NDSI_MASK':stats_paloma_zb_mean['NDSI_MASK'],'ID':'La Paloma ZB'}

    dict_juncal_za_mean = {'ingestion_time':now,'timestamp':date_datetime,'B01':stats_juncal_za_mean['B1'],
                           'B02':stats_juncal_za_mean['B2'],'B03':stats_juncal_za_mean['B3'],
                           'B04':stats_juncal_za_mean['B4'],'B05':stats_juncal_za_mean['B5'],
                           'B06':stats_juncal_za_mean['B6'],'B07':stats_juncal_za_mean['B7'],
                           'B08':stats_juncal_za_mean['B8'],'B8A':stats_juncal_za_mean['B8A'],
                           'B09':stats_juncal_za_mean['B9'],'B11':stats_juncal_za_mean['B11'],
                           'B12':stats_juncal_za_mean['B12'],'NDSI':stats_juncal_za_mean['NDSI'],
                           'NDSI_MASK':stats_juncal_za_mean['NDSI_MASK'],'ID':'Juncal ZA'}

    dict_juncal_zb_mean = {'ingestion_time':now,'timestamp':date_datetime,'B01':stats_juncal_zb_mean['B1'],
                           'B02':stats_juncal_zb_mean['B2'],'B03':stats_juncal_zb_mean['B3'],
                           'B04':stats_juncal_zb_mean['B4'],'B05':stats_juncal_zb_mean['B5'],
                           'B06':stats_juncal_zb_mean['B6'],'B07':stats_juncal_zb_mean['B7'],
                           'B08':stats_juncal_zb_mean['B8'],'B8A':stats_juncal_zb_mean['B8A'],
                           'B09':stats_juncal_zb_mean['B9'],'B11':stats_juncal_zb_mean['B11'],
                           'B12':stats_juncal_zb_mean['B12'],'NDSI':stats_juncal_zb_mean['NDSI'],
                           'NDSI_MASK':stats_juncal_zb_mean['NDSI_MASK'],'ID':'Juncal ZB'}
    dict_olivares_za_mean = {'ingestion_time':now,'timestamp':date_datetime,'B01':stats_olivares_za_mean['B1'],
                           'B02':stats_olivares_za_mean['B2'],'B03':stats_olivares_za_mean['B3'],
                           'B04':stats_olivares_za_mean['B4'],'B05':stats_olivares_za_mean['B5'],
                           'B06':stats_olivares_za_mean['B6'],'B07':stats_olivares_za_mean['B7'],
                           'B08':stats_olivares_za_mean['B8'],'B8A':stats_olivares_za_mean['B8A'],
                           'B09':stats_olivares_za_mean['B9'],'B11':stats_olivares_za_mean['B11'],
                           'B12':stats_olivares_za_mean['B12'],'NDSI':stats_olivares_za_mean['NDSI'],
                           'NDSI_MASK':stats_olivares_za_mean['NDSI_MASK'],'ID':'Olivares ZA'}

    dict_olivares_zb_mean = {'ingestion_time':now,'timestamp':date_datetime,
                             'B01':stats_olivares_zb_mean['B1'],
                       'B02':stats_olivares_zb_mean['B2'],'B03':stats_olivares_zb_mean['B3'],
                       'B04':stats_olivares_zb_mean['B4'],'B05':stats_olivares_zb_mean['B5'],
                       'B06':stats_olivares_zb_mean['B6'],'B07':stats_olivares_zb_mean['B7'],
                       'B08':stats_olivares_zb_mean['B8'],'B8A':stats_olivares_zb_mean['B8A'],
                       'B09':stats_olivares_zb_mean['B9'],'B11':stats_olivares_zb_mean['B11'],
                       'B12':stats_olivares_zb_mean['B12'],'NDSI':stats_olivares_zb_mean['NDSI'],
                       'NDSI_MASK':stats_olivares_zb_mean['NDSI_MASK'],'ID':'Olivares ZB'}

    df = df.append(dict_paloma_za_mean,ignore_index=True)
    df = df.append(dict_paloma_zb_mean,ignore_index=True)
    df = df.append(dict_juncal_za_mean,ignore_index=True)
    df = df.append(dict_juncal_zb_mean,ignore_index=True)
    df = df.append(dict_olivares_za_mean,ignore_index=True)
    df = df.append(dict_olivares_zb_mean,ignore_index=True)
    print(df)
import os
df.to_csv(os.getcwd()+'/Time_Series_BG.csv',index=False)