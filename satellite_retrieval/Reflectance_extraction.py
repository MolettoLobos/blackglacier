import ee
from satellite_retrieval.cloudless import get_s2_sr_cld_col
from areas.geometries import Areas

aoi = Areas.ae
start_date = '2018-12-31'
end_date = '2022-06-20'
collection = get_s2_sr_cld_col(aoi, start_date, end_date).filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10))

n_img = 5000
colList = collection.toList(n_img)
n = colList.size().getInfo()

for i in range(0,n):
    S2_SR_img = ee.Image(colList.get(i))

    id_image = S2_SR_img.get('PRODUCT_ID').getInfo()
    date = id_image.split('_')[2]
    date_yyyymmdd = date[0:4]+'-'+date[4:6]+'-'+date[6:8]

    stats_paloma_za_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.la_paloma_za)

    stats_paloma_zb_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.la_paloma_zb)

    stats_juncal_za_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.juncal_za)

    stats_juncal_zb_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.juncal_zb)

    stats_olivares_za_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.olivares_za)

    stats_juncal_zb_mean = S2_SR_img.reduceRegion(
        reducer= ee.Reducer.mean(),
        bestEffort= True,
        geometry=Areas.olivares_zb)

