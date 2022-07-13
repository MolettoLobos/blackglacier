from auth.authGee import *
import ee
import geemap
import os
import geemap.foliumap as geemap

uri_rgb = 'gs://terramodels-prod/raster/BLACK_GLACIER/ANDES/SR/SR_S2SR_T19HCD_2022-06-17.tif'
uri_ndsi = 'gs://terramodels-prod/raster/BLACK_GLACIER/ANDES/NDSI/NDSI_S2SR_T19HCD_2022-06-17.tif'
img_rgb = ee.Image.loadGeoTIFF(uri_rgb)#.mask(0)
img_ndsi = ee.Image.loadGeoTIFF(uri_ndsi)#.mask(0)
img_rgb_mask = img_rgb.mask(img_rgb.select('B8').neq(0))
img_ndsi_mask = img_ndsi.mask(img_rgb.select('B8').neq(0))
# Define the visualization parameters.
vizParams = {
    'bands': ['B11', 'B3', 'B2'],
    'min': 0,
    'max': 10000,
}



Map = geemap.Map(center=[-33.03654,-70.1092534], zoom=13)
#Map.add_basemap('Esri.NatGeoWorldMap')
# Center the map and display the image.
url = 'https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}'
Map.add_tile_layer(url, name='Google Satellite', attribution='Google')
Map.addLayer(img_ndsi_mask,{'min':0,'max':1},'Cobertura de nieve 17-06-2022')
Map.addLayer(img_rgb_mask, vizParams, 'Imagen 17-06-2022 Sentinel-2')
Map.addLayerControl()
Map
Map.to_html(os.getcwd()+'/tmp/test_BG.html')