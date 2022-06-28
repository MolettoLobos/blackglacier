import ee

def export_image(image_export,path_gcloud,vector_export,scale=10,table='prod'):
    name_description = path_gcloud.replace('/','')
    export = ee.batch.Export.image.toCloudStorage(
        image=image_export,
        bucket='terramodels-' + table,
        description=name_description,
        fileNamePrefix=path_gcloud,
        maxPixels=1e12,
        region=vector_export,
        scale=scale,
        crs='EPSG:4326'
    )
    return export
