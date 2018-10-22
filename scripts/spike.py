from datacube.utils.geometry import CRS, box


coord = {
        "coord": {
            "ll": {
                "lat": -3335851.559,
                "lon": 12231455.716333
            },
            "lr": {
                "lat": -3335851.559,
                "lon": 13343406.236
            },
            "ul": {
                "lat": -2223901.039333,
                "lon": 12231455.716333
            },
            "ur": {
                "lat": -2223901.039333,
                "lon": 13343406.236
            }
        }
}

spatial_reference = 'PROJCS["Sinusoidal",GEOGCS["GCS_Undefined",DATUM["D_Undefined",SPHEROID["User_Defined_Spheroid",6371007.181,0.0]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.017453292519943295]],PROJECTION["Sinusoidal"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],UNIT["Meter",1.0]]'
left, bottom, right, top = 12231455.716333, -3335851.559, 13343406.236, -2223901.039333
# assuming http://spatialreference.org/ref/sr-org/6842/

crs_dic = {'proj': 'sinu', 'lon_0': 0, 'x_0': 0, 'y_0': 0, 'a': 6371007.181, 'b': 6371007.181, 'units': 'm', 'no_defs': True}
#crs = CRS(crs_dic)
crs = CRS(spatial_reference)

box = box(left, bottom, right, top, crs)
print('box.boundingbox')
print(box.boundingbox)
projected = box.to_crs(CRS("EPSG:4326"))
print('projected.boundingbox)')
print(projected.boundingbox)
pleft, pright, pbottom, ptop = projected.boundingbox
print('* projected ***')
print('pleft')
print(pleft)
print('pbottom')
print(pbottom)