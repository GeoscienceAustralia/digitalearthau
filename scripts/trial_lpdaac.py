
from datacube import Datacube
from datacube.storage.masking import mask_invalid_data

dc = Datacube(config='/g/data/u46/users/dsg547/dsg547_dev.conf')
dc.list_products()
dc.list_measurements()
data = dc.load(product='modis_lpdaac_MYD13Q1',
               time=('2017-12-12', '2017-12-13'),
               resolution=(-0.1, 0.1),
               measurements=('Evi',))
print (data)

