from itertools import product

import xarray as xr

from datacube.virtual import Transformation, Measurement

FC_MEASUREMENTS = [
    {
        'name': 'pv',
        'dtype': 'int8',
        'nodata': -1,
        'units': 'percent'
    },
    {
        'name': 'npv',
        'dtype': 'int8',
        'nodata': -1,
        'units': 'percent'
    },
    {
        'name': 'bs',
        'dtype': 'int8',
        'nodata': -1,
        'units': 'percent'
    },
    {
        'name': 'ue',
        'dtype': 'int8',
        'nodata': -1,
        'units': ''
    },
]


class FractionalCover(Transformation):
    """ Applies the fractional cover algorithm to surface reflectance data.
    Requires bands named 'green', 'red', 'nir', 'swir1', 'swir2'
    """

    def __init__(self, regression_coefficients=None):
        if regression_coefficients is None:
            regression_coefficients = {band: [0, 1]
                for band in ['green', 'red', 'nir', 'swir1', 'swir2']
            }
        self.regression_coefficients = regression_coefficients

    def measurements(self, input_measurements):
        return {m['name']: Measurement(**m) for m in FC_MEASUREMENTS}

    def compute(self, data):
        from fc.fractional_cover import fractional_cover
        # Typically creates a list of dictionaries looking like [{time: 1234}, {time: 1235}, ...]
        sel = [dict(p)
               for p in product(*[[(i.name, i.item()) for i in c]
                                  for v, c in data.coords.items()
                                  if v not in data.geobox.dims])]
        fc = []
        measurements = [Measurement(**m) for m in FC_MEASUREMENTS]
        for s in sel:
            fc.append(fractional_cover(data.sel(**s), measurements, self.regression_coefficients))
        return xr.concat(fc, dim='time')

    def algorithm_metadata(self):
        from fc import __version__
        return {
            'algorithm': {
                'name': 'Fractional Cover',
                'version': __version__,
                'repo_url': 'https://github.com/GeoscienceAustralia/fc.git',
                'parameters': {'regression_coefficients': self.regression_coefficients}
            }}


class FakeFractionalCover(Transformation):
    """ Applies the fractional cover algorithm to surface reflectance data.
    Requires bands named 'green', 'red', 'nir', 'swir1', 'swir2'
    """

    def __init__(self, *args, **kwargs):
        self.output_measurements = {m['name']: Measurement(**m) for m in FC_MEASUREMENTS}

    def measurements(self, input_measurements):
        return self.output_measurements

    def compute(self, data):
        return xr.Dataset({'bs': data.red,
                           'pv': data.green,
                           'npv': data.nir,
                           'ue': data.swir1},
                          attrs=data.attrs)
