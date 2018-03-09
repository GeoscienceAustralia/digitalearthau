
.. _Surface Reflectance:

Surface Reflectance
*******************

NBAR
====

Summary
-------
**Surface Reflectance (SR)** is a suite of **Earth Observation (EO)** products from GA.
The SR product suite provides standardised optical surface reflectance datasets using robust physical models to correct
for variations in image radiance values due to atmospheric properties, and sun and sensor geometry.
The resulting stack of surface reflectance grids are consistent over space and time which is instrumental in identifying
and quantifying environmental change. SR is based on radiance data from the Landsat-5 TM, Landsat-7 ETM+ and Landsat-8 OLI/TIRS sensors.

NBAR stands for **Nadir-corrected BRDF Adjusted Reflectance**, where BRDF stands for **Bidirectional reflectance distribution function**
The approach involves atmospheric correction to compute surface-leaving radiance, and bi-directional reflectance modelling to remove the effects of
topography and angular variation in reflectance.

Features
---------
* The standardised SR data products deliver calibrated optical surface reflectance data across land and coastal fringes.
  SR is a medium resolution (~25 m) grid based on the Landsat TM/ETM+/OLI archive and presents surface reflectance data in 25 square metre grid cells.

* Radiance measurements from EO sensors do not directly quantify the surface reflectance of the Earth. Such measurements are modified by variations in atmospheric
  properties, sun position, sensor view angle, surface slope and surface aspect.
  To obtain consistent and comparable measures of Earth surface reflectance from EO,these variations need to be reduced or removed from the radiance measurements (Li et al., 2010).
  This is especially important when comparing imagery acquired in different seasons and geographic regions.

* The SR product is created using a physics-based, coupled BRDF and atmospheric correction model that can be applied to both flat and inclined surfaces (Li et al., 2012).
  The resulting surface reflectance values are comparable both within individual images and between images acquired at different times and/or with different sensors.

.. _table:

It has the following bands:


======  =====  ========================
Bands   Name   Description
======  =====  ========================
Band_1  blue   Blue
Band_2  green  Green
Band_3  red    Red
Band_4  nir    Near Infra-red
Band_5  swir1  Short-wave Infra-red 1
Band_7  swir2  Short-wave Infra-red 2
======  =====  ========================

NBAR-T
======

**Surface reflectance NBAR-T** includes the terrain illumination reflectance correction and has the same features of SR-NBAR and along with some of the features mentioned below.

Features
---------
* The SR product is created using a physics-based coupled BRDF and atmospheric correction model that can be applied to both flat and inclined surfaces (Li et al., 2012).
  The resulting surface reflectance values are comparable both within individual images and between images acquired at different times and/or with different sensors.

* Terrain affects optical satellite images through both irradiance and bidirectional reflectance distribution function (BRDF) effects.
* Slopes facing the sun receive enhanced solar irradiance and appear brighter compared to those facing away from the sun.
* For anisotropic surfaces, the radiance received at the satellite sensor from a sloping surface is also affected by surface
  BRDF which varies with combinations of surface landcover types, sun, and satellite geometry (sun and sensor view, and their relative
  azimuth angle) as well as topographic geometry (primarily slope and aspect angles).
  Consequently, to obtain comparable surface reflectance from satellite images covering mountainous areas,
  it is necessary to process the images to reduce or remove the topographic effect so that the images can be used for different purposes on the same spectral base.
* A Digital Surface Model (DSM) resolution appropriate to the scale of the resolution of satellite image is needed for the best results. 1 second SRTM DSM is
  used for NBART processing.

It has the same bands description provided in Surface reflactance NBAR table_.

Landsat Archive
---------------
GA has acquired Landsat imagery over Australia since 1979, including  TM, ETM+ and OLI imagery. While this data has been used extensively for numerous
land and coastal mapping studies, its utility for accurate monitoring of environmental resources has been limited by the processing methods that have been traditionally
used to correct for inherent geometric and radiometric distortions in EO imagery.
To improve access to Australia s archive of Landsat TM/ETM+/OLI data, several collaborative projects have been undertaken in conjunction with industry, government and academic partners.
These projects have enabled implementation of a more integrated approach to image data correction that incorporates
normalising models to account for atmospheric effects, BRDF and topographic shading (Li et al., 2012).

The approach has been applied to Landsat TM/ETM+ and OLI imagery to create the SR products. The advanced supercomputing facilities provided by the National
Computational Infrastructure (NCI) at the Australian National University (ANU) have been instrumental in handling the considerable data volumes and processing
complexities involved with production of this product.

Surface Reflectance Correction Models
--------------------------------------
    Image radiance values recorded by passive EO sensors are a composite of:

    * surface reflectance;
    * atmospheric condition;
    * interaction between surface land cover, solar radiation and sensor view angle;
    * land surface orientation relative to the imaging sensor.

    It has been traditionally assumed that Landsat imagery display negligible variation in sun and sensor view angles, however these can vary significantly both within
    and between scenes, especially in different seasons and geographic regions (Li et al., 2012). The SR product delivers modeled surface reflectance from Landsat TM/ETM+/OLI
    data using physical rather than empirical models. Accordingly, this product will ensure that reflective value differences between imagery acquired at different times by
    different sensors will be primarily due to on-ground changes in biophysical parameters rather than artifacts of the imaging environment.


Landsat 8-OLI/TIRS also has following bands:

=======  =============== ========================
Bands    Name            Description
=======  =============== ========================
Band_1   coastal_aerosol Coastal Aerosol
Band_2   blue            Blue
Band_3   green           Green
Band_4   red             Red
Band_5   nir             Near- Infrared
Band_6   swir1           Short-wave Infra-red 1
Band_7   swir2           Short-wave Infra-red 2
Band_8   panchromatic    Panchromatic
Band_9   cirrus          Cirrus
Band_10  tirs1           Thermal Infrared Sensor1
Band_11  tirs2           Thermal Infrared Sensor2
=======  =============== ========================

Landsat 5 and 7 have the same bands as NBAR table_.

Pixel Quality
=============

Summary
-------

**Product name : Pixel Quality 25 - (PQ25)**

The PQ25 product is a product which is designed to facilitate interpretation and processing of `Surface Reflectance`_ NBAR/NBART ,  `Fractional Cover`_
and derivative products.

Features
--------
PQ25 is an assessment of each image pixel to determine if it is an unobscured, unsaturated observation
of the Earth surface and also whether the pixel is represented in each spectral band. The PQ product allows
users to produce masks which can be used to exclude pixels which do not meet their quality criteria from analysis .
The capacity to automatically exclude such pixels is essential for emerging multi-temporal analysis techniques that
make use of every quality assured pixel within a time series of observations.Users can choose to process only land pixels,
or only sea pixels depending on their analytical requirements, leading to enhanced computationally efficient.

    PQ provides  an assessment of the quality of observations at a pixel level and includes information about whether a pixel is affected by:

    * Spectral Contiguity (lack of signal in any band)
    * Saturation in any band
    * Presence of cloud
    * Presence of cloud shadow
    * Land or sea

As Landsat Imagery becomes more readily available, there has been a rapid increase in the amount of analyses undertaken
by researchers around the globe.  Most researchers use some form of quality masking schema in order to remove undesirable
pixels from analysis, whether that be cloud, cloud shadow, observations over the ocean, or  saturated pixels.  In the past,
researchers would reject partly cloud-affected scenes in favour of cloud-free scenes.  However, Landsat time series analysis
using all cloud-free pixels has become a valuable technique and has increased the demand for automation of cloud, cloud
shadow and saturation detection.  Emergency response applications such as flood mapping typically have to contend with
individual cloud affected scenes and therefore rely on effective cloud and cloud shadow removal techniques.

The PQ25 product combines established algorithms that detect clouds including the Automated Cloud Cover Assessment
(ACCA) (Irish et al. 2006) and Function of mask (Fmask) (Zhu and Woodcock 2012) . ACCA is already widely used within the
remote sensing community; it is fast and relatively accurate.  Fmask on the other hand is newer, but is rapidly becoming
more established, and can provide a more accurate cloud mask than ACCA in certain cloud environments.

The different sensor designs of Landsat 5 TM, Landsat 7 ETM+, and Landsat 8 OLI all have
different sensor saturation characteristics. The PQ25 layer enables users to exclude
observations from a given band where the pixels are saturated (exceed the dynamic range
of the sensor). The per-band saturation information in PQ allows users to exclude pixels
where their specific band of interest is saturated.

The PQ 25 layer uses two industry standard cloud/cloud shadow detection algorithms to
flag pixels that potentially contain cloud and allows the user to generate masks based on
either algorithm or both algorithms.


.. _Fractional Cover:

Fractional Cover
================

Summary
--------

**Product Name - Fractional Cover (FC25)**
The Fractional Cover product is derived from Geoscience Australias Australian
Reflectance Grid 25 (ARG25) product and provides a 25m scale fractional cover
representation of the proportions of :

 * green or photosynthetic vegetation,
 * nonphotosynthetic vegetation, and
 * bare surface cover across the Australian continent

It is generated using the algorithm developed by the Joint Remote Sensing Research Program (JRSRP)
and described in Scarth et al. (2010)
The FC25 product suite is currently available for every scene Landsat Thematic Mapper
(Landsat 5), Enhanced Thematic Mapper (Landsat 7) and Operational Land Imager
(Landsat 8) scene acquired since 1987.

Features
--------

Fractional cover data can be used to identify large scale patterns and trends and inform
evidence based decision making and policy on topics including wind and water erosion
risk, soil carbon dynamics, land management practices and rangeland condition. This
information could enable policy agencies, natural and agricultural land resource
managers, and scientists to monitor land conditions over large areas over long time
frames.

    - The fractional cover unmixing algorthim uses the spectral signature for a picture element
      (pixel) to break it up into three parts or fractions.
    - This is based on field work identifying the spectral characteristics of each of the fractions.
    - The three fractions are green or photosynthetic vegetation, non-photosynthetic vegetation, and bare soil.
    - The green fraction includes leaves and grass, the non-photosynthetic includes branches, dry grass
      and dead leaf litter, and the bare soil fraction includes bare soil or rock.

FC25 is limited by the frequency and number of successful observations, which are
determined by the satellite revisit rate (see table below) and by clouds. In particular,
short-lived green flush grass growth events may not have been observed. In practice,
areas of inland Australia have been observed over 600 times, whereas highly-cloud-prone
coastal and mountainous areas may have no successful observations. FC25
is also limited by the quality of the sensors, including their spatial resolution, and the
accuracy of the fractional cover algorithms used.


Revisit frequency of the Landsat series of satellites as captured by the
Geoscience Australia ground station network.

=============   ==================
Time range      Revisit frequency
=============   ==================
1986-2003       16 days
2003-20112      8 days
2011-20133      16 days
2013-present    8 days
=============   ==================


Potential Applications
-----------------------

Fractional cover provides valuable information for a range of environemental and
agricultural applications, including:

- Soil erosion monitoring
- Land surface process modelling
- Land management practices (e.g. crop rotation, stubble management, rangeland
  management)
- Vegetation studies
- Fuel load estimation
- Ecosystem modelling
