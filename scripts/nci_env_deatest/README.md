# Script to test DEA module
---
## INTRODUCTION:

The DataCube application is primarily responsible for creating, storing and analysing Data Cubes. DataCube application can be used on the local computers, data hubs and cloud services. 
The easiest way to use Digital Earth Australia is to connect using a remote desktop at NCI, called VDI. (It is also possible to run on NCI's Raijin HPC cluster, but is recommended that prototypes are tested first on VDI system.)

## SCOPE:

The overall purpose of the DEA module test is to ensure there are no regressions failures on the new application build and it meets all of its technical, functional and business requirements. The purpose of this document is to describe the overall test plan and strategy for testing the DEA module. The approach described in this document provides the framework for all testing related to this application. Additional individual test cases may be written for each version of the application that will be released. This document will also be updated as required for each release.

Factors influencing test scope:
* Ensures a working subsystem/ component/ library
* Software reuse more practical

## TESTING GOALS:

The goals in testing this application include validating the quality, usability, reliability and performance of the application. Testing will be performed from a black-box approach, not based on any knowledge of internal design or code. Tests will be designed around requirements and functionality.

Another goal is to make the tests repeatable for use in regression testing during the project lifecycle, and for future application upgrades. A part of the approach in testing will be to initially perform a 'Smoke Test' upon delivery of the application for testing. Smoke Testing is typically an initial testing effort to determine if a new software version is performing well enough to accept it for a major testing effort. After acceptance of the build delivered for system testing, functions will be tested based upon the designated priority (critical, high, medium, low).

## TEST OBJECTIVE:

The objective of the script (Test_DEA_Module.sh) is to verify the following on NCI/Raijin system:
* DataCube product add
* DataCube dataset add
* DataCube indexing
* DataCube ingest
* DataCube Stats
* DataCube Fractional Cover
* Notebook convert on the Jupyter notebook
* PBS Scheduling
* Standard query on database
* Dataset search by location
* Query products based on certain quality
* Search with sources
* Query products by time range
* Data load
* Load SRTM
* Load Radiometric
* Load NBAR
* MODIS Landsat time series
* Load rainfall
* Query/load/plot WoFS
* Query/load/plot NBART
* Query/load/plot Landsat Fractional Cover
* Pixel Quality
* CLI Dataset info search

## Steps to run the script:
### Test DEA module:
1) Clone the git repo [nci_env_deatest](https://github.com/GeoscienceAustralia/digitalearthau/tree/new_nci_scripts/scripts/nci_env_deatest) to local directory.
2) Modify the datacube_config.conf file (if we need to customise the datacube configurations).
3) Navigate on the terminal window to the folder where the test script (Test_DEA_Module.sh) is placed.
4) Execute the shell script by issuing the following:
**sh Test_DEA_Module.sh [--help] [DEA_MODULE_TO_TEST]**
where:
**DEA_MODULE_TO_TEST**  is Module under test (ex. dea/20180503 or dea-env or dea)
5) Once the the execution is complete, verify the logs within **output_files** folder created within the test script directory.

### Test DEA Stacker:
1) Navigate on the terminal window to the folder where the test script (Test_DeaStacker.sh) is placed.
2) Execute the shell script by issuing the following:
**Test_DeaStacker.sh [--help] [DEA_MODULE_TO_TEST] [YEAR_TO_STACK]**
where:
**DEA_MODULE_TO_TEST**  is Module under test (ex. dea/20180503 or dea-env or dea)
**YEAR_TO_STACK** is year in YYYY format (ex. 2018)
3) Once the the execution is complete, verify the logs within **output_files\deastacker** folder created within the test script directory.


### Test DEA Submit Sync:
1) Navigate on the terminal window to the folder where the test script (Test_DeaSubmitSync.sh) is placed.
2) Execute the shell script by issuing the following:
**Test_DeaSubmitSync.sh [--help] [DEA_MODULE_TO_TEST]**
where:
**DEA_MODULE_TO_TEST**  is Module under test (ex. dea/20180503 or dea-env or dea)
3) Once the the execution is complete, verify the logs within **output_files\submit_sync** folder created within the test script directory.
