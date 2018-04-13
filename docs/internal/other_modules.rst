
Other Modules
=============

To use these modules, you will need the public modules available:

    module use /g/data/v10/public/modules/modulefiles

Pycharm
-------

Pycharm (Community Edition) is a free
The easiest way to run Pycharm is to copy the launcher on to your desktop.

You can do this by running:

    cp /g/data/v10/public/modules/pycharm/PyCharm.desktop ~/Desktop/


To upgrade the version, download the latest tarball from the jetbrains website and place it in the folder
``/g/data/v10/public/modules/pycharm/``

If you have a license for Pycharm Professional Edition, you can use your JetBrains Account with the version at:

    cp /g/data/v10/public/modules/pycharm-pro/PyCharmPro.desktop ~/Desktop/

To set up your virtual environment, in PyCharm:

    - **File** -> **Settings...** -> **Project/Project Interpreter**
    - Click the cog icon and select "Add Local..." -> Conda Environment
    - In **Existing Environment** -> **Interpreter**, enter the path to the python executable,
      eg `/g/data/v10/public/modules/dea-env/20180405/bin/python`


NC Tools
--------

The Java NetCDF ToolsUI can be run by calling:

    module load nctools
    nctools

More information can be found at https://www.unidata.ucar.edu/downloads/netcdf/netcdf-java-4/index.jsp

