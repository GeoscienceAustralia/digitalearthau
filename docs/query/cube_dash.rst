.. highlight:: console

.. cube_dash:

Cubedash
====================

Cubedash is a program which allows users to see current available data holdings as indexed in Digital Earth Australia. 
To start the Cubedash program, in a VDI terminal window, after loading the AGDC module, type the following command::

    /g/data/v10/public/run-dash.sh & 

After a moment the program will start running and provide console output similar to::

    [1] 3233
    [bt2744@vdi-n15 testing]$  * Serving Flask app "cubedash"
    * Running on http://127.0.0.1:8080/ (Press CTRL+C to quit)


Once you have received the above response, type the following command to open a Firefox web browser with the Cubedash program loaded::
    
    firefox http://127.0.0.1:8080/ 

A map is initially shown, from where you can explore the features of different products available in DEA

.. figure:: /_static/cubedash.png
   :align: center
   :alt: Start Terminal Menu
