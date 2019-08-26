
.. _install:

Installation and Software Setup
*******************************

Required Software
=================

Install TurboVNC and Strudel according to the instructions at http://vdi.nci.org.au/help.

.. note::
   The NCI instructions recommend specific versions of 
   `TurboVNC <https://sourceforge.net/projects/turbovnc/files/>`_ and
   `Strudel <https://cvl.massive.org.au/launcher_files/stable/>`_.
   More recent versions may or may not be compatible with the VDI.

.. note::
   Your institution may provide this software to be installed via an internal process.
   For example at Geoscience Australia, the software can be requested from the
   `Software Service Management System (SSMS) <http://intranet.ga.gov.au/CherwellPortal/SSMS>`_
   [internal link only].

Connecting to the VDI
=====================

To setup Strudel to connect to the NCI, run the Strudel application, then:

* Select **File** -> **Manage Sites**
* Click **New**
* Enter the details:

  - Name: **NCI Virtual Desktops**
  - URL: **https://vdi.nci.org.au/strudel.json**

* Click **OK**
* Make sure the **Active** checkbox is ticked.
* Click **OK**

To connect:

* Site: **NCI Virtual Desktops**
* Username: Your NCI username (eg `abc123` or `ab1234`)
* Click **Login**

.. note::
   If the drop-down site list in Strudel remains empty, it most likely means 
   that the software is unable to retrieve the strudel.json URL, 
   such as due to firewall or network proxy configuration.

Setting up DEA
==============

Once you have logged on to the VDI, you can add a desktop shortcut to Digital Earth Australia.

From the **Applications** menu in the top left of the screen, choose **System** -> **Terminal**.

.. figure:: /_static/vdi-launch-terminal.png
   :align: center
   :alt: Start Terminal Menu

   Launch the Terminal from the System Tools menu.

In the terminal window run the command::

   sh /g/data/v10/public/digitalearthau/install.sh

You can then launch Jupyter Notebooks, with the Digital Earth Australia environment preconfigured, by double-clicking the icon.
From within this environment you can access the notebooks from the User Guide below, or create your own notebooks to work with Digital Earth Australia.

Shutting Down VDI
=================

After you have finished working on the VDI, any open terminals can be closed using the x in
the corner of the tab and the VDI can be closed by clicking your name displayed in the top
right corner of the VDI interface and selecting Quit. The Strudel window will subsequently also
close after a few moments.

If you wish to disconnect from the VDI but keep the session running you can close the VDI
window using the x in the top right corner of the window and select keep the session running
when prompted by the Strudel window. Later you can reconnect to the VDI and resume the previous
session.
