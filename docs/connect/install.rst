
.. _install:

Install
=======

Required Software
-----------------
 * TurboVNC - https://sourceforge.net/projects/turbovnc/files/
 * Strudel - https://cvl.massive.org.au/launcher_files/stable/

For full details on installing the software, see http://vdi.nci.org.au/help.

.. note::
   Your institution may provide this software to be installed via an internal process.
   For example at Geoscience Australia, the software can be requested from the
   `Software Service Management System (SSMS) <http://intranet.ga.gov.au/CherwellPortal/SSMS>`_
   [internal link only].

Setup
-----

To setup Studel to connect to the NCI, run the Strudel application, then:

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


Activate
--------

Once you have logged on to the VDI, you can add a desktop shortcut to Digital Earth Australia.

From the **Applications** menu in the top left of the screen, choose **System** -> **Terminal**.

.. figure:: /_static/vdi-launch-terminal.png
   :align: center
   :alt: Start Terminal Menu

   Launch the Terminal from the System Tools menu.

In the terminal window run the command::

   sh /g/data/v10/public/digitalearthau/install.sh

You can then launch the Digital Earth Australia Jupyter Notebook by double-clicking the icon.


Shutting Down VDI
-----------------

After you have finished working on the VDI, any open terminals can be closed using the x in
the corner of the tab and the VDI can be closed by clicking your name displayed in the top
right corner of the VDI interface and selecting Quit. The Strudel window will subsequently also
close after a few moments.

If you wish to disconnect from the VDI but keep the session running you can close the VDI
window using the x in the top right corner of the window and select keep the session running
when prompted by the Strudel window. Later you can reconnect to the VDI and resume the previous
session.
