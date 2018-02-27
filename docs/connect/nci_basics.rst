.. highlight:: console

.. nci_basics:

Command Line Usage (Advanced)
=============================

.. note:

   This section is intended for advanced users, and describes using DEA from
   a command line interface. This is mostly useful if you intend on running
   batch jobs on ``raijin`` and need to do some testing on VDI. Or simply if
   you're curious.


On ``VDI``, you can start a terminal window from **Applications** -> **System Tools**.

To manually use the modules on ``raijin`` or ``VDI``, add the datacube module path::

    $ module use /g/data/v10/public/modules/modulefiles/

(you can add the above to your ``.bashrc`` to avoid running it every time)

You should now have access to the following modules:

 * **agdc-py2-prod** - Python 2.7
 * **agdc-py3-prod** - Python 3.5

You can see a list of available modules by running::

    $ module avail

To load the production module with Python 3 (which is highly recommended over 2), run::

    $ module load agdc-py3-prod

The first time you load the module, it will register your account with the datacube, granting you read-only access.

It will store your password in the file `~/.pgpass`.

You can then launch the jupyter notebook by running::

    $ jupyter notebook

.. note::
    VDI and Raijin have separate home directories, so you must copy your pgpass to the other if
    you use both environments.

    You can push the contents of your pgpass file from VDI to Raijin by running on a terminal window in VDI::

        remote-hpc-cmd init
        ssh raijin "cat >> ~/.pgpass" < ~/.pgpass
        ssh raijin "chmod 0600 ~/.pgpass"

    You will most likely be prompted for your NCI password.

    To pull the contents of your pgpass from Raijin to VDI instead, run ::

        ssh raijin "cat ~/.pgpass" >> ~/.pgpass
        chmod 0600 ~/.pgpass

.. warning::
    If you have created a ``.datacube.conf`` file in your home folder from early Data Cube betas, you should rename or remove it
    to avoid it conflicting with the settings loaded by the module.
