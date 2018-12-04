
Bulk download products from Amazon S3
=====================================


Last updated: 28th November 2018


Secure File Transfer Protocol
-----------------------------


SFTP is a method of securely bulk downloading files from a remote server.
We have recently enabled this capability on our Amazon S3 products https://data.dea.ga.gov.au


Creating SSH Keys (Windows)
----------------------------


The secure part of secure File Transfer Protocol requires SSH Keys.
This is used to validate your identity and encrypt information in transit.

To create SSH Keys on windows you will need to use the PuttyGen application which comes with the Putty package.
You can download the Putty package from the `maintainers website <https://www.chiark.greenend.org.uk/~sgtatham/putty/latest.html>`_

You can then follow `These instructions <https://www.ssh.com/ssh/putty/windows/puttygen>`_ to generate a new key pair with PuttyGen (Don't configure a password when generating the key)

You will need to send us the `Public key for pasting into OpenSSH authorized_keys files.`


Creating SSH Keys (Mac OSX)
----------------------------


Open the Terminal app and type ``ssh-keygen -P "" -f ~/.ssh/transfer-key`` This will generate new SSH Keys.
To copy the public key type ``cat ~/.ssh/transfer-key.pub | pbcopy``

You can now paste this in the request an account email template.


Request an account
-------------------


To request an account you will need to email earth.observation@ga.gov.au
please provide the following information.

.. code:: text

    Hi DEA,

    Can you please ask the Cloud team to create an account for me on sftp.dea.ga.gov.au
    My details are as follows:

    email:
    organisation:
    public key:


.. note::

  We aim to make this process self-service in the future


Connecting to the sftp server
-----------------------------


Our SFTP supports the following clients: 

- `WinSCP <https://winscp.net/eng/index.php>`_ – A Windows-only graphical client.
- `Cyberduck <https://cyberduck.io>`_ – A Linux, Macintosh, and Microsoft Windows graphical client.
- `FileZilla <https://filezilla-project.org>`_ – A Linux, Macintosh, and Windows graphical client.
- `OpenSSH <https://www.openssh.com>`_ – A Macintosh and Linux command line utility.

You can connect to our server using the url ``sftp.dea.ga.gov.au`` and port ``22``.

Instructions for configuring each client are available here: 

https://docs.aws.amazon.com/transfer/latest/userguide/getting-started-use-the-service.html
