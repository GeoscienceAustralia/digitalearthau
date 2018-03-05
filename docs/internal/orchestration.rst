.. highlight:: console

.. internal_orchestration:

DEA Orchestration
=================
`DEA orchestration <https://github.com/GeoscienceAustralia/dea-orchestration>`_ is a collection of wrapper and helper
libraries for communicating between AWS lambda and NCI's raijin facilities; and a collection of scripts that can be
triggered within the raijin environment.

Lambda Functions
----------------
To create a new lambda function create a class that inherits from one of the `command classes`_ in the
lambda_modules directory.

.. _command classes: https://github.com/GeoscienceAustralia/dea-orchestration/blob/master/lambda_modules/dea_raijin/dea_raijin/lambda_commands.py

**To Create a new Lambda Function**

- A file must be created called *{{script_name}}.py*, and a correspond *{{script_name}}* directory.
- Inside the directory create the following files:
    * requirements.txt (with the python dependencies)
    * If internal modules are required base them from the lambda_modules directory (e.g. ./../dea_raijin
    * an *env_vars.yaml* file which documents the environment variables required by the lambda.
- The "command" method needs to be overwritten by the subclass and is invoked to run the command.
- The *{{script_name}}.py* file must include a handler method that accepts the event and context variables
  from AWS and instantiates the user defined command class and calls run.
- The command must have a class variable "COMMAND_NAME" which is used to identify the command when logging.
- To create a new function use *./scripts/package_lambda {{script_name}} {{zipfile_name}}* to create a packaged
  zipfile; this will need to be uploaded into AWS Lambda with the corresponding IAM roles and access to
  ec2 parameter store.

- Make sure the private keys are stored in
  `aws ssm <http://docs.aws.amazon.com/systems-manager/latest/userguide/sysman-paramstore-walk.html>`_
- By default the user credentials will be retrieved from the ssm parameters:

:User: orchestrator.raijin.users.default.user
:Host: orchestrator.raijin.users.default.host
:Private Key: orchestrator.raijin.users.default.pkey

.. note:

   The prefix orchestrator.raijin.users.default can be overwritten with
   the ``DEA_RAIJIN_USER_PATH`` environment variable.

- When the lambda is configured it will need an associated role with policy permissions to access
    the ssm to retrieve parameters and the
    `kms <http://docs.aws.amazon.com/kms/latest/developerguide/key-policies.html>`_ decryption key.
    Installs the requirements of a script into the current python env; useful to install internal modules.

An `example lambda class <https://github.com/GeoscienceAustralia/dea-orchestration/blob/master/lambda_functions/example/example.py>`_
is available to use a template.

**Writing a new Lambda function**

* For scripts that inherit from the BaseCommand/RaijinCommand logging
  is available with *self.logger.{{level}}('{{message}}')*
* For RaijinCommands/BaseCommands the constructor must pass itself into the super constructor
  to provide the command name.
* For RaijinCommands the ssh_client can be accessed directly by using self.raijin.ssh_client
* Use the inbuilt exec_command of self.raijin to use default logging behaviour and have the stdout, stderr, and
  exit_code decoded and returned for processing.
* BaseCommands can manage the connection themselves by importing and using the RaijinSession object from
  dea_raijin.auth

**To test run a Lambda function**

* The simplest method to test run a lambda command is to call the run_lambda script in the scripts directory.
* The script will need to be run from an environment that has access to AWS ssm and AWS kms key.
    * This can be done by installing the
      `aws cli and invoking aws configure <http://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html>`_
* This script invokes the lambda handler based on the script name after initialising the
  environment variables in env_vars.
* If additional raijin commands are required they should be submitted first for approval.

Raijin Scripts
--------------
Raijin scripts folder contain a list of pre-approved commands that are available to run under one of DEA's
NCI accounts. Commands in this folder should be locked down to ensure that the user isn't able to
execute arbitrary code in our environment.

**To Create a new Raijin script**

    * create a folder in the raijin_scripts directory with the name of that will be used to invoke the command.
    * Inside the directory is an executable run file which will be called via the executor with the
      commandline arguments passed into the function.
    * If you require additional files please store them in this directory, for example have a python virtual
      environment in order to access libraries please store them in this directory.
    * If there is work required to install the command, please create an install.sh file in this directory
      which is where the code will be executed from following approval.
    * stderr, stdout and exit_code is returned to the lambda function by default
    * An exit code of 127 (command not found) is returned if remote cannot find the command requested.

**Running a Raijin Command**

    * The entry point to raijin is the ./scripts/remote executable.
    * If you wish to test raijin commands it can be done from this entry point.

        * copy the repository into your NCI environment and from the base folder run
          *./scripts/remote {{raijin_script_name}} {{args}}*

Updating internal modules
--------------------------
    * To update internal modules in your virtual env run ``pip install --upgrade -r requirements.txt``
      to ensure that your installed copies of the modules are up to date

Repo Script Reference
----------------------

* `./scripts/install_script {{script_name}} <https://github.com/GeoscienceAustralia/dea-orchestration/blob/master/scripts/install_script>`_ :
  Installs the requirements of a script into the current python env; useful to install internal modules.
* `./scripts/package_lambda {{script_name}} {{output_zip}} <https://github.com/GeoscienceAustralia/dea-orchestration/blob/master/scripts/package_lambda>`_ :
  Creates a lambda zipfile with dependencies from the scripts' requirements.txt file which can be used by lambda.
* `./scripts/run_lambda {{script_name}} <https://github.com/GeoscienceAustralia/dea-orchestration/blob/master/scripts/run_lambda>`_ :
  runs the script importing the environment variables from the env_vars.yaml file.
* `./scripts/remote {{raijin_script}} {{args}} <https://github.com/GeoscienceAustralia/dea-orchestration/blob/master/scripts/remote>`_ :
  runs the script file in the raijin environment with the passed args; scripts must exist in the raijin folder
* `./scripts/git_pull <https://github.com/GeoscienceAustralia/dea-orchestration/blob/master/scripts/git_pull>`_ :
  script to update the repository from the current production branch

Collection Installation on Raijin
----------------------------------
In order to set up this library on Raijin the user is required to generate 3 ssh keys.

* One to be able to access the remote script
* One to be able to access the git_pull script (to limit how this is triggered)
* One to be able to read from the code repository.

The first 2 keys should be appended to the users ~/.ssh/authorized_keys file.

::

    The ssh key for remote should be prepended with
    command="{{directory_location/scripts/remote",no-agent-forwarding,no-port-forwarding,no-pty,no-user-rc,
    no-X11-forwarding ssh-rsa AA3tEnxs/...E4S+UGaYQ== Running of scripts under NCI

    The ssh key for git pull should be prepended with
    command="{{directory_location/scripts/git_pull",no-agent-forwarding,no-port-forwarding,no-pty,no-user-rc,
    no-X11-forwarding ssh-rsa AA3tEnxs/...E4S+UGaYQ== Automated deployment of dea-orchestration


