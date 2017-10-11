# What's a Goalkeeper

The ``DEA Goalkeeper`` is a software engineer who provides frontline support to the community of DEA 
and OpenDatacube developers and users. 

The role of the ``DEA Goalkeeper`` is periodically rotated (usually weekly)
amongst the group of DEA developers who work at Geoscience Australia. If you are new to the ``DEA Goalkeeper`` role, this 
document should help.

# Goal Keeper Responsibilities

## Level 1 Support
This is a key responsibility for the ``DEA Goalkeeper``:

- Support requests on the `#general` and `#develop` channels on [opendatacube Slack](https://opendatacube.slack.com/messages)
- GitHub issues sent to [digitalearthau](https://github.com/GeoscienceAustralia/digitalearthau/issues)
or [datacube-core](https://github.com/opendatacube/datacube-core/issues)

Each request or issue coming through any of these channels should be triaged as quickly as possible:

1. Determine the nature of the issue and 
1. who is the best person to address it initially (Consider attending to the issue yourself)
1. task the appropriate person 
1. ensure an initial response is sent back to the requestor ASAP (even if it is simply ``Thankyou for raising this. We are looking into it now``
1. Manage the issue until it is resolved

If the request or issue is still outstanding, summarise it's status in your handover to the next ``DEA Goalkeeper``

## Build manager

Review the ``Continuous Integration Status`` of each of the following projects

* [digitalearthau](https://github.com/GeoscienceAustralia/digitalearthau/)
* [datacube-core](https://github.com/opendatacube/datacube-core)
* [dea-orchestration](https://github.com/GeoscienceAustralia/dea-orchestration)

They all should be ``green``. If the ``CI Loop`` gets broken:

  - Follow up with whomever broke it
  - Or simply fix it yourself
  
BTW: The ``Continuous Integration Status`` (or ``CI Loop``) is the status indication shown as the first line of the
project ``README`` file when viewed via the ``Github`` website.


## Release manager

The ``DEA Goalkeeper`` will manage the release process when a new release is required

Refer to the following documentation:

- [digitalearthau build instructions](https://github.com/GeoscienceAustralia/digitalearthau/blob/develop/modules/README.md)
- others?
