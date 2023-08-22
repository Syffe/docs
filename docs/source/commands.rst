.. meta::
   :description: Mergify Documentation for Commands
   :keywords: mergify, commands

.. _Commands:

============
 ⚙️ Commands
============

Mergify is able to run some :ref:`Actions` directly without leveraging the rule
system. By commenting on a pull request, you can ask Mergify to executes some
actions.

The syntax of the comment is::

  @Mergifyio <command> [parameters …]

.. note::

  Each command have default restrictions described below. A user might need to
  be the author of the pull request or have write access to the repository to
  run a command. More restrictions can be added with :ref:`Commands
  Restrictions`.

The list of available commands is listed below, with their parameters:

.. _backport command:

backport
========

Runs the :ref:`backport action` action.

.. list-table::
  :widths: 2 10
  :align: left

  * - Syntax
    - ``@Mergifyio backport <branch name> <branch name 2> …``
  * - Example
    - ``@Mergifyio backport stable/3.1 stable/4.0``
  * - Default restrictions
    - Write access

.. _copy command:

copy
====

Runs the :ref:`copy action` action.

.. list-table::
  :widths: 2 10
  :align: left

  * - Syntax
    - ``@Mergifyio copy <branch name> <branch name 2> …``
  * - Example
    - ``@Mergifyio copy stable/3.1 stable/4.0``
  * - Default restrictions
    - Write access


.. _queue command:

queue
=====

Add this pull request to the merge queue.

.. list-table::
  :widths: 2 10
  :align: left

  * - Syntax
    - ``@Mergifyio queue [<queue-name>]``
  * - Example
    - ``@Mergifyio queue``

      ``@Mergifyio queue default``
  * - Default restrictions
    - Write access

.. _rebase command:

rebase
======

Runs the :ref:`rebase action` action.

.. list-table::
  :widths: 2 10
  :align: left

  * - Syntax
    - ``@Mergifyio rebase``
  * - Example
    - ``@Mergifyio rebase``
  * - Default restrictions
    - Write access or author of the pull request

.. _refresh command:

refresh
========

Re-evaluates your Mergify rules on this pull request.

.. list-table::
  :widths: 2 10
  :align: left

  * - Syntax
    - ``@Mergifyio refresh``
  * - Example
    - ``@Mergifyio refresh``
  * - Default restrictions
    - Write access or author of the pull request

squash
======

Runs the :ref:`squash action` action.

.. list-table::
  :widths: 2 10
  :align: left

  * - Syntax
    - ``@Mergifyio squash [<commit_message format>]``
  * - Example
    - ``@Mergifyio squash first-commit``
  * - Default restrictions
    - Write access or author of the pull request

.. _update command:

update
======

Runs the :ref:`update action` action.

.. list-table::
  :widths: 2 10
  :align: left

  * - Syntax
    - ``@Mergifyio update``
  * - Example
    - ``@Mergifyio update``
  * - Default restrictions
    - Write access or author of the pull request


unqueue
=======

Removes this pull request from the merge queue if it has been queued with
:ref:`queue action page` action or :ref:`queue command` command.

.. list-table::
  :widths: 2 10
  :align: left

  * - Syntax
    - ``@Mergifyio unqueue``
  * - Example
    - ``@Mergifyio unqueue``
  * - Default restrictions
    - Write access

.. _requeue command:

requeue
=======

Inform Mergify that the CI failure was not due to the pull request itself, but to, e.g., a flaky test.
If you do not have any ``queue`` action in your ``pull_request_rules``, the pull request will
automatically be queued again, in addition to the failed queued state being cleaned.

The optional ``queue-name`` parameter for the command can only be used when no ``pull_request_rules`` use the ``queue`` action.

.. list-table::
  :widths: 2 10
  :align: left

  * - Syntax
    - ``@Mergifyio requeue [<queue-name>]``
  * - Example
    - ``@Mergifyio requeue``

      ``@Mergifyio requeue foo``
  * - Default restrictions
    - Write access


.. include:: global-substitutions.rst
