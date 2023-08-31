.. meta::
   :description: Mergify Documentation for github_actions Action
   :keywords: mergify, github actions, workflow
   :summary: Dispatch GitHub Actions workflows.
   :doc:icon: cogs

.. _github_actions action:

github_actions
==============

The ``github_actions`` action dispatches an existing workflow of the repository.

.. note::

    Your workflow must have the ``workflow_dispatch`` trigger configured, with
    optional inputs as depicted in the GitHub's Actions API documentation.
    On top of that, Mergify must be granted write permissions on Actions.
    Go to Mergify's UI to accept the requested permissions to allow for this action.

Options
-------

The ``github_actions`` rule can be defined as follows:

.. list-table::
   :header-rows: 1
   :widths: 1 1 1 3

   * - Key Name
     - Value Type
     - Default
     - Value Description
   * - ``workflow``
     - :ref:`dispatch`
     -
     - The dispatch configuration.



.. _dispatch:

Dispatch
~~~~~~~~

The ``dispatch`` rule takes the following parameters:

.. list-table::
   :header-rows: 1
   :widths: 1 1 1 3

   * - Key Name
     - Value Type
     - Default
     - Value Description
   * - ``dispatch``
     - list of :ref:`workflow dispatch`
     -
     - The list of Workflows to dispatch via the action.

.. _workflow dispatch:

Workflow Dispatch
~~~~~~~~~~~~~~~~~

A ``workflow_dispatch`` item is defined as follows:

.. list-table::
   :header-rows: 1
   :widths: 1 1 1 3

   * - Key Name
     - Value Type
     - Default
     - Value Description
   * - ``workflow``
     - string
     -
     - The name of the ``.yaml`` GitHub Workflow file with its extension.
   * - ``inputs``
     - mapping of input: value pairs up to 10 inputs
     -
     - The inputs passed to your workflow execution if any.


Examples
--------

Dispatch GitHub Workflows with inputs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following rule dispatches the two workflows ``hello_world_workflow.yaml``
and ``foo_workflow.yaml`` which are configured on the repository, when the
``dispatch`` label is set.
Two inputs have been configured on the workflow ``hello_world_workflow.yaml``.
They are passed the ``name`` string value and the ``age`` number value.

.. code-block:: yaml

    pull_request_rules:
      - name: Dispatch GitHub Actions
        conditions:
          - label=dispatch
        actions:
          github_actions:
            workflow:
              dispatch:
                - workflow: hello_world_workflow.yaml
                  inputs:
                    name: steve
                    age: 42
                - workflow: foo_workflow.yaml

