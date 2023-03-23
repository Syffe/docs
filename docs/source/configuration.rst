.. meta::
   :description: Mergify Documentation for Configuration
   :keywords: mergify, configuration

.. _configuration file format:

=====================
🔖 Configuration File
=====================

File Used
---------

Mergify uses the configuration file that is:

- The file named ``.mergify.yml``, or, as a fallback, ``.mergify/config.yml`` or ``.github/mergify.yml``
  from the root directory.

- In the default repository branch configured on GitHub — usually ``main``.
- The configuration can be extended once by another configuration using the keyword ``extends``.

Format
------

Here's what you need to know about the file format:

- The file format is `YAML <http://yaml.org/>`_.

- The file main type is a dictionary whose keys are named
  ``pull_request_rules``, ``queue_rules``, ``partition_rules``,
  ``commands_restrictions``, ``defaults``, ``shared`` and ``extends``.

Pull Request Rules
~~~~~~~~~~~~~~~~~~

- The value type of the ``pull_request_rules`` is list.

- Each entry in ``pull_request_rules`` must be a dictionary.

Each dictionary must have the following keys:

.. list-table::
   :header-rows: 1
   :widths: 1 1 2

   * - Key Name
     - Value Type
     - Value Description
   * - ``actions``
     - dictionary of :ref:`Actions`
     - A dictionary made of :ref:`Actions` that will be executed on the
       matching pull requests.
   * - ``conditions``
     - list of :ref:`Conditions`
     - A list of :ref:`Conditions` string that must match against the pull
       request for the rule to be applied.
   * - ``disabled``
     - dictionary with ``reason`` key
     - This optional key allows to disabled a rule and cancel any ongoing
       actions. A reason must be set using the ``reason`` key.
   * - ``name``
     - string
     - The name of the rule. This is not used by the engine directly, but is
       used when reporting information about a rule. It's not possible to
       have two rules with the same name.


The rules are evaluated in the order they are defined in ``pull_request_rules``
and, therefore, the actions are executed in that same order.

See :ref:`Examples` for configuration file examples.

Queue Rules
~~~~~~~~~~~

- The value type of the ``queue_rules`` is list.

- Each entry in ``queue_rules`` must be a dictionary.

See :ref:`queue rules` for the complete list and description of options.

Partition Rules
~~~~~~~~~~~~~~~

- The value type of the ``partition_rules`` is a list.

- Each entry in ``partition_rules`` must be a dictionary.

See :ref:`partition rules` for the complete list and description of options.

.. _commands restrictions:

Commands Restrictions
~~~~~~~~~~~~~~~~~~~~~

- The value type of the ``commands_restrictions`` is dictionary.

- Each entry in ``commands_restrictions`` must be an :ref:`Actions` name and a dictionary.

Each dictionary must have the following keys:

.. list-table::
   :header-rows: 1
   :widths: 1 1 2

   * - Key Name
     - Value Type
     - Value Description
   * - ``conditions``
     - list of :ref:`Conditions`
     - A list of :ref:`Conditions` string that must match against the pull
       request for the command to be allowed.

For example, to limit backport commands for pull requests coming from the main branch:

.. code-block:: yaml

  commands_restrictions:
    backport:
      conditions:
        - base=main

Another example, to limit backport commands usage to a specific team (or user):

.. code-block:: yaml

  commands_restrictions:
    backport:
      conditions:
        - sender=@team

Or to limit backport commands for users with a specific permission on the repository.

.. code-block:: yaml

  commands_restrictions:
    backport:
      conditions:
        - sender-permission>=write


Defaults
~~~~~~~~

- The value type of ``defaults`` is a dictionary.

This dictionary must have the following key:

.. list-table::
   :header-rows: 1
   :widths: 1 1 2

   * - Key Name
     - Value Type
     - Value Description
   * - ``actions``
     - dictionary of :ref:`Actions`
     - A dictionary made of :ref:`Actions` whose configuration will be used by default.

The ``defaults`` section is used to define default configuration valued for actions run by pull request rules and by :ref:`Commands`.
If the options are defined in ``pull_request_rules`` they are used, otherwise, the values set in ``defaults`` are used.

For example:

.. code-block:: yaml

  defaults:
    actions:
      comment:
        bot_account: Autobot

  pull_request_rules:
    - name: comment with default
      conditions:
        - label=comment
      actions:
        comment:
          message: I 💙 Mergify

The configuration above is the same as below:

.. code-block:: yaml

  pull_request_rules:
    - name: comment with default
      conditions:
        - label=comment
      actions:
        comment:
          message: I 💙 Mergify
          bot_account: Autobot

Shared
~~~~~~

Anything can be stored in this key. Its main goal is to be able to have a place where you can put your redundant YAML anchors.

Examples
++++++++

.. code-block:: yaml

    shared:
      my_ci: &common_checks
        - check-success=ci-one
        - check-success=ci-two

    queue_rules:
      - name: hotfix
        merge_conditions: *common_checks

      - name: default
        merge_conditions:
        - check-success=slow-ci
        - and: *common_checks

    pull_request_rules:
      - name: Default merge
        conditions:
          - base=main
          - and: *common_checks
        actions:
          queue:
            name: default

      - name: Hotfix merge
        conditions:
          - base=main
          - label=hotfix
        actions:
          queue:
            name: hotfix

Data Types
----------
.. _data type commit:

Commits
~~~~~~~

List of commit object.

Example structure of a commit object:

.. code-block:: javascript

    {
        "sha": "foo-sha",
        "parents": ["parent-foo-sha", "another-parent-foo-sha"],
        "commit_message": "a commit message",
        "commit_verification_verified": True,
        "author": "commit-author",
        "date_author": "2012-04-14T16:00:49Z",
        "email_author": "user@example.com",
        "committer": "commit-committer",
        "date_committer": "2012-04-15T16:00:49Z"
        "email_committer": "user@example.com",
    }

Using the list of commit within a :ref:`template <data type template>`:

.. code-block::

    {% for commit in commits %}
    Co-Authored-By: {{ commit.author }} <{{ commit.email_author }}>
    {% endfor %}

.. _regular expressions:

Regular Expressions
~~~~~~~~~~~~~~~~~~~

You can use regular expression with matching :ref:`operators <Operators>` in
your :ref:`conditions <Conditions>` .

Mergify leverages `Python regular expressions
<https://docs.python.org/3/library/re.html>`_ to match rules.

.. tip::

   You can use `regex101 <https://regex101.com/>`_, `PyRegex
   <http://www.pyregex.com>`_ or `Pythex <https://pythex.org/>`_ to test your
   regular expressions.

Examples
++++++++

.. code-block:: yaml

    pull_request_rules:
      - name: add python label if a Python file is modified
        conditions:
          - files~=\.py$
        actions:
          label:
            add:
              - python

      - name: automatic merge for main when the title does not contain “WIP” (ignoring case)
        conditions:
          - base=main
          - -title~=(?i)wip
        actions:
          merge:
            method: merge

.. _schedule format:

Schedule
~~~~~~~~

This format represents a schedule.
It can contains only days, only times or both and can have a timezone specified
with the times (for the list of available time zones, see `IANA format <https://www.iana.org/time-zones>`_).
If no timezone is specified, it will default to UTC.

It can be used with the equality operators ``=`` and ``!=``.

.. code-block::

  schedule=Mon-Fri
  schedule=09:00-19:00
  schedule=09:00-19:00[America/Vancouver]
  schedule!=Mon-Fri 09:00-19:00[America/Vancouver]
  schedule!=SAT-SUN


Examples
++++++++

.. code-block:: yaml

      - name: merge on working hour
        conditions:
          - schedule=Mon-Fri 09:00-19:00[America/Vancouver]
        actions:
          merge:


.. _iso timestamp:

Timestamp
~~~~~~~~~

The timestamp format must follow the `ISO 8601 standard
<https://en.wikipedia.org/wiki/ISO_8601>`_. If the timezone is missing, the
timestamp is assumed to be in UTC.

.. code-block::

   2021-04-05
   2012-09-17T22:02:51
   2008-09-22T14:01:54Z
   2013-12-05T07:19:04-08:00
   2013-12-05T07:19:04[Europe/Paris]

Examples
++++++++

.. code-block:: yaml

      - name: end of life version 10.0
        conditions:
          - base=stable/10.0
          - updated-at<=2021-04-05
        actions:
          comment:
            message: |
              The pull request needs to be rebased after end of life of version 10.0


.. _relative timestamp:

Relative Timestamp
~~~~~~~~~~~~~~~~~~

Timestamps can be expressed relative to the current date and time.
The format is ``[DD days] [HH:MM] ago``:

* DD, the number of days
* HH, the number of hours
* MM, the number of minutes

If the current date is 18th June 2020, ``updated-at>=14 days ago`` will be translated ``updated-at>=2020-06-04T00:00:00``.

Examples
++++++++

.. code-block:: yaml

      - name: close stale pull request
        conditions:
          - base=main
          - -closed
          - updated-at<14 days ago
        actions:
          close:
            message: |
              This pull request looks stale. Feel free to reopen it if you think it's a mistake.


.. _duration:

Duration
~~~~~~~~

Duration can be expressed as ``quantity unit [quantity unit...]`` where
quantity is a number (possibly signed); unit is second, minute, hour, day,
week, or abbreviations or plurals of these units;

.. code-block::

   1 day 15 hours 6 minutes 42 seconds
   1 d 15 h 6 m 42 s

.. _priority:

Priority
~~~~~~~~

Priority values can be expressed by using an integer between 1 and 10000.
You can also use those aliases:
* ``low`` (1000)
* ``medium`` (2000)
* ``high`` (3000)

.. code-block:: yaml

    priority_rules:
      - name: my hotfix priority rule
        conditions:
          - base=main
          - label=hotfix
          - check-success=linters
        priority: high

      - name: my low priority rule
        conditions:
          - base=main
          - label=low
          - check-success=linters
        priority: 550

.. _data type template:

Template
~~~~~~~~

The template data type is a regular string that is rendered using the `Jinja2
template language <https://jinja.palletsprojects.com/templates/>`_.

If you don't need any of the power coming with this templating language, you
can just use this as a regular string.

However, those templates allow to use any of the :ref:`pull request attribute
<attributes>` in the final string.

For example the template string:

.. code-block:: jinja

    Thank you @{{author}} for your contribution!

will render to:

.. code-block:: jinja

    Thank you @jd for your contribution!

when used in your configuration file — considering the pull request author
login is ``jd``.

`Jinja2 filters <https://jinja.palletsprojects.com/en/3.0.x/templates/#builtin-filters>`_ are supported, you can build string from list for example with:

.. code-block:: jinja

    Approved by: @{{ approved_reviews_by | join(', @') }}

`Jinja2 string manipulation <https://jinja.palletsprojects.com/en/3.0.x/templates/#python-methods>`_ are also supported, you can split string for example with:

.. code-block:: jinja

   {{ body.split('----------')[0] | trim }}

We also provide custom Jinja2 filters:

* ``markdownify``: to convert HTML to Markdown:

.. code-block:: jinja

    {{ body | markdownify }}

* ``get_section(<section>, <default>)``: to extract one Markdown section

.. code-block:: jinja

    {{ body | get_section("## Description") }}

.. note::

   You need to replace the ``-`` character by ``_`` from the :ref:`pull request
   attribute <attributes>` names when using templates. The ``-`` is not a valid
   character for variable names in Jinja2 template.

.. note::

   By default, the HTML comments are stripped from ``body``. To get the
   full body, you can use the ``body_raw`` attribute.


YAML Anchors and Aliases
------------------------

The configuration file supports `YAML anchors and aliases
<https://yaml.org/spec/1.2.2/#anchors-and-aliases>`_. It allows reusing
configuration sections. For example, you could reuse the list of continuous
integration checks:

.. code-block:: yaml

    queue_rules:
      - name: hotfix
        merge_conditions:
          - and: &CheckRuns
            - check-success=linters
            - check-success=unit
            - check-success=functionnal
            - check-success=e2e
            - check-success=docker

      - name: default
        merge_conditions:
          - and: *CheckRuns
          - schedule=Mon-Fri 09:00-17:30[Europe/Paris]

    pull_request_rules:
      - name: automatic merge for hotfix
        conditions:
          - label=hotfix
          - and: *CheckRuns
        actions:
          queue:
            name: hotfix

      - name: automatic merge reviewed pull request
        conditions:
          - "#approved-reviews-by>=1"
          - and: *CheckRuns
        actions:
          queue:
            name: default


Disabling Rules
---------------

You can disable a rule while keeping it in the configuration. This allows
gracefully handling the cancellation of any ongoing actions (e.g., like stopping
the merge queue).

Example
~~~~~~~

.. code-block:: yaml

      - name: automatic merge for main when the title does not contain “WIP” (ignoring case)
        disabled:
          reason: code freeze
        conditions:
          - base=main
          - -title~=(?i)wip
        actions:
          merge:
            method: merge

Extends
-------

``extends`` is an optional key with its value type being a string.


You can extend a configuration once by inheriting the configuration from another
repository configuration where Mergify is installed. The value of the
``extends`` key is a repository name.

.. code-block:: yaml

  extends: my_repo


The local configuration inherits rules from the remote configuration. Remote
rules will be overridden by the local configuration if they have the same name.

Example:

``remote_repository/.mergify.yml``

.. code-block:: yaml

  pull_request_rules:
    - name: comment with default
      conditions:
        - label=comment
      actions:
        comment:
          message: I am a default comment 😊
    - name: comment when closed
      conditions:
        - label=closed
      actions:
        comment:
          message: Closed by Mergify 🔒

    commands_restrictions:
      backport:
        conditions:
          - base=main

``.mergify.yml``

.. code-block:: yaml

  extends: remote_repository

  pull_request_rules:
    - name: comment with default
      conditions:
        - label=comment
      actions:
        comment:
          message: I 💙 Mergify

    commands_restrictions:
      backport:
        conditions:
          - sender=@team

The result will be:

.. code-block:: yaml

  pull_request_rules:
    - name: comment with default
      conditions:
        - label=comment
      actions:
        comment:
          message: I 💙 Mergify
    - name: comment when closed
      conditions:
        - label=closed
      actions:
        comment:
          message: Closed by Mergify 🔒

    commands_restrictions:
      backport:
        conditions:
          - sender=@team


.. warning::

   Values in the ``shared`` key will not be merged and shared between local and
   remote configurations.


.. note::

   Values in the ``default`` key will be merged and remote default values will
   apply to local configuration.
