.. meta::
   :description: Mergify Documentation for Rebase Action
   :keywords: mergify, rebase, pull request
   :summary: Rebase a pull request on top of its base branch.
   :doc:icon: undo

.. _rebase action:

rebase
======


The ``rebase`` action will rebase the pull request against its base branch. To
this effect, it clones the branch, run `git rebase` locally and push back the
result to the GitHub repository.

.. tip::

   You do not need to use this action if you use the :ref:`queue action page`
   action. The merge queue automatically update the pull requests it processes
   as necessary, making sure they are test with up-to-date code before being
   merged.

.. warning::

   GitHub Applications are not allowed to force-push to a branch. To get around
   that limitation, Mergify tries to impersonate the pull request author or one
   of the repository members to force-push the branch. That means that the
   GitHub UI will show the collaborator as author of the push, while Mergify
   actually executed it. If you need to control which user is impersonated, you
   can use the ``bot_account`` option.


Options
-------

.. list-table::
  :header-rows: 1
  :widths: 1 1 1 2

  * - Key Name
    - Value Type
    - Default
    - Value Description

  * - ``autosquash``
    - bool
    - True
    - When set to ``True``, commits starting with ``fixup!``, ``squash!`` and ``amend!``
      are squashed during the rebase.

  * - ``bot_account``
    - :ref:`data type template`
    -
    - |essential plan tag|
      For certain actions, such as rebasing branches, Mergify has to
      impersonate a GitHub user. You can specify the account to use with this
      option. If no ``bot_account`` is set, Mergify picks randomly one of the
      organization users instead. The user account **must** have already been
      logged in Mergify dashboard once.


.. include:: ../global-substitutions.rst
