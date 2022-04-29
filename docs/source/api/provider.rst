==================
Provider
==================

export_experiment
***********

.. function:: export_experiment

    Create a delta sharing share containing all the models from an experiment.

    :param share_name: Share Name that will contain the table of models.
    :type share_name: String
    :param table_name: Table Name that will contain the models.
    :type table_name: String
    :param experiment_name: Experiment Name that will be shared.
    :type experiment_name: String
    :rtype: None: No return type.

    :example:

.. tabs::
   .. tab:: arcuate-magic

    >>> ``%%arcuate_export_experiment``
        CREATE SHARE share_name WITH TABLE table_name FROM EXPERIMENT experiment_name

   .. code-tab:: python

    >>> import arcuate

        # export the experiment experiment_name to table_name, and add it to share_name
        export_experiments(experiment_name, table_name, share_name)


.. note:: This function doesnt have any real return type. It is either a success or it will throw an error.

export_model
************

.. function:: export_model

    Create a delta sharing share containing the requested model.

    :param share_name: Share Name that will contain the table with the model.
    :type share_name: String
    :param table_name: Table Name that will contain the model.
    :type table_name: String
    :param model_name: Model Name that will be shared.
    :type model_name: String
    :rtype: None: No return type.

    :example:

.. tabs::
   .. code-tab:: arcuate-magic

    >>> ``%%arcuate_export_model``
        CREATE SHARE share_name WITH TABLE table_name FROM MODEL model_name

   .. code-tab:: python

    >>> import arcuate

        # export the model model_name to table_name, and add it to share_name
        export_models(model_name, table_name, share_name)


.. note:: This function doesnt have any real return type. It is either a success or it will throw an error.