==================
Recipient
==================

import_experiment
***********

.. function:: import_experiment

    Create a delta sharing share containing all the models from an experiment.

    :param delta_sharing_coordinate: 3 piece coordinates identifying the share.
    :type delta_sharing_coordinate: String
    :param experiment_name: Experiment Name that will be imported.
    :type experiment_name: String
    :rtype: None: No return type.

    :example:

.. tabs::
   .. code-tab:: python

    >>> ``%%arcuate_import_experiment``
        CREATE EXPERIMENT [OVERWRITE] experiment_name AS [PANDAS/SPARK] delta_sharing_coordinate

   .. code-tab:: python

    >>> import arcuate
        import delta_sharing

        df = delta_sharing.load_as_pandas(delta_sharing_coordinate)

        # import the shared table as experiment
        import_experiments(df, experiment_name)


.. note:: This function doesnt have any real return type. It is either a success or it will throw an error.

export_model
************

.. function:: export_experiment

    Create a delta sharing share containing the requested model.

    :param delta_sharing_coordinate: 3 piece coordinates identifying the share.
    :type delta_sharing_coordinate: String
    :param model_name: Model Name that will be imported.
    :type model_name: String
    :rtype: None: No return type.

    :example:

.. tabs::
   .. code-tab:: arcuate-magic

    >>> ``%%arcuate_import_model``
        CREATE MODEL [OVERWRITE] model_name AS [PANDAS/SPARK] delta_sharing_coordinate

   .. code-tab:: python

    >>> import arcuate
        import delta_sharing

        df = delta_sharing.load_as_pandas(delta_sharing_coordinate)

        # import the model
        import_models(df, model_name)


.. note:: This function doesnt have any real return type. It is either a success or it will throw an error.