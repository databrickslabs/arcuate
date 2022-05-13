================================
ML Model & Experiment Recipient
================================

import_experiment
*****************

.. function:: import_experiment

    Create an MLflow experiment containing all the runs from a share

    :param delta_sharing_coordinate: 3 piece coordinates identifying the share.
    :type delta_sharing_coordinate: String
    :param experiment_name: Experiment Name that will be imported.
    :type experiment_name: String
    :rtype: None: No return type.

    :example:

.. tabs::
   .. code-tab:: arcuate_magic Arcuate Magic

        %%arcuate
        CREATE [OR REPLACE] EXPERIMENT experiment_name AS [PANDAS/SPARK] delta_sharing_coordinate

   .. code-tab:: python

        import arcuate
        import delta_sharing

        df = delta_sharing.load_as_pandas(delta_sharing_coordinate)

        # import the shared table as experiment
        import_experiments(df, experiment_name)


.. note:: This function doesnt have any real return type. It is either a success or it will throw an error.

import_model
************

.. function:: import_model Arcuate Magic

    Create an MLflow model in the registry containing all the models from a share

    :param delta_sharing_coordinate: 3 piece coordinates identifying the share.
    :type delta_sharing_coordinate: String
    :param model_name: Model Name that will be imported.
    :type model_name: String
    :rtype: None: No return type.

    :example:

.. tabs::
   .. code-tab:: arcuate_magic

       %%arcuate
        CREATE [OR REPLACE] MODEL model_name AS [PANDAS/SPARK] delta_sharing_coordinate

   .. code-tab:: python

        import arcuate
        import delta_sharing

        df = delta_sharing.load_as_pandas(delta_sharing_coordinate)

        # import the model
        import_models(df, model_name)


.. note:: This function doesnt have any real return type. It is either a success or it will throw an error.