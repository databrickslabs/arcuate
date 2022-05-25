===============================
ML Model & Experiment Provider
===============================

export_experiment
*****************

.. function:: export_experiment

    Create a delta sharing share containing all the runs from an experiment.

    :param share_name: Share Name that will contain the table of models.
    :type share_name: String
    :param full_table_name: Table Name that will contain the models.
    :type table_name: String
    :param experiment_name: Experiment Name that will be shared.
    :type experiment_name: String
    :rtype: None: No return type.

    :example:

.. tabs::
   .. code-tab:: python

        import arcuate

        # export the experiment experiment_name to full_table_name, and add it to share_name
        export_experiments(experiment_name, full_table_name, share_name)


.. note:: This function doesnt have any real return type. It is either a success or it will throw an error.

export_model
************

.. function:: export_model

    Create a delta sharing share containing all versions of the requested model.

    :param share_name: Share Name that will contain the table with the model.
    :type share_name: String
    :param full_table_name: Table Name that will contain the model.
    :type table_name: String
    :param model_name: Model Name that will be shared.
    :type model_name: String
    :rtype: None: No return type.

    :example:

.. tabs::
   .. code-tab:: python

        import arcuate

        # export the model model_name to full_table_name, and add it to share_name
        export_models(model_name, full_table_name, share_name)


.. note:: This function doesnt have any real return type. It is either a success or it will throw an error.