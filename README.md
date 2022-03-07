## Delta Sharing for MLFlow
### Model exchange via Delta Sharing
One of the main drivers for data sharing is the knowledge contained within the data. An alternative for sharing data in highly regulated environments can be sharing of models trained on the sensitive data.
Current options are not fit for purpose

Leveraging experiments & model in MLflow, combining it with Delta to leverage Delta Sharing capabilities to enable models exchange

Using Delta Sharing also allow sharing of relevant metadata such as training parameters, model accuracy, artifacts, etc.

![How it works](images/model_exchange.png)

### How to use:

Install the library

```python
pip install 
```

- Train model in Databricks (or elsewhere), store it in MLflow
- Export MLflow experiments & models to a Delta table and add it to a share, using IPython magic 
    ```python
    %%arcuate_export_experiment  
    CREATE SHARE share_name WITH TABLE table_name FROM EXPERIMENT experiment_name
    ```

    ```python
    %%arcuate_export_model
    CREATE SHARE share_name WITH TABLE table_name FROM MODEL model_name
    ```
- Recipient of this shared table can load it into MLflow seamlessly, using IPython magic:
    ```python
    %%arcuate_import_experiment
    CREATE EXPERIMENT [OVERWRITE] experiment_name AS [PANDAS/SPARK] delta_sharing_coordinate
    ```

    ```python
    %%arcuate_import_model
    CREATE MODEL [OVERWRITE] model_name AS [PANDAS/SPARK] delta_sharing_coordinate
    ```

- Users who prefer Python API instead of IPython magic can leverage these API calls:

  - On the provider side

    ```python
    import delta_sharing_mlflow

    # export the experiment experiment_name to table_name, and add it to share_name
    export_experiments(experiment_name, table_name, share_name)
    
    # export the model model_name to table_name, and add it to share_name
    export_models(model_name, table_name, share_name)    
    ```

  - On the recipient side

    ```python
    import delta_sharing_mlflow
    import delta_sharing

    df = delta_sharing.load_as_pandas(delta_sharing_coordinate)
    
    # import the shared table as experiment
    import_experiments(df, experiment_name)
    # or import the model
    import_models(df, model_name)
    ```

### Authors:
- Vuong Nguyen, Solutions Architect, vuong.nguyen@databricks.com
- Milos Colic, Sr. Solutions Architect, milos.colic@databricks.com
