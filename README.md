Mini project to demonstrate ability to share models stored in delta.
Authors:
- Vuong Nguyen, Solutions Architect, vuong.nguyen@databricks.com
- Milos Colic, Sr. Solutions Architect, milos.colic@databricks.com

Combine mlflow with delta to leverage delta sharing capabilities to exchange models.
Delta table containing models will have maping of schema to match the data stored in our mlflow UI.
E2E idea:
- train model in databricks (or elswhere) store it in mlflow
- export mlflow model into delta (ie. "EXPORT MODEL model_id TO SHARE share_id")
- ingest model into another environment 
- load it into mlflow (ie. with delta "IMPORT MODEL model_id to experiment_id")

TODO: add visuals.


Name: arcuate delta - the wide fan shaped river delta - mlflow model echange will have wide impact.

