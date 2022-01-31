import cloudpickle
import delta_sharing

def load_delta_sharing_ml_model(table_url: str):
    shared_models = delta_sharing.load_as_spark(table_url)
    return cloudpickle.loads(shared_models.collect()[0]["model_payload"])