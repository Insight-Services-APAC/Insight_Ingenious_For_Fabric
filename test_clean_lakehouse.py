from ingen_fab.python_libs.common.config_utils import get_configs_as_object
from ingen_fab.python_libs.pyspark.lakehouse_utils import lakehouse_utils
configs = get_configs_as_object()
lh_utils = lakehouse_utils(target_lakehouse_id=configs.config_lakehouse_id,
                           target_workspace_id=configs.config_workspace_id)
lh_utils.drop_all_tables()
