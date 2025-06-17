# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Fabric Diagram Generator

# CELL ********************

DEBUG = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if DEBUG == False:
    exit_message = "Notebook execution skipped because DEBUG=False"
    notebookutils.notebook.exit(exit_message)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%pip install diagrams cairosvg

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import cairosvg

from IPython.display import (
    display,
    Image
)

from diagrams import Diagram, Edge, Cluster
from diagrams.azure.analytics import SynapseAnalytics
from diagrams.azure.storage import DataLakeStorage
from diagrams.azure.compute import FunctionApps
from diagrams.azure.database import SQLDatabases
from diagrams.custom import Custom

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

class FabricDiagramGenerator:

    def __init__(self, diagram_title: str, filename: str = "architecture_diagram"):
        self.diagram_title = diagram_title
        self.filename = filename
        self.fabric_icons_base_url = "https://raw.githubusercontent.com/FabricTools/fabric-icons/refs/heads/main/node_modules/%40fabric-msft/svg-icons/dist/svg"
        self.downloaded_icons = {}
        
    def download_fabric_icons(self, icon_names: List[str], size: int = 64) -> Dict[str, str]:
        for icon_name in icon_names:
            icon_svg = f"{icon_name}_{size}_item.svg"
            url = f"{self.fabric_icons_base_url}/{icon_svg}"
            icon_png = f"{icon_name}_{size}_item.png"
            
            try:
                cairosvg.svg2png(url=url, write_to=icon_png, dpi=1000)
                self.downloaded_icons[icon_name] = icon_png
            except Exception as e:
                print(f"Warning: Could not download {icon_name} icon: {e}")
                
        return self.downloaded_icons
    
    def create_diagram(self, 
                      clusters: Dict[str, Dict[str, any]], 
                      connections: List[Tuple[str, str, str]],
                      show: bool = False,
                      outformat: str = "png"):

        with Diagram(self.diagram_title, show=show, outformat=outformat, filename=self.filename):
            components = {}
            
            # create clusters recursively
            def create_cluster_components(cluster_data, parent_cluster=None):
                for cluster_name, cluster_info in cluster_data.items():
                    if parent_cluster:
                        # create nested cluster
                        with Cluster(cluster_name):
                            add_components(cluster_info)
                    else:
                        # create top-level cluster
                        with Cluster(cluster_name):
                            add_components(cluster_info)
            
            def add_components(cluster_info):
                # add components to current cluster
                if "components" in cluster_info:
                    for comp_id, (display_name, icon_type, icon_file) in cluster_info["components"].items():
                        if icon_type == "custom":
                            components[comp_id] = Custom(display_name, icon_file)
                        elif icon_type == "synapse":
                            components[comp_id] = SynapseAnalytics(display_name)
                        elif icon_type == "storage":
                            components[comp_id] = DataLakeStorage(display_name)
                        elif icon_type == "function":
                            components[comp_id] = FunctionApps(display_name)
                        elif icon_type == "sql":
                            components[comp_id] = SQLDatabases(display_name)
                        # Add more icon types as needed
                
                # handle subclusters
                if "subclusters" in cluster_info:
                    create_cluster_components(cluster_info["subclusters"], parent_cluster=True)
            
            # create all clusters and components
            create_cluster_components(clusters)
            
            # create connections
            for source_id, target_id, label in connections:
                if source_id in components and target_id in components:
                    components[source_id] >> Edge(label=label) >> components[target_id]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_fabric_architecture_diagram(
    title: str,
    fabric_icons: List[str],
    clusters_config: Dict[str, Dict[str, any]],
    connections_config: List[Tuple[str, str, str]],
    filename: str = "architecture",
):
    generator = FabricDiagramGenerator(title, filename)
    
    generator.download_fabric_icons(fabric_icons)
    
    generator.create_diagram(clusters_config, connections_config)
    
    return f"{filename}.png"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
