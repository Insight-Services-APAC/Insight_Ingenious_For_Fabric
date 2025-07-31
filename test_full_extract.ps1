python ./test_clean_lakehouse.py                                                                                   
python -m ingen_fab.cli package synthetic-data compile-generic-templates --target-environment lakehouse
python -m ingen_fab.cli package extract compile --include-samples --target-datastore lakehouse
python -m ingen_fab.cli ddl compile                                                                                
python ./sample_project/fabric_workspace_items/ddl_scripts/Lakehouses/00_all_lakehouses_orchestrator.Notebook/notebook-content.py                                                                                                   │
python ./sample_project/fabric_workspace_items/synthetic_data_generation/generic/generic_single_dataset_lakehouse.Notebook/notebook-content.py               
python ./sample_project/fabric_workspace_items/extract_generation_notebook.Notebook/notebook-content.py 