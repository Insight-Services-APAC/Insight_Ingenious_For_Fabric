from ingen_fab.config_utils.variable_lib import VariableLibraryUtils


vlu = VariableLibraryUtils(
    environment="development",
    project_path="sample_project",
)

# Call inject_variables_into_template with specific parameters:
# - output_dir=None for in-place (default), or specified output directory
# - replace_placeholders=False (only inject code, don't replace {{varlib:...}})
# - inject_code=True (inject code between markers)
vlu.inject_variables_into_python_libs(
    replace_placeholders=False,  # Don't replace {{varlib:...}} placeholders
    inject_code=True             # Only inject code between markers
)