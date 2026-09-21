"""The DDL notebook generator can regenerate the bundled library template
(templates/ddl/<mode>/lib.py.jinja) from python_libs, ordered by dependency."""

from ingen_fab.ddl_scripts.notebook_generator import NotebookGenerator


def test_dependency_injection_writes_mode_lib_template(tmp_path):
    templates_dir = tmp_path / "ddl"
    (templates_dir / "warehouse").mkdir(parents=True)

    generator = NotebookGenerator(
        generation_mode=NotebookGenerator.GenerationMode.warehouse,
        output_mode=NotebookGenerator.OutputMode.local,
        templates_dir=str(templates_dir),
    )
    generator.inject_python_libs_into_template()

    lib_template = templates_dir / "warehouse" / "lib.py.jinja"
    assert lib_template.exists(), "lib.py.jinja should be written under the mode folder"
    content = lib_template.read_text(encoding="utf-8")
    assert content.startswith("{% raw %}")
    assert "class warehouse_utils" in content
    assert "class ddl_utils" in content
    # nothing is written into the package's own template tree
    assert not (templates_dir / "lib.py.jinja").exists()
