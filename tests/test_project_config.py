import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ingen_fab import load_project_config


def test_load_current_directory(tmp_path, monkeypatch):
    cfg = tmp_path / "project.yml"
    cfg.write_text("a: 1")
    monkeypatch.chdir(tmp_path)
    assert load_project_config() == {"a": 1}


def test_load_env_variable(tmp_path, monkeypatch):
    cfg = tmp_path / "project.yml"
    cfg.write_text("b: 2")
    monkeypatch.setenv("IGEN_FAB_CONFIG", str(tmp_path))
    assert load_project_config() == {"b": 2}


def test_load_option_path(tmp_path):
    cfg = tmp_path / "project.yml"
    cfg.write_text("c: 3")
    assert load_project_config(cfg.parent) == {"c": 3}
