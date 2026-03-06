from pathlib import Path

from audits_punishment.paths import (
    clean_dir,
    docs_dir,
    ensure_data_dirs,
    interim_dir,
    project_data_dir,
    raw_dir,
    repo_root,
)


def test_repo_root_exists() -> None:
    root = repo_root()
    assert root.exists()
    assert (root / "src").exists()


def test_data_paths_resolve_and_exist(monkeypatch) -> None:
    monkeypatch.setenv("PROJECT_DATA_DIR", "data")
    ensure_data_dirs()

    assert project_data_dir().name == "data"
    assert raw_dir().exists()
    assert interim_dir().exists()
    assert clean_dir().exists()


def test_docs_path_exists() -> None:
    path = docs_dir()
    assert isinstance(path, Path)
    assert path.exists()
