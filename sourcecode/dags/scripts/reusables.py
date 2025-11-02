import os
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable
from functools import lru_cache
import json
import yaml
import pyarrow.parquet as pq



@lru_cache(maxsize=None)
def parquet_columns(filepath: str | os.PathLike) -> list[str]:
    """ambil list nama column dari file parquet, pakai pyarrow, dengan cache
    supaya optimal bila dilakukan berulang
    @param filepath: str or os.PathLike, path file parquet
    return list of column names
    """
    schema = pq.read_schema(filepath)
    return schema.names

@lru_cache(maxsize=None)
def get_base_dir(what_exists_in_base_dir='.git') -> Path:
    """
    cari parent directory sampai ada file/dir `what_exists_in_base_dir` disitu
    @param what_exists_in_base_dir: str, nama file/dir yang dicari di base dir
        if omitted default: '.git'
    @return Path
    """
    p = Path(__file__).resolve()
    # cari sampai ketemu what_exists_in_base_dir
    while not (p / what_exists_in_base_dir).exists():
        if p.parent == p:
            # sudah root, tidak ketemu .git
            raise FileNotFoundError(
                f"Tidak ditemukan {what_exists_in_base_dir} di parent " \
                "directories.  jangan tanpa param di luar repo git")
        p = p.parent
    return p

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# error-only logger
error_logger = logging.getLogger("projecterror_logger")
error_logger.setLevel(logging.ERROR)

def configure_error_logger(log_file) -> None:
    """setup object `error_logger` dari module ini pakai RotatingFileHandler
    write to log_file
    @return error_logger object
    """
    # Avoid duplicate handlers when reloaded
    if any([
        (isinstance(h, RotatingFileHandler) and
        getattr(h, "baseFilename", None) == str(log_file))
        for h in error_logger.handlers
    ]):
        return

    handler = RotatingFileHandler(
        filename=str(log_file), maxBytes=5 * 1024 * 1024, backupCount=5)
    handler.setLevel(logging.ERROR)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    handler.setFormatter(fmt)
    error_logger.addHandler(handler)
    # Propagate to root logger supaya muncul juga di Airflow task logs.
    error_logger.propagate = True
    return error_logger

def configure_logger(log_file) -> None:
    """setup object `logger` dari module ini dengan RotatingFileHandler
    write to log_file
    return logger object
    """
    # Avoid duplicate handlers when reloaded
    if any([
        (isinstance(h, RotatingFileHandler) and
        getattr(h, "baseFilename", None) == str(log_file))
        for h in logger.handlers
    ]):
        return

    handler = RotatingFileHandler(
        filename=str(log_file), maxBytes=5 * 1024 * 1024, backupCount=5)
    handler.setLevel(logging.INFO)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    # Propagate to root logger supaya muncul juga di Airflow task logs.
    logger.propagate = True
    return logger



def ensure_dir(path: str | os.PathLike) -> Path:
    """make direktori & parent if not exist
    using mkdir(parents=True, exist_ok=True)
    @param path: (str or os.PathLike) path direktori to make or check
    return pathlib.Path directory 
    """
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def assert_files_exist(
        raw_dir: Path,
        files: Iterable[str]):
    """check apakah semua file ada di raw_dir
    @param dir_path: (pathlib.Path) direktori yang di cek 
    @param files: list of filenames (str)
    """
    missing = [f for f in files if not (raw_dir / f).exists()]
    if missing:
        raise FileNotFoundError(
            f"Missing files: {missing}. di direktori {raw_dir}")
    empty = [f for f in files if (raw_dir / f).stat().st_size == 0]
    if empty:
        raise Exception(f"File kosong: {empty}")


@lru_cache(maxsize=1)
def load_yaml(
        yaml_file: Path
    ) -> Dict[str, Any]:
    """load yaml_file, atau bila omited load config/config.yaml 
    cached pakai lru_cache
    @param yaml_file: load file tersebut
    returns empty dict if file invalid
    """
    cfg_path = yaml_file
    if not cfg_path.exists():
        logger.warning("Config file not found: %s", cfg_path)
        return {}
    try:
        with cfg_path.open() as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error("Failed to load config %s: %s", cfg_path, e)
        return {}
