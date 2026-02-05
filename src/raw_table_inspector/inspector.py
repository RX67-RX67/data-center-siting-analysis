from pathlib import Path

import pandas as pd


def _resolve_path(path_str: str, base_path: Path | None) -> Path:
    p = Path(path_str)
    if base_path is not None and not p.is_absolute():
        return base_path / p
    return p


def _read_csv(path: Path) -> pd.DataFrame:
    """Read CSV with encoding and delimiter fallback (UTF-16 BOM → tab, else comma)."""
    with open(path, "rb") as f:
        head = f.read(4)
    if head[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return pd.read_csv(path, encoding="utf-16", sep="\t", low_memory=False)
    for enc in ("utf-8", "latin-1", "cp1252"):
        for sep in (",", "\t"):
            try:
                return pd.read_csv(path, encoding=enc, sep=sep, low_memory=False)
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
    return pd.read_csv(path, encoding="utf-8", low_memory=False)


def _read_one(cfg: dict, base_path: Path | None) -> pd.DataFrame:
    """Load a single table from a config that has 'path' and 'format' (or per-item in sources)."""
    path = _resolve_path(cfg["path"], base_path)
    if not path.exists():
        raise FileNotFoundError(f"Data not found: {path}")
    fmt = cfg.get("format", "csv").lower()
    if fmt == "csv":
        return _read_csv(path)
    if fmt in ("xlsx", "xls"):
        engine = cfg.get("engine", "openpyxl")
        kwargs = {"engine": engine}
        if "sheet" in cfg:
            kwargs["sheet_name"] = cfg["sheet"]
        if "skiprows" in cfg:
            kwargs["skiprows"] = cfg["skiprows"]
        return pd.read_excel(path, **kwargs)
    raise ValueError(f"Unsupported format: {fmt}")


def parse_config(cfg: dict, base_path: Path | None = None) -> pd.DataFrame:
    """
    Load raw table(s) from a source config.
    Supports single-source (path + format) or multi-source (sources list, optional concat).
    Paths are resolved against base_path when provided.
    """
    if "sources" in cfg:
        dfs = [_read_one(s, base_path) for s in cfg["sources"]]
        if cfg.get("concat", False):
            return pd.concat(dfs, ignore_index=True)
        return dfs[0] if len(dfs) == 1 else pd.concat(dfs, ignore_index=True)
    return _read_one(cfg, base_path)


def inspect_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Return a small DataFrame listing each column and its dtype."""
    return (
        df.dtypes.astype(str)
        .reset_index()
        .rename(columns={"index": "column", 0: "dtype"})
    )
