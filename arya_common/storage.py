# created by Secil Sen
from __future__ import annotations

import gzip
import io
import os
from pathlib import Path
from typing import List, Tuple, Optional
import boto3

def write_jsonl(object_uri: str, lines: List[str], compression: str = "zstd") -> None:
    """
    Combines the given lines (JSON strings) with line breaks and writes them to object_uri.
    object_uri:      - s3://<bucket>/<key>    -> AWS S3 (put_object)
    Args:
    object_uri: Target URI ('s3://bucket/final/...').
    lines: JSON lines
    compression: 'gzip'
    Note:
    - This function does not "append", it writes all content at once.
    - finalize pipeline calls this function to produce the file atomically.
    """
    scheme, bucket, key_or_path = parse_uri(object_uri)
    payload = encode_lines(lines, compression=compression)

    if scheme == "out":
        # Write to local file system
        out_path = resolve_out_path(key_or_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("wb") as f:
            f.write(payload)
        return

    if scheme == "s3":
        if boto3 is None:
            raise RuntimeError(
                "boto3 not found. Install with 'pip install boto3' for S3 writing."
            )
        s3 = boto3.client("s3")
        # Note: Since finalize produces all content at once, we do 'overwrite' not 'append'.
        s3.put_object(Bucket=bucket, Key=key_or_path, Body=payload)
        return

    raise ValueError(f"Unsupported URI scheme: {scheme!r} (object_uri={object_uri})")


# Helpers

def parse_uri(uri: str) -> Tuple[str, Optional[str], str]:
    """
        Parses the URI.
        Returns:
            scheme: 's3'
            bucket: bucket name for s3
        path_or_key: key for s3
    """

    if uri.startswith("s3://"):
        rest = uri[len("s3://"):]
        # 'bucket/key...' separation
        parts = rest.split("/", 1)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            raise ValueError(f"S3 URI not valid: {uri}")
        bucket, key = parts[0], parts[1]
        return "s3", bucket, key

    raise ValueError(f"Non valid or unsupported URI: {uri}")


def resolve_out_path(rel_path: str) -> Path:
    """
    Maps out:// scheme to local file system.
    Default strategy: relative path to working directory.
    You can customize the root with ARYA_OUT_ROOT environment variable if desired.
    """
    root = os.getenv("ARYA_OUT_ROOT", ".")
    return Path(root).joinpath(rel_path)


def encode_lines(lines: List[str], compression: str = "gzip") -> bytes:
    """
    Joins lines with '\n' and produces bytes with selected compression.
    compression:  gzip or none
    """
    # All lines should end with newline; we also leave a newline at the end
    text = join_with_newlines(lines)
    raw = text.encode("utf-8")

    comp = compression.lower().strip()
    if comp == "none":
        return raw
    if comp == "gzip":
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(raw)
        return buf.getvalue()

    raise ValueError(f"Unsupported compression: {compression!r}")


def join_with_newlines(lines: List[str]) -> str:
    """
    Treats each of the given lines as a single line, joins them with '\n'.
    Also adds '\n' at the end (idempotent even if empty file).
    """
    if not lines:
        return "\n"
    # Assuming lines don't contain '\n', we don't strip to stay on the safe side.
    return "\n".join(lines) + "\n"