"""Raw web and API captures: exact response bytes plus one metadata row."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from urllib.parse import urlsplit

import polars as pl

from finresearch.cases import (
    MANIFEST_V2,
    Artifact,
    CaseContractError,
    CaseManifest,
    canonical_parameters_sha256,
    case_directory,
    read_manifest,
    resolve_relative_path,
)
from finresearch.data_contracts import (
    CAPTURE_KINDS,
    CAPTURE_PROVIDERS,
    RAW_CAPTURE_V1,
    DataContractError,
    canonical_request_body,
    validate_capture_url,
)
from finresearch.ingestion import (
    IngestionError,
    publish_artifact_bytes,
    publish_snapshot,
)

CAPTURE_BODY_KIND: Final = "raw.capture-body"
CAPTURE_PRODUCER: Final = "finresearch.data.capture"
CAPTURE_PRODUCER_VERSION: Final = "1"
_EXTENSIONS: Final = {
    "application/json": "json",
    "application/pdf": "pdf",
    "text/html": "html",
    "text/markdown": "md",
    "text/plain": "txt",
}
_FILE_MEDIA_TYPES: Final = {
    ".json": "application/json",
    ".pdf": "application/pdf",
    ".html": "text/html",
    ".htm": "text/html",
    ".md": "text/markdown",
    ".txt": "text/plain",
}
_SHA256_HEX = re.compile(r"[0-9a-f]{64}")
CAPTURE_REFERENCE_PATTERN: Final = re.compile(
    r"capture:([a-z0-9](?:[a-z0-9_.-]*[a-z0-9])?)"
)


@dataclass(frozen=True)
class CaptureReceipt:
    """Stable result metadata for one registered capture."""

    capture_id: str
    body_artifact_id: str
    body_path: Path
    metadata_path: Path
    content_sha256: str
    content_bytes: int
    retrieved_at: datetime


@dataclass(frozen=True)
class LoadedCapture:
    """A capture's metadata row and exact body bytes, hash-checked."""

    capture_id: str
    artifact: Artifact
    metadata: dict[str, object]
    body: bytes

    @property
    def provider(self) -> str:
        return str(self.metadata["provider"])

    @property
    def url(self) -> str:
        return str(self.metadata["url"])

    @property
    def request_body(self) -> str | None:
        value = self.metadata["request_body"]
        return None if value is None else str(value)

    @property
    def retrieved_at(self) -> datetime:
        value = self.metadata["retrieved_at"]
        if not isinstance(value, datetime):
            raise IngestionError(f"capture {self.capture_id} has no retrieved_at")
        return value


def require_manifest_v2(manifest: CaseManifest, command: str) -> None:
    """Token and capture workflows need v2 lineage."""
    if manifest.manifest_version != MANIFEST_V2:
        raise CaseContractError(f"{command} requires manifest v2; run case migrate")


def case_context(workspace: Path, case_id: str) -> tuple[Path, CaseManifest]:
    """Return an existing v2 case directory and its manifest."""
    case_dir = case_directory(workspace, case_id)
    if not case_dir.is_dir():
        raise CaseContractError(f"case not found: {case_id}")
    return case_dir, read_manifest(case_dir)


def timestamp_key(value: datetime) -> str:
    """Return the lowercase filename-safe form of a UTC timestamp."""
    return value.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ").lower()


PROVIDER_HOSTS: Final[dict[str, frozenset[str]]] = {
    "coingecko": frozenset({"api.coingecko.com"}),
    "defillama": frozenset(
        {"api.llama.fi", "coins.llama.fi", "stablecoins.llama.fi", "yields.llama.fi"}
    ),
    "hyperliquid": frozenset({"api.hyperliquid.xyz"}),
}


def validate_capture_provider(provider: str, url: str) -> None:
    """Reject a provider label that disagrees with the URL host.

    Captures are immutable, so a wrong label would otherwise be permanent and
    route the body to the wrong parser.
    """
    host = (urlsplit(url).hostname or "").lower()
    owner = next(
        (name for name, hosts in PROVIDER_HOSTS.items() if host in hosts), None
    )
    if provider == "web":
        if owner is not None:
            raise IngestionError(
                f"URL host {host} belongs to provider {owner}; use --provider {owner}"
            )
        return
    if owner != provider:
        expected = ", ".join(sorted(PROVIDER_HOSTS[provider]))
        raise IngestionError(
            f"provider {provider} requires a URL host in: {expected} (got {host})"
        )


def capture_key(method: str, url: str, request_body: str | None) -> str:
    """Return a readable host slug plus a 12-digit request hash."""
    host = (urlsplit(url).hostname or "host").lower()
    slug = re.sub(r"[^a-z0-9]+", "-", host).strip("-") or "host"
    digest = hashlib.sha256(
        "\n".join((method, url, request_body or "")).encode("utf-8")
    ).hexdigest()
    return f"{slug}-{digest[:12]}"


def publish_capture(
    case_dir: Path,
    manifest: CaseManifest,
    *,
    provider: str,
    kind: str,
    recorded_by: str,
    method: str,
    url: str,
    request_body: str | None,
    http_status: int | None,
    media_type: str,
    content: bytes,
    retrieved_at: datetime,
) -> CaptureReceipt:
    """Publish the body artifact, then its metadata row; reruns are idempotent."""
    require_manifest_v2(manifest, "captures")
    if not content:
        raise IngestionError("capture content must not be empty")
    if provider not in CAPTURE_PROVIDERS:
        raise IngestionError(
            f"provider must be one of: {', '.join(sorted(CAPTURE_PROVIDERS))}"
        )
    validate_capture_provider(provider, url)
    if kind not in CAPTURE_KINDS:
        raise IngestionError(
            f"capture kind must be one of: {', '.join(sorted(CAPTURE_KINDS))}"
        )
    if retrieved_at.utcoffset() is None:
        raise IngestionError("retrieved_at must be timezone-aware")
    retrieved_at = retrieved_at.astimezone(UTC)
    key = capture_key(method, url, request_body)
    stamp = timestamp_key(retrieved_at)
    content_sha256 = hashlib.sha256(content).hexdigest()
    body_id = f"{CAPTURE_BODY_KIND}.{key}.{stamp}"
    capture_id = f"{RAW_CAPTURE_V1.name}.{key}.{stamp}"
    frame = pl.DataFrame(
        [
            {
                "schema_version": RAW_CAPTURE_V1.version,
                "capture_id": capture_id,
                "body_artifact_id": body_id,
                "provider": provider,
                "capture_kind": kind,
                "recorded_by": recorded_by,
                "request_method": method,
                "url": url,
                "request_body": request_body,
                "http_status": http_status,
                "media_type": media_type,
                "content_sha256": content_sha256,
                "content_bytes": len(content),
                "retrieved_at": retrieved_at,
            }
        ],
        schema=RAW_CAPTURE_V1.schema,
    )
    RAW_CAPTURE_V1.validate(frame)
    parameters = {
        "capture_key": key,
        "capture_kind": kind,
        "content_sha256": content_sha256,
        "http_status": http_status,
        "media_type": media_type,
        "method": method,
        "provider": provider,
        "recorded_by": recorded_by,
        "request_body": request_body,
        "retrieved_at": stamp,
        "url": url,
    }
    body = publish_artifact_bytes(
        case_dir=case_dir,
        manifest=manifest,
        path_role="raw",
        kind=CAPTURE_BODY_KIND,
        schema_version=1,
        path_parts=("captures", provider, key),
        filename=f"{stamp}.{_EXTENSIONS.get(media_type, 'bin')}",
        entity_key=key,
        identity=stamp,
        content=content,
        producer=CAPTURE_PRODUCER,
        producer_version=CAPTURE_PRODUCER_VERSION,
        parameters_sha256=canonical_parameters_sha256(
            {**parameters, "contract": CAPTURE_BODY_KIND}
        ),
        input_artifact_ids=(),
        produced_at=retrieved_at,
    )
    metadata = publish_snapshot(
        case_dir=case_dir,
        manifest=read_manifest(case_dir),
        path_role="raw",
        frame=frame,
        contract=RAW_CAPTURE_V1,
        path_parts=("captures", provider, key),
        entity_key=key,
        identity=stamp,
        producer=CAPTURE_PRODUCER,
        producer_version=CAPTURE_PRODUCER_VERSION,
        parameters_sha256=canonical_parameters_sha256(
            {**parameters, "contract": RAW_CAPTURE_V1.identifier}
        ),
        input_artifact_ids=(body.artifact_id,),
        produced_at=retrieved_at,
    )
    return CaptureReceipt(
        capture_id=metadata.artifact_id,
        body_artifact_id=body.artifact_id,
        body_path=body.path,
        metadata_path=metadata.path,
        content_sha256=content_sha256,
        content_bytes=len(content),
        retrieved_at=retrieved_at,
    )


def register_capture(
    workspace: Path,
    case_id: str,
    source_file: Path,
    *,
    provider: str,
    kind: str,
    url: str,
    retrieved_at: datetime,
    method: str = "GET",
    request_body_file: Path | None = None,
    http_status: int | None = None,
    media_type: str | None = None,
    expect_sha256: str | None = None,
) -> CaptureReceipt:
    """Register agent-fetched bytes as a capture; the CLI computes the hash."""
    if not source_file.is_file():
        raise IngestionError(f"capture file must be a readable file: {source_file}")
    try:
        content = source_file.read_bytes()
        body_text = (
            None
            if request_body_file is None
            else request_body_file.read_text(encoding="utf-8")
        )
    except (OSError, UnicodeDecodeError) as exc:
        raise IngestionError(f"cannot read capture input: {exc}") from exc
    if expect_sha256 is not None:
        if _SHA256_HEX.fullmatch(expect_sha256) is None:
            raise IngestionError("--expect-sha256 must be 64 lowercase hex digits")
        actual = hashlib.sha256(content).hexdigest()
        if actual != expect_sha256:
            raise IngestionError(
                f"capture bytes differ from --expect-sha256: expected "
                f"{expect_sha256}, computed {actual}"
            )
    if method not in {"GET", "POST"}:
        raise IngestionError("method must be GET or POST")
    if kind == "api-response" and http_status is None:
        raise IngestionError("--http-status is required for an api-response capture")
    resolved_media_type = media_type or _FILE_MEDIA_TYPES.get(
        source_file.suffix.lower()
    )
    if resolved_media_type is None:
        raise IngestionError("cannot infer --media-type from the file extension")
    try:
        validate_capture_url(url)
        request_body = None if body_text is None else canonical_request_body(body_text)
    except DataContractError as exc:
        raise IngestionError(str(exc)) from exc
    case_dir, manifest = case_context(workspace, case_id)
    return publish_capture(
        case_dir,
        manifest,
        provider=provider,
        kind=kind,
        recorded_by="agent",
        method=method,
        url=url,
        request_body=request_body,
        http_status=http_status,
        media_type=resolved_media_type,
        content=content,
        retrieved_at=retrieved_at,
    )


def declared_captures(manifest: CaseManifest) -> dict[str, Artifact]:
    """Return every declared ``raw.capture.v1`` artifact by id."""
    return {
        artifact.artifact_id: artifact
        for artifact in manifest.artifacts
        if artifact.kind == RAW_CAPTURE_V1.name
        and artifact.schema_version == RAW_CAPTURE_V1.version
    }


def load_capture(
    case_dir: Path,
    manifest: CaseManifest,
    capture_artifact_id: str,
) -> LoadedCapture:
    """Read one declared capture and verify its body bytes against the row."""
    artifact = declared_captures(manifest).get(capture_artifact_id)
    if artifact is None:
        raise IngestionError(f"not a declared raw.capture.v1: {capture_artifact_id}")
    path = resolve_relative_path(case_dir, artifact.path, f"artifact {artifact.path}")
    try:
        frame = pl.read_parquet(path)
        RAW_CAPTURE_V1.validate(frame)
    except (OSError, pl.exceptions.PolarsError, DataContractError) as exc:
        raise IngestionError(
            f"capture {capture_artifact_id} is invalid: {exc}"
        ) from exc
    if frame.height != 1:
        raise IngestionError(f"capture {capture_artifact_id} must have one row")
    metadata = frame.row(0, named=True)
    body_id = str(metadata["body_artifact_id"])
    body_artifact = next(
        (item for item in manifest.artifacts if item.artifact_id == body_id), None
    )
    if (
        metadata["capture_id"] != capture_artifact_id
        or body_artifact is None
        or body_artifact.input_artifact_ids
        or artifact.input_artifact_ids != (body_id,)
    ):
        raise IngestionError(f"capture {capture_artifact_id} metadata is inconsistent")
    body_path = resolve_relative_path(
        case_dir, body_artifact.path, f"artifact {body_id}"
    )
    try:
        body = body_path.read_bytes()
    except OSError as exc:
        raise IngestionError(f"capture body {body_id} is not readable: {exc}") from exc
    if hashlib.sha256(body).hexdigest() != metadata["content_sha256"]:
        raise IngestionError(
            f"capture {capture_artifact_id} body bytes do not match content_sha256"
        )
    return LoadedCapture(capture_artifact_id, artifact, metadata, body)
