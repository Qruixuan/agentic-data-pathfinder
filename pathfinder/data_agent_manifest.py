from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping


DATA_AGENT_MANIFEST_VERSION = "pathfinder.data-agent-manifest/v1alpha1"
DATA_OBJECT_CATALOG_VERSION = "pathfinder.data-object-catalog/v1alpha1"


class DataAgentManifestError(ValueError):
    """Raised when a Data Agent manifest is invalid."""


class DataAgentManifestLookupError(LookupError):
    """Raised when a requested representation has no manifest entry."""


class DataAgentBindingMismatchError(RuntimeError):
    """Raised when a request does not match the configured plan binding."""


@dataclass(frozen=True)
class DataAgentBindingSpec:
    location: str | None
    minimum_latency_ms: float
    realized_cost: float
    cache_hit: bool | None
    path: Path | None = None


@dataclass(frozen=True)
class DataAgentRepresentationSpec:
    representation_id: str
    kind: str
    media_type: str
    path: Path | None
    default_binding: DataAgentBindingSpec
    plan_bindings: dict[str, DataAgentBindingSpec]


@dataclass(frozen=True)
class DataAgentObjectRepresentationSpec:
    path: Path
    plan_paths: dict[str, Path]


@dataclass(frozen=True)
class DataAgentObjectCatalog:
    source_path: Path
    catalog_version: str
    objects: dict[str, dict[str, DataAgentObjectRepresentationSpec]]

    def resolve_path(
        self,
        *,
        object_id: str,
        representation_id: str,
        plan_id: str,
    ) -> Path:
        representations = self.objects.get(object_id)
        if representations is None:
            raise DataAgentManifestLookupError(
                f"object is not configured: {object_id}"
            )
        representation = representations.get(representation_id)
        if representation is None:
            raise DataAgentManifestLookupError(
                "object representation is not configured: "
                f"{object_id}/{representation_id}"
            )
        return representation.plan_paths.get(plan_id, representation.path)


@dataclass(frozen=True)
class ResolvedDataAgentAccess:
    object_id: str | None
    representation_id: str
    kind: str
    media_type: str
    path: Path
    location: str
    minimum_latency_ms: float
    realized_cost: float
    cache_hit: bool | None


@dataclass(frozen=True)
class DataAgentManifest:
    source_path: Path
    node_id: str
    require_plan_binding: bool
    representations: dict[str, DataAgentRepresentationSpec]
    object_catalog: DataAgentObjectCatalog | None = None

    def resolve(
        self,
        *,
        plan_id: str,
        object_id: str | None = None,
        representation_id: str,
        requested_location: str,
    ) -> ResolvedDataAgentAccess:
        representation = self.representations.get(representation_id)
        if representation is None:
            raise DataAgentManifestLookupError(
                f"representation is not configured: {representation_id}"
            )
        binding = representation.plan_bindings.get(plan_id)
        if binding is None:
            if self.require_plan_binding:
                raise DataAgentManifestLookupError(
                    "plan binding is not configured: "
                    f"{plan_id}/{representation_id}"
                )
            binding = representation.default_binding
        if (
            binding.location is not None
            and binding.location != requested_location
        ):
            raise DataAgentBindingMismatchError(
                "requested location does not match the Data Agent manifest: "
                f"requested={requested_location}, "
                f"configured={binding.location}"
            )
        if self.object_catalog is not None:
            if object_id is None:
                raise DataAgentManifestLookupError(
                    "object_id is required when an object catalog is configured"
                )
            path = self.object_catalog.resolve_path(
                object_id=object_id,
                representation_id=representation_id,
                plan_id=plan_id,
            )
        else:
            path = binding.path or representation.path
        if path is None:
            raise DataAgentManifestLookupError(
                f"representation has no file path: {representation_id}"
            )
        return ResolvedDataAgentAccess(
            object_id=object_id,
            representation_id=representation_id,
            kind=representation.kind,
            media_type=representation.media_type,
            path=path,
            location=binding.location or requested_location,
            minimum_latency_ms=binding.minimum_latency_ms,
            realized_cost=binding.realized_cost,
            cache_hit=binding.cache_hit,
        )


def load_data_agent_manifest(path: str | Path) -> DataAgentManifest:
    source_path = Path(path).resolve()
    try:
        raw = json.loads(source_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise DataAgentManifestError(
            f"cannot read Data Agent manifest {source_path}: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise DataAgentManifestError(
            f"Data Agent manifest is not valid JSON: {exc}"
        ) from exc
    if not isinstance(raw, Mapping):
        raise DataAgentManifestError(
            "Data Agent manifest root must be an object"
        )
    if raw.get("schema_version") != DATA_AGENT_MANIFEST_VERSION:
        raise DataAgentManifestError(
            "Data Agent manifest has an unsupported schema_version"
        )
    node_id = _nonempty_string(raw, "node_id")
    require_plan_binding = raw.get("require_plan_binding", True)
    if not isinstance(require_plan_binding, bool):
        raise DataAgentManifestError(
            "Data Agent manifest require_plan_binding must be a boolean"
        )
    raw_representations = raw.get("representations")
    if not isinstance(raw_representations, Mapping) or not raw_representations:
        raise DataAgentManifestError(
            "Data Agent manifest representations must be a non-empty object"
        )
    object_catalog_path = raw.get("object_catalog_path")
    representations: dict[str, DataAgentRepresentationSpec] = {}
    for representation_id, value in raw_representations.items():
        if not isinstance(representation_id, str) or not representation_id:
            raise DataAgentManifestError(
                "Data Agent representation IDs must be non-empty strings"
            )
        if not isinstance(value, Mapping):
            raise DataAgentManifestError(
                f"representation {representation_id} must be an object"
            )
        representations[representation_id] = _parse_representation(
            source_path.parent,
            representation_id,
            value,
            require_path=object_catalog_path is None,
        )
    object_catalog = None
    if object_catalog_path is not None:
        resolved_catalog_path = _optional_path(
            source_path.parent,
            object_catalog_path,
        )
        assert resolved_catalog_path is not None
        object_catalog = load_data_object_catalog(resolved_catalog_path)
        unknown_representations = {
            representation_id
            for object_representations in object_catalog.objects.values()
            for representation_id in object_representations
            if representation_id not in representations
        }
        if unknown_representations:
            raise DataAgentManifestError(
                "object catalog references representations absent from the "
                "Data Agent manifest: "
                + ", ".join(sorted(unknown_representations))
            )
    return DataAgentManifest(
        source_path=source_path,
        node_id=node_id,
        require_plan_binding=require_plan_binding,
        representations=representations,
        object_catalog=object_catalog,
    )


def load_data_object_catalog(path: str | Path) -> DataAgentObjectCatalog:
    source_path = Path(path).resolve()
    try:
        raw = json.loads(source_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise DataAgentManifestError(
            f"cannot read Data Agent object catalog {source_path}: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise DataAgentManifestError(
            f"Data Agent object catalog is not valid JSON: {exc}"
        ) from exc
    if not isinstance(raw, Mapping):
        raise DataAgentManifestError(
            "Data Agent object catalog root must be an object"
        )
    if raw.get("schema_version") != DATA_OBJECT_CATALOG_VERSION:
        raise DataAgentManifestError(
            "Data Agent object catalog has an unsupported schema_version"
        )
    catalog_version = _nonempty_string(raw, "catalog_version")
    raw_objects = raw.get("objects")
    if not isinstance(raw_objects, Mapping) or not raw_objects:
        raise DataAgentManifestError(
            "Data Agent object catalog objects must be a non-empty object"
        )
    objects: dict[str, dict[str, DataAgentObjectRepresentationSpec]] = {}
    for object_id, raw_object in raw_objects.items():
        if not isinstance(object_id, str) or not object_id.strip():
            raise DataAgentManifestError(
                "Data Agent object IDs must be non-empty strings"
            )
        if not isinstance(raw_object, Mapping):
            raise DataAgentManifestError(
                f"object {object_id} must be an object"
            )
        raw_representations = raw_object.get("representations")
        if not isinstance(raw_representations, Mapping) or not raw_representations:
            raise DataAgentManifestError(
                f"object {object_id} representations must be a non-empty object"
            )
        parsed_representations: dict[
            str, DataAgentObjectRepresentationSpec
        ] = {}
        for representation_id, raw_representation in raw_representations.items():
            if not isinstance(representation_id, str) or not representation_id:
                raise DataAgentManifestError(
                    f"object {object_id} has an invalid representation ID"
                )
            if not isinstance(raw_representation, Mapping):
                raise DataAgentManifestError(
                    f"object representation {object_id}/{representation_id} "
                    "must be an object"
                )
            object_path = _optional_path(
                source_path.parent,
                raw_representation.get("path"),
            )
            if object_path is None:
                raise DataAgentManifestError(
                    f"object representation {object_id}/{representation_id} "
                    "must declare path"
                )
            raw_plan_paths = raw_representation.get("plan_paths", {})
            if not isinstance(raw_plan_paths, Mapping):
                raise DataAgentManifestError(
                    f"object representation {object_id}/{representation_id} "
                    "plan_paths must be an object"
                )
            plan_paths: dict[str, Path] = {}
            for plan_id, raw_plan_path in raw_plan_paths.items():
                if not isinstance(plan_id, str) or not plan_id:
                    raise DataAgentManifestError(
                        f"object representation {object_id}/{representation_id} "
                        "has an invalid plan ID"
                    )
                plan_path = _optional_path(
                    source_path.parent,
                    raw_plan_path,
                )
                if plan_path is None:
                    raise DataAgentManifestError(
                        f"object representation {object_id}/{representation_id} "
                        f"plan {plan_id} path is missing"
                    )
                plan_paths[plan_id] = plan_path
            parsed_representations[representation_id] = (
                DataAgentObjectRepresentationSpec(
                    path=object_path,
                    plan_paths=plan_paths,
                )
            )
        objects[object_id] = parsed_representations
    return DataAgentObjectCatalog(
        source_path=source_path,
        catalog_version=catalog_version,
        objects=objects,
    )


def _parse_representation(
    manifest_directory: Path,
    representation_id: str,
    raw: Mapping[str, Any],
    *,
    require_path: bool,
) -> DataAgentRepresentationSpec:
    kind = _nonempty_string(raw, "kind")
    if kind not in {"inline_text", "artifact_uri"}:
        raise DataAgentManifestError(
            f"representation {representation_id} has unsupported kind {kind}"
        )
    media_type = _nonempty_string(raw, "media_type")
    path = _optional_path(manifest_directory, raw.get("path"))
    raw_default = raw.get("default_binding", {})
    if not isinstance(raw_default, Mapping):
        raise DataAgentManifestError(
            f"representation {representation_id} default_binding "
            "must be an object"
        )
    default_binding = _parse_binding(
        manifest_directory,
        representation_id,
        "default_binding",
        raw_default,
    )
    raw_plan_bindings = raw.get("plan_bindings", {})
    if not isinstance(raw_plan_bindings, Mapping):
        raise DataAgentManifestError(
            f"representation {representation_id} plan_bindings "
            "must be an object"
        )
    plan_bindings: dict[str, DataAgentBindingSpec] = {}
    for plan_id, binding in raw_plan_bindings.items():
        if not isinstance(plan_id, str) or not plan_id:
            raise DataAgentManifestError(
                f"representation {representation_id} has an invalid plan ID"
            )
        if not isinstance(binding, Mapping):
            raise DataAgentManifestError(
                f"binding {representation_id}/{plan_id} must be an object"
            )
        plan_bindings[plan_id] = _parse_binding(
            manifest_directory,
            representation_id,
            plan_id,
            binding,
        )
    if require_path and path is None and default_binding.path is None and not any(
        binding.path is not None for binding in plan_bindings.values()
    ):
        raise DataAgentManifestError(
            f"representation {representation_id} has no configured path"
        )
    return DataAgentRepresentationSpec(
        representation_id=representation_id,
        kind=kind,
        media_type=media_type,
        path=path,
        default_binding=default_binding,
        plan_bindings=plan_bindings,
    )


def _parse_binding(
    manifest_directory: Path,
    representation_id: str,
    binding_id: str,
    raw: Mapping[str, Any],
) -> DataAgentBindingSpec:
    location = raw.get("location")
    if location is not None and (
        not isinstance(location, str) or not location.strip()
    ):
        raise DataAgentManifestError(
            f"binding {representation_id}/{binding_id} location "
            "must be a non-empty string or null"
        )
    minimum_latency_ms = _nonnegative_number(
        raw.get("minimum_latency_ms", 0.0),
        f"binding {representation_id}/{binding_id} minimum_latency_ms",
    )
    realized_cost = _nonnegative_number(
        raw.get("realized_cost", 0.0),
        f"binding {representation_id}/{binding_id} realized_cost",
    )
    cache_hit = raw.get("cache_hit")
    if cache_hit is not None and not isinstance(cache_hit, bool):
        raise DataAgentManifestError(
            f"binding {representation_id}/{binding_id} cache_hit "
            "must be a boolean or null"
        )
    return DataAgentBindingSpec(
        location=location,
        minimum_latency_ms=minimum_latency_ms,
        realized_cost=realized_cost,
        cache_hit=cache_hit,
        path=_optional_path(manifest_directory, raw.get("path")),
    )


def _optional_path(
    manifest_directory: Path,
    raw_path: Any,
) -> Path | None:
    if raw_path is None:
        return None
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise DataAgentManifestError(
            "Data Agent representation path must be a non-empty string"
        )
    path = Path(raw_path)
    if not path.is_absolute():
        path = manifest_directory / path
    return path.resolve()


def _nonempty_string(mapping: Mapping[str, Any], name: str) -> str:
    value = mapping.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DataAgentManifestError(
            f"Data Agent manifest {name} must be a non-empty string"
        )
    return value


def _nonnegative_number(value: Any, name: str) -> float:
    if (
        not isinstance(value, (int, float))
        or isinstance(value, bool)
        or not math.isfinite(float(value))
        or float(value) < 0
    ):
        raise DataAgentManifestError(
            f"{name} must be a finite non-negative number"
        )
    return float(value)
