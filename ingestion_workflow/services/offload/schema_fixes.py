"""Fixes for a record written to an older extraction-record schema than study_schema's.

The offloaded 27B was trained on a frozen schema; study_schema has moved since, so some of
its records name fields the schema no longer has, or hold a value in a form it no longer
takes. Each fix changes only what the schema refuses:

    merge_numbered_list  a second list beside one the schema has (`tasks2` beside `tasks`)
                         is merged into it
    drop_extra           a key the schema does not allow is dropped
    id_from_object       `{"local_id": X}` where an id string belongs becomes X
    only_candidate       a required `measure` or `model_estimation` the record has one of
                         references it
    clear_optional       a value the schema does not allow is cleared where the schema
                         lets it be null or absent; an entity in a list is never dropped

Inside an anyOf the branch is the one the object's own `details_type` names, else the one
with the fewest errors. Nothing is invented: a required value with no single candidate
leaves the record refused.
"""

from __future__ import annotations

import copy
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

#: A required reference and the record's list of what it can name.
REFERENCES = {"measure": "measures", "model_estimation": "model_estimations"}
NUMBERED = re.compile(r"^([a-z_]+?)\d+$")
#: More rounds than any refused production record needed (each round fixes one error).
ROUNDS = 200

_schema: Optional[Dict[str, Any]] = None
_validator = None


def _load():
    global _schema, _validator
    if _validator is None:
        import jsonschema
        from study_schema.jsonschema import load

        _schema = load("extraction-record")
        _validator = jsonschema.validators.validator_for(_schema)(_schema)
    return _schema, _validator


def _resolve(schema):
    defs = _load()[0]["$defs"]
    while isinstance(schema, dict) and "$ref" in schema:
        schema = defs[schema["$ref"].rsplit("/", 1)[-1]]
    return schema


def _leaves(error) -> List[Any]:
    """The errors that say what is wrong, an anyOf followed into one branch."""
    if error.validator not in ("anyOf", "oneOf") or not error.context:
        return [error]
    branches: Dict[int, list] = {}
    for sub in error.context:
        branches.setdefault(sub.relative_schema_path[0], []).append(sub)
    named = error.instance.get("details_type") if isinstance(error.instance, dict) else None
    pick = None
    if named:
        for i, option in enumerate(error.schema[error.validator]):
            if option.get("$ref", "").endswith("/" + named):
                pick = i
        if pick is not None and pick not in branches:
            return []
    if pick is None:
        pick = min(branches, key=lambda i: len(branches[i]))
    return [leaf for sub in branches[pick] for leaf in _leaves(sub)]


def _nullable(schema) -> bool:
    schema = _resolve(schema)
    if not isinstance(schema, dict):
        return False
    kind = schema.get("type")
    if kind == "null" or (isinstance(kind, list) and "null" in kind):
        return True
    return any(_nullable(o) for o in schema.get("anyOf", []) + schema.get("oneOf", []))


def _holder_schema(record, path):
    """The schema of the object that holds `path[-1]`."""
    schema, node = _load()[0], record
    for step in path[:-1]:
        schema = _resolve(schema)
        if isinstance(step, int):
            schema = schema.get("items", {})
        else:
            prop = _resolve(schema.get("properties", {}).get(step, {}))
            options = [_resolve(o) for o in prop.get("anyOf", [])]
            if options:
                value = node[step]
                named = value.get("details_type") if isinstance(value, dict) else None
                match = [o for o in options if named and
                         o.get("properties", {}).get("details_type", {}).get("enum") == [named]]
                objects = [o for o in options if "properties" in o]
                prop = (match or objects or options)[0]
            schema = prop
        node = node[step]
    return _resolve(schema)


def _fix_one(record) -> Optional[str]:
    """Fix one error; the fix's name, or None when no error can be fixed."""
    for top in _load()[1].iter_errors(record):
        for e in _leaves(top):
            path = list(e.absolute_path)
            if e.validator == "additionalProperties":
                allowed = e.schema.get("properties", {})
                for key in [k for k in e.instance if k not in allowed]:
                    base = NUMBERED.match(key)
                    base = base.group(1) if base else None
                    if (base in allowed and isinstance(e.instance[key], list)
                            and isinstance(e.instance.get(base) or [], list)):
                        have = {i.get("local_id") for i in e.instance.get(base) or []
                                if isinstance(i, dict)}
                        e.instance[base] = (e.instance.get(base) or []) + [
                            i for i in e.instance.pop(key)
                            if not (isinstance(i, dict) and i.get("local_id") in have)]
                        return "merge_numbered_list"
                    del e.instance[key]
                    return "drop_extra"
            if (e.validator == "type" and isinstance(e.instance, dict) and path
                    and isinstance(e.instance.get("local_id"), str)
                    and "string" in str(e.validator_value)):
                holder = record
                for step in path[:-1]:
                    holder = holder[step]
                holder[path[-1]] = e.instance["local_id"]
                return "id_from_object"
            if e.validator == "required":
                for key in re.findall(r"'([^']+)' is a required property", e.message):
                    pool = record.get(REFERENCES.get(key, "")) or []
                    if len(pool) == 1 and isinstance(pool[0], dict) and pool[0].get("local_id"):
                        e.instance[key] = pool[0]["local_id"]
                        return "only_candidate"
                continue
            if not path:
                continue
            holder = record
            for step in path[:-1]:
                holder = holder[step]
            key = path[-1]
            if isinstance(key, int):
                if isinstance(holder[key], (dict, list)):
                    continue
                del holder[key]
                return "clear_optional"
            schema = _holder_schema(record, path)
            prop = schema.get("properties", {}).get(key)
            if prop is None:
                continue
            if _nullable(prop):
                if holder[key] is None:
                    continue
                holder[key] = None
                return "clear_optional"
            if key not in schema.get("required", []):
                del holder[key]
                return "clear_optional"
    return None


def fix(record: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """(`record` made valid, the fixes that changed it), or (None, []) when it cannot be."""
    record = copy.deepcopy(record)
    validator = _load()[1]
    fired: Counter = Counter()
    for _ in range(ROUNDS):
        if validator.is_valid(record):
            return record, sorted(fired)
        name = _fix_one(record)
        if name is None:
            break
        fired[name] += 1
    return None, []
