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
    number_from_string   a number written as a string in a list value (`[2, "2.5", 3]`)
                         becomes the number
    clear_optional       a value the schema does not allow is cleared where the schema
                         lets it be null or absent, its status then `not_reported`; a list
                         value is cleared whole, never shortened, and an entity in a list is
                         never dropped; only a reference list loses the element it refuses

Inside an anyOf the branch is the one the object's own `details_type` names, else the one
with the fewest errors. Nothing is invented: a required value with no single candidate
leaves the record refused.
"""

from __future__ import annotations

import copy
import functools
import math
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

#: A required reference and the record's list of what it can name.
REFERENCES = {"measure": "measures", "model_estimation": "model_estimations"}
NUMBERED = re.compile(r"^([a-z_]+?)\d+$")
#: More rounds than any refused production record needed (each round fixes one error).
ROUNDS = 200



@functools.lru_cache(maxsize=1)
def validator():
    """study_schema's extraction-record validator, built once."""
    import jsonschema
    from study_schema.jsonschema import load

    schema = load("extraction-record")
    return jsonschema.validators.validator_for(schema)(schema)


def _resolve(schema):
    defs = validator().schema["$defs"]
    while isinstance(schema, dict) and "$ref" in schema:
        schema = defs[schema["$ref"].rsplit("/", 1)[-1]]
    return schema


def _named(options, instance) -> Optional[int]:
    """The anyOf option whose `details_type` is the one `instance` names, else None."""
    named = instance.get("details_type") if isinstance(instance, dict) else None
    for i, option in enumerate(options if named else ()):
        if _resolve(option).get("properties", {}).get("details_type", {}).get("enum") == [named]:
            return i
    return None


def _leaves(error) -> List[Any]:
    """The errors that say what is wrong, an anyOf followed into one branch."""
    if error.validator not in ("anyOf", "oneOf") or not error.context:
        return [error]
    branches: Dict[int, list] = {}
    for sub in error.context:
        branches.setdefault(sub.relative_schema_path[0], []).append(sub)
    pick = _named(error.schema[error.validator], error.instance)
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
    schema, node = validator().schema, record
    for step in path[:-1]:
        schema = _resolve(schema)
        if isinstance(step, int):
            schema = schema.get("items", {})
        else:
            prop = _resolve(schema.get("properties", {}).get(step, {}))
            options = [_resolve(o) for o in prop.get("anyOf", [])]
            if options:
                pick = _named(options, node[step])
                objects = [o for o in options if "properties" in o]
                prop = options[pick] if pick is not None else (objects or options)[0]
            schema = prop
        node = node[step]
    return _resolve(schema)


def _number(value, kind) -> Optional[float]:
    """`value`, a string, as the number of JSON Schema type `kind`, or None."""
    if not isinstance(value, str) or kind not in ("number", "integer"):
        return None
    try:
        number = float(value.strip())
    except ValueError:
        return None
    if not math.isfinite(number) or (kind == "integer" and not number.is_integer()):
        return None
    return int(number) if number.is_integer() and kind == "integer" else number


def _not_reported(extracted: Dict[str, Any]) -> None:
    """An extracted value cleared: no value, and the status that says so."""
    extracted["value"] = None
    extracted["extraction_status"] = "not_reported"


def _extracted(holder, key) -> bool:
    """Whether `holder[key]` is an extracted value's `value`."""
    return key == "value" and isinstance(holder, dict) and "extraction_status" in holder


def _fix_one(record) -> Optional[str]:
    """Fix one error; the fix's name, or None when no error can be fixed."""
    for top in validator().iter_errors(record):
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
                parent = record
                for step in path[:-2]:
                    parent = parent[step]
                if len(path) >= 2 and _extracted(parent, path[-2]):
                    number = (_number(holder[key], e.validator_value)
                              if e.validator == "type" else None)
                    if number is not None:
                        holder[key] = number
                        return "number_from_string"
                    _not_reported(parent)
                    return "clear_optional"
                if e.schema.get("type") != "string":
                    continue
                del holder[key]   # a reference list: what is left still names entities
                return "clear_optional"
            schema = _holder_schema(record, path)
            prop = schema.get("properties", {}).get(key)
            if prop is None:
                continue
            if _nullable(prop):
                if holder[key] is None:
                    continue
                if _extracted(holder, key):
                    _not_reported(holder)
                else:
                    holder[key] = None
                return "clear_optional"
            if key not in schema.get("required", []):
                del holder[key]
                return "clear_optional"
    return None


def fix(record: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """(`record` made valid, the fixes that changed it), or (None, []) when it cannot be."""
    record = copy.deepcopy(record)
    fired: Counter = Counter()
    for _ in range(ROUNDS):
        if validator().is_valid(record):
            return record, sorted(fired)
        name = _fix_one(record)
        if name is None:
            break
        fired[name] += 1
    return None, []
