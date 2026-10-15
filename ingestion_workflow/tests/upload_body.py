"""A vcr matcher for an upload's body that leaves out the hashes the catalog's stages derive."""

from __future__ import annotations

import json


def without_input_fingerprints(node):
    """`header.inputs[*].fingerprint` of every artifact in the body, blanked: those name the
    catalog artifacts a parse was built from, and change whenever any stage upstream of
    them changes how it fingerprints, which is no change to what is sent."""
    if isinstance(node, dict):
        header = node.get("header")
        if isinstance(header, dict):
            for ref in header.get("inputs") or []:
                ref["fingerprint"] = ""
        for value in node.values():
            without_input_fingerprints(value)
    elif isinstance(node, list):
        for value in node:
            without_input_fingerprints(value)
    return node


def _canonical(request) -> str:
    return json.dumps(without_input_fingerprints(json.loads(request.body)), sort_keys=True)


def same_body_but_input_fingerprints(recorded, replayed) -> None:
    assert _canonical(recorded) == _canonical(replayed)
