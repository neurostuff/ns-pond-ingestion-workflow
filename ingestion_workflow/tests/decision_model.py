"""A real decision-model directory for tests: tiny, random, and answering what it is told.

`write_model(root, favour={"role": "anchor", ...})` writes what
`experiments/role_classifier/export_decision.py` writes, with a one-layer RoBERTa and a
word-level tokenizer. A head given in `favour` answers that value for every input (its
weights are zero and its bias picks the value); the other heads are random, so their
answers differ by input.
"""

from __future__ import annotations

import json
from pathlib import Path

from ingestion_workflow.services.set_roles import decision
from ingestion_workflow.services.set_roles.labels import (
    ANCHOR_KINDS,
    COORDINATE_ROLES,
    COORDINATE_SOURCES,
)

MAX_LENGTH = 48
WORDS = ["[ORIGIN]", "table", "text", "[NAME]", "[CAPTION]", "[ROWS]", "patients", "controls",
         "seed", "Lee", "et", "al.", "amygdala", "|", "-42", "18", "24"]
#: The value each binary head answers: index 1 is "coordinates" / "template".
BINARY = {"coordinates": {False: 0, True: 1}, "space": {"other": 0, "template": 1}}


def write_model(root: Path, favour=None, **card) -> Path:
    import torch
    from safetensors.torch import save_file
    from tokenizers import Tokenizer, models, pre_tokenizers
    from transformers import PreTrainedTokenizerFast, RobertaConfig, RobertaModel

    root = Path(root)
    torch.manual_seed(0)
    vocab = {w: i for i, w in enumerate(["<pad>", "<s>", "</s>", "<unk>", *WORDS])}
    tok = Tokenizer(models.WordLevel(vocab, unk_token="<unk>"))
    tok.pre_tokenizer = pre_tokenizers.WhitespaceSplit()
    PreTrainedTokenizerFast(tokenizer_object=tok, pad_token="<pad>", unk_token="<unk>",
                            bos_token="<s>", eos_token="</s>").save_pretrained(root / "tokenizer")
    config = RobertaConfig(vocab_size=len(vocab), hidden_size=16, num_hidden_layers=1,
                           num_attention_heads=2, intermediate_size=32,
                           max_position_embeddings=MAX_LENGTH + 2, pad_token_id=0)
    RobertaModel(config).save_pretrained(root / "encoder", safe_serialization=True)
    labels = {"role": COORDINATE_ROLES, "kind": ANCHOR_KINDS, "source": COORDINATE_SOURCES}
    heads = {}
    for head in decision.HEADS:
        n = 2 if head in BINARY else len(labels[head])
        weight, bias = torch.randn(n, 16), torch.randn(n)
        if head in (favour or {}):
            value = favour[head]
            pick = BINARY[head][value] if head in BINARY else labels[head].index(value)
            weight, bias = torch.zeros(n, 16), torch.full((n,), -10.0)
            bias[pick] = 10.0
        heads[f"{head}.weight"], heads[f"{head}.bias"] = weight, bias
    save_file(heads, str(root / decision.HEADS_FILE))
    (root / decision.CARD).write_text(json.dumps({
        "name": "role-test", "version": "v1", "max_length": MAX_LENGTH,
        "roles": list(COORDINATE_ROLES), "kinds": list(ANCHOR_KINDS),
        "sources": list(COORDINATE_SOURCES), "context_versions": dict(decision.CONTEXT_VERSIONS),
        **card,
    }))
    return root
