"""Hardware that torch cannot drive is not offered to Docling workers.

`nvidia-smi` reports the cards; it says nothing about whether this torch build
can use them. A torch compiled for a newer CUDA than the installed driver lists
every device and then refuses to initialise, and Docling answers that by falling
back to the CPU without a word -- an order of magnitude slower, no error.
"""

from __future__ import annotations

import ingestion_workflow.extractors.docling_convert as dc


class _Completed:
    def __init__(self, stdout: str, returncode: int = 0) -> None:
        self.stdout = stdout
        self.returncode = returncode
        self.stderr = ""


def _gpus(monkeypatch, *free_mib: int) -> None:
    rows = "".join(f"{i}, GPU-{i:08d}-0000-0000-0000-000000000000, {free}\n"
                   for i, free in enumerate(free_mib))
    monkeypatch.setattr(dc.subprocess, "run", lambda *a, **k: _Completed(rows))
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES", raising=False)


def _two_free_gpus(monkeypatch) -> None:
    _gpus(monkeypatch, 24000, 24000)


def test_devices_are_offered_when_torch_can_use_them(monkeypatch) -> None:
    _two_free_gpus(monkeypatch)
    monkeypatch.setattr(dc, "_torch_can_use_cuda", lambda: (True, ""))

    assert dc.usable_cuda_devices() == [0, 1]


def test_no_devices_when_torch_cannot_initialise(monkeypatch, caplog) -> None:
    _two_free_gpus(monkeypatch)
    monkeypatch.setattr(
        dc,
        "_torch_can_use_cuda",
        lambda: (False, "RuntimeError: The NVIDIA driver on your system is too old"),
    )

    with caplog.at_level("WARNING"):
        assert dc.usable_cuda_devices() == []

    assert "torch cannot use them" in caplog.text
    assert "driver on your system is too old" in caplog.text


def test_torch_is_not_probed_when_no_card_has_room(monkeypatch) -> None:
    """The probe costs an interpreter start, so it only runs if it could matter."""
    _gpus(monkeypatch, 100, 100)

    def _fail() -> tuple[bool, str]:
        raise AssertionError("probed torch despite no usable card")

    monkeypatch.setattr(dc, "_torch_can_use_cuda", _fail)

    assert dc.usable_cuda_devices() == []


#: beast's four cards (`nvidia-smi --query-gpu=index,uuid,memory.free`, 2026-11-17), all free.
BEAST = """0, GPU-b553ad73-ac49-7883-5cfb-6b71c637020d, 7800
1, GPU-8a63f34a-f4d9-a028-80c7-85cd9f0263bf, 7800
2, GPU-470ae6b1-4e52-464b-d84d-b7f306cdf5d5, 7800
3, GPU-90108dd3-2552-b26e-340e-55df1d30217f, 7800
"""


def test_only_the_cards_cuda_visible_devices_names_are_offered(monkeypatch) -> None:
    """A run held to GPUs 0-2 took GPU 3 too once another job freed it: a fourth MinerU
    worker, and beast ran out of memory."""
    monkeypatch.setattr(dc.subprocess, "run", lambda *a, **k: _Completed(BEAST))

    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,1,2")
    assert dc.free_cuda_devices(4000) == [0, 1, 2]
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "GPU-90108dd3-2552-b26e-340e-55df1d30217f, 1")
    assert dc.free_cuda_devices(4000) == [1, 3]
    monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
    assert dc.free_cuda_devices(4000) == []
    monkeypatch.delenv("CUDA_VISIBLE_DEVICES")
    assert dc.free_cuda_devices(4000) == [0, 1, 2, 3]
