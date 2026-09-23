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


def _two_free_gpus(monkeypatch) -> None:
    monkeypatch.setattr(
        dc.subprocess, "run", lambda *a, **k: _Completed("24000\n24000\n")
    )


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
    monkeypatch.setattr(dc.subprocess, "run", lambda *a, **k: _Completed("100\n100\n"))

    def _fail() -> tuple[bool, str]:
        raise AssertionError("probed torch despite no usable card")

    monkeypatch.setattr(dc, "_torch_can_use_cuda", _fail)

    assert dc.usable_cuda_devices() == []
