"""A source commitment must not depend on the checkout's line endings.

Windows checkouts store CRLF and Linux checkouts store LF.  Hashing the raw
bytes made the same commit produce two different admissions, so an admission
frozen on one host could never be reproduced on the other.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from pathfinder.simulator.full_flow_local_semantic_admission import (
    _sha256,
    _source_sha256,
)

SOURCE_LF = b'"""A module."""\n\n\ndef f() -> int:\n    return 1\n'
SOURCE_CRLF = SOURCE_LF.replace(b"\n", b"\r\n")


class SourceDigestPortabilityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def _write(self, name: str, payload: bytes) -> Path:
        path = self.root / name
        path.write_bytes(payload)
        return path

    def test_both_checkout_conventions_commit_to_one_digest(self) -> None:
        lf = self._write("lf.py", SOURCE_LF)
        crlf = self._write("crlf.py", SOURCE_CRLF)
        self.assertNotEqual(lf.read_bytes(), crlf.read_bytes())
        self.assertEqual(_source_sha256(lf), _source_sha256(crlf))

    def test_the_digest_is_the_lf_form(self) -> None:
        crlf = self._write("crlf.py", SOURCE_CRLF)
        self.assertEqual(_sha256(SOURCE_LF), _source_sha256(crlf))

    def test_a_real_content_change_still_changes_the_digest(self) -> None:
        # Normalising line endings must not weaken the commitment.
        original = self._write("a.py", SOURCE_LF)
        edited = self._write("b.py", SOURCE_LF.replace(b"return 1", b"return 2"))
        self.assertNotEqual(_source_sha256(original), _source_sha256(edited))

    def test_a_lone_carriage_return_is_not_rewritten(self) -> None:
        # Only the CRLF pair is a line-ending convention; a bare CR is content.
        bare = self._write("c.py", b"x = 1\ry = 2\n")
        self.assertEqual(_sha256(b"x = 1\ry = 2\n"), _source_sha256(bare))


if __name__ == "__main__":
    unittest.main()
