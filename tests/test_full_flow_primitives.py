from __future__ import annotations

import json
import math
import unittest

from pathfinder.simulator._full_flow_primitives import (
    LOWER_SHA256_PATTERN,
    W4_IDENTIFIER_PATTERN,
    canonical_json_bytes,
    canonical_json_lines_bytes,
    checksum_manifest_bytes,
    checked_identifier,
    checked_lower_sha256,
    pretty_json_bytes,
    sha256_hex,
    strict_json_loads,
)


class BoundaryError(ValueError):
    pass


class FullFlowPrimitiveTest(unittest.TestCase):
    def test_json_encodings_are_byte_stable(self) -> None:
        value = {"z": "雪", "a": [1, True, None]}

        self.assertEqual(
            canonical_json_bytes(value),
            '{"a":[1,true,null],"z":"雪"}'.encode("utf-8"),
        )
        self.assertEqual(
            pretty_json_bytes(value),
            (
                '{\n'
                '  "a": [\n'
                '    1,\n'
                '    true,\n'
                '    null\n'
                '  ],\n'
                '  "z": "雪"\n'
                '}\n'
            ).encode("utf-8"),
        )
        self.assertEqual(
            canonical_json_lines_bytes([{"b": 2}, {"a": "雪"}]),
            b'{"b":2}\n' + '{"a":"雪"}\n'.encode("utf-8"),
        )

    def test_canonical_error_translation_preserves_type_and_message(self) -> None:
        with self.assertRaisesRegex(BoundaryError, "domain canonical failure") as ctx:
            canonical_json_bytes(
                {"value": math.nan},
                error_type=BoundaryError,
                error_message="domain canonical failure",
            )

        self.assertIsInstance(ctx.exception.__cause__, ValueError)

        with self.assertRaisesRegex(BoundaryError, "domain pretty failure") as ctx:
            pretty_json_bytes(
                {"value": math.nan},
                error_type=BoundaryError,
                error_message="domain pretty failure",
            )

        self.assertIsInstance(ctx.exception.__cause__, ValueError)

    def test_checksum_manifest_is_sorted_and_byte_stable(self) -> None:
        documents = {"z.json": b"z", "a.json": b"a"}

        self.assertEqual(
            checksum_manifest_bytes(documents),
            (
                "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785"
                "afee48bb  a.json\n"
                "594e519ae499312b29433b7dd8a97ff068defcba9755b6d5d00e84"
                "c524d67b06  z.json\n"
            ).encode("utf-8"),
        )
        self.assertEqual(
            checksum_manifest_bytes(documents, encoding="ascii"),
            checksum_manifest_bytes(documents),
        )

    def test_sha256_and_shared_patterns_preserve_w4_contract(self) -> None:
        digest = sha256_hex(b"pathfinder")

        self.assertEqual(
            digest,
            "0e43650b148e1557def21ef7ae16ebd8f7c21ccfa676e0d9f64e042681855970",
        )
        self.assertIsNotNone(LOWER_SHA256_PATTERN.fullmatch(digest))
        self.assertIsNotNone(W4_IDENTIFIER_PATTERN.fullmatch("trial|D7:r0001"))

    def test_checked_identifier_uses_caller_error_contract(self) -> None:
        self.assertEqual(
            checked_identifier(
                "trial|D7:r0001",
                "trial_id",
                error_type=BoundaryError,
            ),
            "trial|D7:r0001",
        )
        with self.assertRaisesRegex(BoundaryError, "trial_id is invalid"):
            checked_identifier(
                "contains space",
                "trial_id",
                error_type=BoundaryError,
            )

    def test_checked_digest_supports_exact_domain_wording(self) -> None:
        digest = "a" * 64
        self.assertEqual(
            checked_lower_sha256(
                digest,
                "plan_sha256",
                error_type=BoundaryError,
            ),
            digest,
        )
        with self.assertRaisesRegex(
            BoundaryError,
            "plan_sha256 is not a lowercase SHA-256 digest",
        ):
            checked_lower_sha256(
                "A" * 64,
                "plan_sha256",
                error_type=BoundaryError,
                message="plan_sha256 is not a lowercase SHA-256 digest",
            )

    def test_strict_json_rejects_duplicate_keys_and_nonfinite_numbers(self) -> None:
        kwargs = {
            "error_type": BoundaryError,
            "duplicate_key_message": lambda key: f"duplicate key: {key}",
            "nonfinite_number_message": lambda token: f"nonfinite: {token}",
        }

        self.assertEqual(
            strict_json_loads(b'{"value":1}', **kwargs),
            {"value": 1},
        )
        with self.assertRaisesRegex(BoundaryError, "duplicate key: value"):
            strict_json_loads(b'{"value":1,"value":2}', **kwargs)
        with self.assertRaisesRegex(BoundaryError, "nonfinite: NaN"):
            strict_json_loads(b'{"value":NaN}', **kwargs)
        with self.assertRaises(json.JSONDecodeError):
            strict_json_loads(b'{"value":', **kwargs)


if __name__ == "__main__":
    unittest.main()
