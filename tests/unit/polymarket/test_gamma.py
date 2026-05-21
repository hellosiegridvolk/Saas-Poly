import json

import pytest

from shared.polymarket.gamma import parse_clob_token_ids


class TestParseClobTokenIds:
    def test_double_encoded_string_decodes_to_list(self) -> None:
        raw = json.dumps(["0xabc", "0xdef"])
        assert parse_clob_token_ids(raw) == ["0xabc", "0xdef"]

    def test_already_parsed_list_passthrough(self) -> None:
        assert parse_clob_token_ids(["0xabc"]) == ["0xabc"]

    def test_rejects_none(self) -> None:
        with pytest.raises(ValueError):
            parse_clob_token_ids(None)

    def test_rejects_non_list_decoding(self) -> None:
        with pytest.raises(ValueError):
            parse_clob_token_ids(json.dumps({"k": "v"}))

    def test_rejects_non_string_entries(self) -> None:
        with pytest.raises(ValueError):
            parse_clob_token_ids(json.dumps([1, 2]))
