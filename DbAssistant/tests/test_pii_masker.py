"""Tests for PII masking before AI prompt submission."""

from ai_query.pii_masker import mask_pii
from ai_query.agent import AIQueryAgent


class TestPIIMasker:
    def test_email_masked(self):
        r = mask_pii("Contact user@example.com for access")
        assert "[REDACTED:EMAIL]" in r.text
        assert "user@example.com" not in r.text
        assert r.masked

    def test_password_assignment_masked(self):
        r = mask_pii("connect with password=SuperSecret123")
        assert "[REDACTED:SECRET]" in r.text
        assert "SuperSecret123" not in r.text

    def test_quoted_secret_with_spaces_masked(self):
        r = mask_pii('connect with password="super secret with spaces"')
        assert 'password="[REDACTED:SECRET]"' in r.text
        assert "super secret" not in r.text

    def test_api_key_masked(self):
        r = mask_pii("use sk-abcdefghijklmnopqrstuvwxyz1234567890")
        assert "[REDACTED:API_KEY]" in r.text

    def test_bearer_token_masked(self):
        r = mask_pii("Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abc.def")
        assert "[REDACTED:TOKEN]" in r.text

    def test_plain_text_unchanged(self):
        r = mask_pii("show all active customers")
        assert r.text == "show all active customers"
        assert not r.masked

    def test_url_credential_masked(self):
        r = mask_pii("mysql://admin:secretpass@db.example.com:3306/app")
        assert "[REDACTED:PASSWORD]" in r.text
        assert "secretpass" not in r.text


class TestAgentMaskIntegration:
    def test_agent_masks_when_enabled(self):
        agent = AIQueryAgent()
        agent.set_mask_pii(True)
        out = agent.mask_text_for_ai("email me at alice@corp.com")
        assert "[REDACTED:EMAIL]" in out

    def test_agent_skips_when_disabled(self):
        agent = AIQueryAgent()
        agent.set_mask_pii(False)
        out = agent.mask_text_for_ai("email me at alice@corp.com")
        assert out == "email me at alice@corp.com"
