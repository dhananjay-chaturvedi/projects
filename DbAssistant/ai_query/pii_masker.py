"""
Detect and mask PII / secrets in text before it is sent to external AI backends.

Masking is applied at prompt-build time only; local UI and stored conversation
history keep the original text unless callers choose otherwise.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class PIIMaskResult:
    text: str
    masked: bool
    findings: list[str] = field(default_factory=list)


def _sub(pattern: re.Pattern, repl: str, text: str, kind: str, findings: list[str]) -> str:
    def _repl(m: re.Match) -> str:
        findings.append(kind)
        if callable(repl):
            return repl(m)
        return repl

    return pattern.sub(_repl, text)


def mask_pii(text: str) -> PIIMaskResult:
    """Return *text* with sensitive segments replaced by typed redaction tokens."""
    if not text:
        return PIIMaskResult(text=text or "", masked=False)

    findings: list[str] = []
    out = text

    # PEM private keys / certificates (multiline)
    pem = re.compile(
        r"-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----[\s\S]*?"
        r"-----END (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----",
        re.MULTILINE,
    )
    if pem.search(out):
        findings.append("private_key")
        out = pem.sub("[REDACTED:PRIVATE_KEY]", out)

    # JWT (header.payload.signature)
    jwt = re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b")
    out = _sub(jwt, "[REDACTED:JWT]", out, "jwt", findings)

    # AWS access key id
    aws_key = re.compile(r"\b(AKIA[0-9A-Z]{16})\b")
    out = _sub(aws_key, "[REDACTED:AWS_ACCESS_KEY]", out, "aws_access_key", findings)

    # OpenAI / generic API key prefixes
    api_prefix = re.compile(
        r"\b(sk-[A-Za-z0-9]{20,}|"
        r"sk-proj-[A-Za-z0-9_-]{20,}|"
        r"ghp_[A-Za-z0-9]{36,}|"
        r"gho_[A-Za-z0-9]{36,}|"
        r"xox[baprs]-[A-Za-z0-9-]{10,}|"
        r"AIza[0-9A-Za-z_-]{35})\b"
    )
    out = _sub(api_prefix, "[REDACTED:API_KEY]", out, "api_key", findings)

    # Bearer / Basic auth headers
    bearer = re.compile(r"\b(Bearer\s+)[A-Za-z0-9._~+/=-]{20,}", re.IGNORECASE)
    out = _sub(bearer, r"\1[REDACTED:TOKEN]", out, "bearer_token", findings)

    basic = re.compile(r"\b(Basic\s+)[A-Za-z0-9+/=]{20,}", re.IGNORECASE)
    out = _sub(basic, r"\1[REDACTED:TOKEN]", out, "basic_auth", findings)

    # Key=value / key: value secret assignments (password, token, secret, api_key, etc.)
    secret_kv = re.compile(
        r"(?i)\b("
        r"password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|"
        r"client[_-]?secret|auth[_-]?token|refresh[_-]?token|private[_-]?key|"
        r"session[_-]?id|credential|apikey"
        r")(\s*[:=]\s*)"
        r"(?:(['\"])(.*?)(\3)|([^\s'\",;#]+))"
    )

    def _mask_kv(m: re.Match) -> str:
        findings.append("secret_assignment")
        quote = m.group(3) or ""
        return f"{m.group(1)}{m.group(2)}{quote}[REDACTED:SECRET]{quote}"

    out = secret_kv.sub(_mask_kv, out)

    # URL credentials user:pass@host
    url_creds = re.compile(
        r"([a-z][a-z0-9+.-]*://[^:/@\s]+:)([^@\s/]+)(@[^\s/]+)",
        re.IGNORECASE,
    )

    def _mask_url(m: re.Match) -> str:
        findings.append("url_credential")
        return f"{m.group(1)}[REDACTED:PASSWORD]{m.group(3)}"

    out = url_creds.sub(_mask_url, out)

    # Email
    email = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
    out = _sub(email, "[REDACTED:EMAIL]", out, "email", findings)

    # US SSN
    ssn = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
    out = _sub(ssn, "[REDACTED:SSN]", out, "ssn", findings)

    # Credit card (simple 13-19 digit groups with optional separators)
    cc = re.compile(r"\b(?:\d[ -]*?){13,19}\b")
    def _mask_cc(m: re.Match) -> str:
        digits = re.sub(r"\D", "", m.group(0))
        if 13 <= len(digits) <= 19:
            findings.append("credit_card")
            return "[REDACTED:CARD]"
        return m.group(0)

    out = cc.sub(_mask_cc, out)

    # Phone (US-centric loose pattern)
    phone = re.compile(r"\b(?:\+?1[-.\s]?)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}\b")
    out = _sub(phone, "[REDACTED:PHONE]", out, "phone", findings)

    return PIIMaskResult(text=out, masked=bool(findings), findings=findings)
