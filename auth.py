"""
Authentication module for BigQuery Data Agent A2A Proxy.
Decodes Entra ID OAuth tokens to inspect user identity claims.
"""
import time
import logging
import jwt
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("oauth.auth")


def decode_token_claims(token: str) -> dict:
    """Decode a JWT token WITHOUT verification to inspect its claims."""
    try:
        # Log JWT header metadata (alg/kid only — no sensitive data)
        try:
            header = jwt.get_unverified_header(token)
            logger.debug(
                "token_header: alg=%s kid=%s typ=%s",
                header.get("alg"), header.get("kid"), header.get("typ"),
            )
        except Exception as he:
            logger.warning("token_header_parse_failed: %s", he)

        claims = jwt.decode(token, options={"verify_signature": False})

        now = int(time.time())
        exp = claims.get("exp")
        iat = claims.get("iat")
        ttl = (exp - now) if exp else None
        age = (now - iat) if iat else None

        # Which email-bearing claims are present (bool only — no PII values logged)
        email_claims_present = [
            k for k in ("upn", "preferred_username", "unique_name", "email")
            if claims.get(k)
        ]

        logger.info(
            "token_decoded: iss=%s aud=%s ttl_sec=%s age_sec=%s "
            "email_claims=%s scp=%s appid=%s tid=%s",
            claims.get("iss"),
            claims.get("aud"),
            ttl,
            age,
            email_claims_present,
            claims.get("scp"),
            claims.get("appid") or claims.get("azp"),
            claims.get("tid"),
        )

        if ttl is not None:
            if ttl < 0:
                logger.error("token_expired: ttl_sec=%d (expired %ds ago)", ttl, -ttl)
            elif ttl < 300:
                logger.warning("token_expiring_soon: ttl_sec=%d", ttl)
        else:
            logger.warning("token_no_expiry: no 'exp' claim found")

        return claims

    except jwt.exceptions.DecodeError as e:
        logger.error("token_decode_error: %s | token_prefix=%.20s...", e, token)
        return {}
    except Exception as e:
        logger.exception("token_unexpected_error: %s", e)
        return {}
