"""Request identity for Graph Service.

Graph Service sits behind the API Gateway, which validates the user's JWT and
forwards the verified subject as `X-User-Id` (it overwrites any client-supplied
value). Public `/graph/**` routes use that identity for ownership checks.

`/internal/**` is service-to-service only and requires a shared service
credential (`Authorization: Bearer <INTERNAL_SERVICE_TOKENS entry>`); a user
JWT or a missing/unknown credential is rejected. gRPC callers (llm-service)
pass `user_id` in the request, which the handler checks against the node owner.
"""

import hmac

from fastapi import Header, HTTPException, status

from app.core.config import settings


async def get_caller_user_id(x_user_id: str | None = Header(default=None)) -> str:
    if not x_user_id or not x_user_id.strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing authenticated user identity")
    return x_user_id.strip()


async def require_internal_service(authorization: str | None = Header(default=None)) -> None:
    scheme, _, presented = (authorization or "").partition(" ")
    if scheme.lower() == "bearer" and presented:
        for token in settings.internal_service_tokens:
            if hmac.compare_digest(presented.encode(), token.encode()):
                return
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing service credential")
