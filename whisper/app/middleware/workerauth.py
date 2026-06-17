import secrets

from fastapi import Header, HTTPException

from lib.core.config import ENVIRONMENT, VM_WORKER_AUTH_TOKEN


async def verify_worker_token(authorization: str | None = Header(default=None)) -> None:
    """Validate the shared worker token on the VM's /worker routes.

    Remote workers authenticate with a single static bearer token
    (VM_WORKER_AUTH_TOKEN) instead of per-user API keys. Attach this as a
    router dependency so it guards only the worker routes.
    """


    if not VM_WORKER_AUTH_TOKEN:
        raise HTTPException(
            status_code=500, detail="Worker auth token is not configured"
        )

    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    token = authorization.replace("Bearer ", "").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Invalid Authorization header")

    if not secrets.compare_digest(token, VM_WORKER_AUTH_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid worker token")
    
    print("Worker authenticated successfully")
