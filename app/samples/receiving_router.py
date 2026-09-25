from __future__ import annotations

from fastapi import APIRouter, Depends, status

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection, transaction
from app.samples.receiving import ReceivingService
from app.samples.receiving_schemas import (
    DiffExplanation,
    HoldRelease,
    ManifestRevision,
    PendingItem,
    PendingResolve,
    ReceivingStart,
    RejectItem,
    ScanBatch,
)

router = APIRouter(prefix="/api/receiving", tags=["接收复核"])


@router.post("/sessions", status_code=status.HTTP_201_CREATED)
def start_receiving(payload: ReceivingStart, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).start(principal, payload.model_dump())


@router.get("/sessions/open")
def list_open_sessions(principal: Principal = Depends(current_principal)):
    return ReceivingService(get_connection()).list_open(principal)


@router.get("/sessions/{session_id}")
def receiving_detail(session_id: int, principal: Principal = Depends(current_principal)):
    return ReceivingService(get_connection()).detail(principal, session_id)


@router.put("/sessions/{session_id}/manifest")
def revise_manifest(session_id: int, payload: ManifestRevision, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).revise_manifest(principal, session_id, payload.model_dump())


@router.post("/sessions/{session_id}/scans")
def submit_scans(session_id: int, payload: ScanBatch, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).scan(principal, session_id, payload.model_dump())


@router.post("/sessions/{session_id}/rejections", status_code=status.HTTP_201_CREATED)
def reject_item(session_id: int, payload: RejectItem, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).reject(principal, session_id, payload.model_dump())


@router.post("/sessions/{session_id}/pending", status_code=status.HTTP_201_CREATED)
def raise_pending(session_id: int, payload: PendingItem, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).mark_pending(principal, session_id, payload.model_dump())


@router.post("/sessions/{session_id}/pending/{pending_id}/resolutions")
def resolve_pending(
    session_id: int,
    pending_id: int,
    payload: PendingResolve,
    principal: Principal = Depends(current_principal),
):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).resolve_pending(principal, session_id, pending_id, payload.model_dump())


@router.post("/sessions/{session_id}/differences/explanations")
def explain_difference(session_id: int, payload: DiffExplanation, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).explain_diff(principal, session_id, payload.model_dump())


@router.post("/sessions/{session_id}/reconcile")
def reconcile_receiving(session_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).reconcile(principal, session_id)


@router.post("/sessions/{session_id}/reopen")
def reopen_receiving(session_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).reopen(principal, session_id)


@router.post("/sessions/{session_id}/close")
def close_receiving(session_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).close(principal, session_id)


@router.post("/sessions/{session_id}/holds/{hold_id}/release")
def release_hold(session_id: int, hold_id: int, payload: HoldRelease, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).release_hold(principal, session_id, hold_id, payload.note)
