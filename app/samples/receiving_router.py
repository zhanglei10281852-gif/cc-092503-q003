from __future__ import annotations

from fastapi import APIRouter, Depends, Query, status

from app.api.dependencies import current_principal
from app.core.security import Principal
from app.database import get_connection, transaction
from app.samples.receiving import ReceivingService
from app.samples.receiving_schemas import (
    DiscrepancyExplain,
    ManifestDeclare,
    ManifestRevise,
    RejectionCreate,
    ScanCreate,
    ScanSessionOpen,
)

router = APIRouter(prefix="/api/receiving", tags=["接收复核"])


@router.post("/batches/{batch_id}/manifest", status_code=status.HTTP_201_CREATED)
def declare_manifest(batch_id: int, payload: ManifestDeclare, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).declare_manifest(principal, batch_id, payload.model_dump())


@router.get("/batches/{batch_id}/manifest")
def get_manifest(batch_id: int, principal: Principal = Depends(current_principal)):
    return ReceivingService(get_connection()).manifest(principal, batch_id)


@router.get("/batches/{batch_id}/manifest/revisions")
def get_manifest_revisions(batch_id: int, principal: Principal = Depends(current_principal)):
    return ReceivingService(get_connection()).manifest_revisions(principal, batch_id)


@router.post("/batches/{batch_id}/manifest/{line_key}/revisions", status_code=status.HTTP_201_CREATED)
def revise_manifest_line(
    batch_id: int, line_key: str, payload: ManifestRevise, principal: Principal = Depends(current_principal)
):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).revise_manifest_line(principal, batch_id, line_key, payload.model_dump())


@router.post("/batches/{batch_id}/scan-sessions", status_code=status.HTTP_201_CREATED)
def open_scan_session(batch_id: int, payload: ScanSessionOpen, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).open_session(principal, batch_id, payload.model_dump())


@router.get("/batches/{batch_id}/review")
def review_batch(batch_id: int, principal: Principal = Depends(current_principal)):
    return ReceivingService(get_connection()).review(principal, batch_id)


@router.post("/batches/{batch_id}/reconcile")
def reconcile_batch(batch_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).reconcile(principal, batch_id)


@router.post("/batches/{batch_id}/close")
def close_batch(batch_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).close(principal, batch_id)


@router.post("/batches/{batch_id}/rejections", status_code=status.HTTP_201_CREATED)
def reject_line(batch_id: int, payload: RejectionCreate, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).reject(principal, batch_id, payload.model_dump())


@router.get("/batches/{batch_id}/discrepancies")
def list_discrepancies(
    batch_id: int, state: str | None = Query(default=None), principal: Principal = Depends(current_principal)
):
    return ReceivingService(get_connection()).list_discrepancies(principal, batch_id, state)


@router.get("/scan-sessions/{session_id}")
def get_scan_session(session_id: int, principal: Principal = Depends(current_principal)):
    return ReceivingService(get_connection()).session_detail(principal, session_id)


@router.post("/scan-sessions/{session_id}/pause")
def pause_scan_session(session_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).pause_session(principal, session_id)


@router.post("/scan-sessions/{session_id}/resume")
def resume_scan_session(session_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).resume_session(principal, session_id)


@router.post("/scan-sessions/{session_id}/complete")
def complete_scan_session(session_id: int, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).complete_session(principal, session_id)


@router.post("/scan-sessions/{session_id}/scans", status_code=status.HTTP_201_CREATED)
def record_scan(session_id: int, payload: ScanCreate, principal: Principal = Depends(current_principal)):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).scan(principal, session_id, payload.model_dump())


@router.post("/discrepancies/{discrepancy_id}/explain")
def explain_discrepancy(
    discrepancy_id: int, payload: DiscrepancyExplain, principal: Principal = Depends(current_principal)
):
    with transaction(immediate=True) as connection:
        return ReceivingService(connection).explain_discrepancy(principal, discrepancy_id, payload.model_dump())
