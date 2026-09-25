from __future__ import annotations

from pydantic import BaseModel, Field


class ManifestLineInput(BaseModel):
    line_key: str | None = Field(default=None, min_length=1, max_length=64)
    item_code: str = Field(min_length=1, max_length=100)
    sample_type: str = Field(min_length=1, max_length=100)
    expected_quantity: float = Field(gt=0)
    unit: str = Field(min_length=1, max_length=20)
    note: str = Field(default="", max_length=500)


class ManifestDeclare(BaseModel):
    lines: list[ManifestLineInput] = Field(min_length=1, max_length=500)


class ManifestRevise(BaseModel):
    item_code: str | None = Field(default=None, min_length=1, max_length=100)
    sample_type: str | None = Field(default=None, min_length=1, max_length=100)
    expected_quantity: float | None = Field(default=None, gt=0)
    unit: str | None = Field(default=None, min_length=1, max_length=20)
    note: str | None = Field(default=None, max_length=500)
    change_reason: str = Field(min_length=4, max_length=500)


class ScanSessionOpen(BaseModel):
    note: str = Field(default="", max_length=500)


class ScanCreate(BaseModel):
    item_code: str = Field(min_length=1, max_length=100)
    quantity: float | None = Field(default=None, gt=0)
    idempotency_key: str = Field(min_length=4, max_length=100)
    damaged: bool = False
    note: str = Field(default="", max_length=500)


class RejectionCreate(BaseModel):
    line_key: str = Field(min_length=1, max_length=64)
    reason: str = Field(min_length=4, max_length=500)
    quantity: float = Field(default=1, gt=0)


class DiscrepancyExplain(BaseModel):
    explanation: str = Field(min_length=4, max_length=1000)
    anomaly_id: int | None = Field(default=None, gt=0)
