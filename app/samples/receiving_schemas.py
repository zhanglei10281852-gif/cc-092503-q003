from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_validator

SEVERITY = Literal["low", "medium", "high", "critical"]

REJECTION_REASON_LITERAL = Literal[
    "damaged_packaging",
    "label_conflict",
    "wrong_item",
    "contamination",
    "temperature_breach",
    "paperwork_mismatch",
    "other",
]

PENDING_REASON_LITERAL = Literal[
    "damaged_packaging",
    "label_conflict",
    "wrong_item",
    "contamination",
    "temperature_breach",
    "paperwork_mismatch",
    "shortage",
    "unexpected_item",
    "other",
]


class ManifestLine(BaseModel):
    line_no: int = Field(gt=0)
    sample_code: str = Field(min_length=1, max_length=100)
    sample_type: str = Field(min_length=1, max_length=100)
    quantity: float = Field(gt=0)
    unit: str = Field(min_length=1, max_length=20)


class ReceivingStart(BaseModel):
    batch_code: str = Field(min_length=3, max_length=64)
    project_code: str = Field(min_length=2, max_length=64)
    expected_count: int = Field(gt=0, le=100_000)
    session_code: str | None = Field(default=None, min_length=3, max_length=64)
    manifest_lines: list[ManifestLine] = Field(default_factory=list, max_length=10_000)

    @model_validator(mode="after")
    def _validate_manifest(self):
        if self.manifest_lines:
            if len(self.manifest_lines) != self.expected_count:
                raise ValueError("明细行数必须等于预期数量 expected_count")
            line_nos = [line.line_no for line in self.manifest_lines]
            codes = [line.sample_code for line in self.manifest_lines]
            if len(set(line_nos)) != len(line_nos):
                raise ValueError("箱单明细行号不能重复")
            if len(set(codes)) != len(codes):
                raise ValueError("箱单样品编码不能重复")
        return self


class ManifestRevision(BaseModel):
    expected_count: int = Field(gt=0, le=100_000)
    manifest_lines: list[ManifestLine] = Field(min_length=1, max_length=10_000)
    revision_reason: str = Field(min_length=2, max_length=500)

    @model_validator(mode="after")
    def _validate_manifest(self):
        if len(self.manifest_lines) != self.expected_count:
            raise ValueError("明细行数必须等于预期数量 expected_count")
        line_nos = [line.line_no for line in self.manifest_lines]
        codes = [line.sample_code for line in self.manifest_lines]
        if len(set(line_nos)) != len(line_nos):
            raise ValueError("箱单明细行号不能重复")
        if len(set(codes)) != len(codes):
            raise ValueError("箱单样品编码不能重复")
        return self


class ScanItem(BaseModel):
    scan_code: str = Field(min_length=1, max_length=100)
    sample_type: str | None = Field(default=None, min_length=1, max_length=100)
    quantity: float | None = Field(default=None, gt=0)
    unit: str | None = Field(default=None, min_length=1, max_length=20)
    location_id: int | None = Field(default=None, gt=0)
    note: str | None = Field(default=None, max_length=500)


class ScanBatch(BaseModel):
    scan_group: str = Field(min_length=1, max_length=100)
    device_label: str = Field(default="", max_length=100)
    items: list[ScanItem] = Field(min_length=1, max_length=5_000)

    @model_validator(mode="after")
    def _unique_within_batch(self):
        codes = [item.scan_code for item in self.items]
        if len(set(codes)) != len(codes):
            raise ValueError("同一次扫描中不能重复提交相同条码")
        return self


class RejectItem(BaseModel):
    sample_code: str = Field(min_length=1, max_length=100)
    reason_code: REJECTION_REASON_LITERAL
    severity: SEVERITY = "medium"
    note: str = Field(default="", max_length=500)


class PendingItem(BaseModel):
    sample_code: str = Field(min_length=1, max_length=100)
    reason_code: PENDING_REASON_LITERAL
    severity: SEVERITY = "medium"
    note: str = Field(default="", max_length=500)


class PendingResolve(BaseModel):
    resolution: Literal["received", "rejected"]
    sample_type: str | None = Field(default=None, min_length=1, max_length=100)
    quantity: float | None = Field(default=None, gt=0)
    unit: str | None = Field(default=None, min_length=1, max_length=20)
    location_id: int | None = Field(default=None, gt=0)
    reject_reason_code: REJECTION_REASON_LITERAL | None = None
    note: str = Field(default="", max_length=500)

    @model_validator(mode="after")
    def _validate_resolution(self):
        if self.resolution == "received":
            missing = [
                name
                for name in ("sample_type", "quantity", "unit")
                if getattr(self, name) in (None, "")
            ]
            if missing:
                raise ValueError("待查件判为收讫必须补齐样品类型、数量和单位")
        if self.resolution == "rejected" and not self.reject_reason_code:
            raise ValueError("待查件判为拒收必须填写拒收原因")
        return self


class DiffExplanation(BaseModel):
    diff_type: Literal["missing", "unexpected"]
    sample_code: str = Field(min_length=1, max_length=100)
    explanation: str = Field(min_length=2, max_length=500)


class HoldRelease(BaseModel):
    note: str = Field(min_length=2, max_length=500)
