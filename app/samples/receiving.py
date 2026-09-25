from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.core.security import Principal
from app.samples.repository import SampleRepository
from app.services.audit import AuditService

ACCEPTED_OUTCOMES = ("accepted", "resolved_accepted")
REJECTED_OUTCOMES = ("rejected", "resolved_rejected")
HIGH_SEVERITIES = ("high", "critical")

REASON_LABELS = {
    "damaged_packaging": "包装破损",
    "label_conflict": "标签冲突",
    "wrong_item": "物品不符",
    "contamination": "污染",
    "temperature_breach": "温度失控",
    "paperwork_mismatch": "单据不符",
    "shortage": "少件",
    "unexpected_item": "清单外物品",
    "other": "其他",
}


def ensure_not_held(connection: sqlite3.Connection, sample_id: int) -> None:
    """借用/消耗前的接收隔离守卫：存在生效隔离 hold 一律阻止。"""
    row = connection.execute(
        "SELECT reason_code FROM receiving_holds WHERE sample_id=? AND active=1 LIMIT 1",
        (sample_id,),
    ).fetchone()
    if row is not None:
        raise ConflictError(
            "样品处于接收隔离状态，禁止借用或消耗",
            context={"reason_code": row["reason_code"]},
        )


def _row(row: sqlite3.Row | None, message: str) -> dict[str, Any]:
    if row is None:
        raise NotFoundError(message)
    return dict(row)


class ReceivingService:
    """接收批次复核状态机：open → reconciling → closed。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.samples = SampleRepository(connection)
        self.audit = AuditService(connection, self.clock)

    # ------------------------------------------------------------------ 开启

    def start(self, principal: Principal, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        existing_batch = self.connection.execute(
            "SELECT id FROM receipt_batches WHERE batch_code=?", (data["batch_code"],)
        ).fetchone()
        if existing_batch:
            raise ConflictError("批次编码已经存在")
        existing_session = self.connection.execute(
            "SELECT id FROM receiving_sessions WHERE session_code=? OR batch_code=?",
            (data.get("session_code") or "", data["batch_code"]),
        ).fetchone()
        if data.get("session_code") and existing_session:
            raise ConflictError("接收会话编码已经存在")
        now = to_storage(self.clock.now())
        qr_payload = f"sample-batch:{data['batch_code']}:{data['project_code']}"
        cursor = self.connection.execute(
            """INSERT INTO receipt_batches(batch_code,project_code,received_by,received_at,
                  expected_count,accepted_count,rejected_count,status,qr_payload,created_at,updated_at)
               VALUES(?,?,?,?,?,0,0,'open',?,?,?)""",
            (
                data["batch_code"], data["project_code"], principal.user_id, now,
                data["expected_count"], qr_payload, now, now,
            ),
        )
        batch_id = cursor.lastrowid
        session_code = data.get("session_code") or f"RCV-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO receiving_sessions(session_code,batch_id,batch_code,project_code,
                  state,current_version,created_by,created_at,updated_at)
               VALUES(?,?,?,?,'open',1,?,?,?)""",
            (session_code, batch_id, data["batch_code"], data["project_code"], principal.user_id, now, now),
        )
        session_id = cursor.lastrowid
        manifest = [
            {
                "line_no": item["line_no"],
                "sample_code": item["sample_code"],
                "sample_type": item["sample_type"],
                "quantity": item["quantity"],
                "unit": item["unit"],
            }
            for item in data.get("manifest_lines", [])
        ]
        for item in manifest:
            self.connection.execute(
                """INSERT INTO receiving_expected_lines(session_id,version_added,line_no,
                       sample_code,sample_type,quantity,unit)
                   VALUES(?,1,?,?,?,?,?)""",
                (
                    session_id, item["line_no"], item["sample_code"], item["sample_type"],
                    item["quantity"], item["unit"],
                ),
            )
        self.connection.execute(
            """INSERT INTO receiving_manifest_versions(session_id,version,expected_count,
                   manifest_json,revision_reason,created_by,created_at)
               VALUES(?,1,?,?,'初始箱单',?,?)""",
            (
                session_id, data["expected_count"],
                json.dumps(manifest, ensure_ascii=False), principal.user_id, now,
            ),
        )
        self.audit.record(
            principal, "receiving.start", "receiving_session", str(session_id),
            after={"batch_code": data["batch_code"], "expected_count": data["expected_count"]},
        )
        return self.detail(principal, session_id)

    # ------------------------------------------------------------- 清单版本

    def revise_manifest(self, principal: Principal, session_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        session = self._get_session(session_id)
        if session["state"] != "open":
            raise ConflictError("只有开放中的批次可以修正箱单")
        now = to_storage(self.clock.now())
        new_lines = data["manifest_lines"]
        active = self._active_lines(session_id)
        active_by_code = {line["sample_code"]: line for line in active}
        new_codes = {item["sample_code"] for item in new_lines}
        for code, line in active_by_code.items():
            if code not in new_codes and line["status"] != "expected":
                raise ConflictError(
                    "不能从箱单中移除已经接收或拒收的明细",
                    context={"sample_code": code, "status": line["status"]},
                )
        new_version = int(session["current_version"]) + 1
        for code, line in active_by_code.items():
            if code not in new_codes:
                self.connection.execute(
                    "UPDATE receiving_expected_lines SET version_removed=? WHERE id=?",
                    (new_version, line["id"]),
                )
        for item in new_lines:
            existed = active_by_code.get(item["sample_code"])
            if existed is None:
                self.connection.execute(
                    """INSERT INTO receiving_expected_lines(session_id,version_added,line_no,
                           sample_code,sample_type,quantity,unit)
                       VALUES(?,?,?,?,?,?,?)""",
                    (
                        session_id, new_version, item["line_no"], item["sample_code"],
                        item["sample_type"], item["quantity"], item["unit"],
                    ),
                )
            elif existed["status"] == "expected":
                self.connection.execute(
                    """UPDATE receiving_expected_lines SET line_no=?,sample_type=?,quantity=?,unit=?
                       WHERE id=?""",
                    (
                        item["line_no"], item["sample_type"], item["quantity"],
                        item["unit"], existed["id"],
                    ),
                )
        self.connection.execute(
            "UPDATE receipt_batches SET expected_count=?,updated_at=? WHERE id=?",
            (data["expected_count"], now, session["batch_id"]),
        )
        self.connection.execute(
            "UPDATE receiving_sessions SET current_version=?,updated_at=? WHERE id=?",
            (new_version, now, session_id),
        )
        manifest = [
            {
                "line_no": item["line_no"],
                "sample_code": item["sample_code"],
                "sample_type": item["sample_type"],
                "quantity": item["quantity"],
                "unit": item["unit"],
            }
            for item in new_lines
        ]
        self.connection.execute(
            """INSERT INTO receiving_manifest_versions(session_id,version,expected_count,
                   manifest_json,revision_reason,created_by,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (
                session_id, new_version, data["expected_count"],
                json.dumps(manifest, ensure_ascii=False), data["revision_reason"],
                principal.user_id, now,
            ),
        )
        self.audit.record(
            principal, "receiving.manifest.revise", "receiving_session", str(session_id),
            before={"version": session["current_version"]},
            after={"version": new_version, "expected_count": data["expected_count"]},
            metadata={"reason": data["revision_reason"]},
        )
        return self.detail(principal, session_id)

    # ---------------------------------------------------------------- 扫描

    def scan(self, principal: Principal, session_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        session = self._get_session(session_id)
        if session["state"] == "closed":
            raise ConflictError("批次已关闭，不能继续扫描")
        replay = self.connection.execute(
            "SELECT * FROM receiving_scan_groups WHERE session_id=? AND scan_group=?",
            (session_id, data["scan_group"]),
        ).fetchone()
        if replay is not None:
            return {
                **self._group_summary(session_id, data["scan_group"]),
                "replayed": True,
            }
        now = to_storage(self.clock.now())
        accepted = 0
        duplicates = 0
        pending = 0
        scanned: list[dict[str, Any]] = []
        for item in data["items"]:
            result = self._scan_one(principal, session, item, data["scan_group"], data["device_label"], now)
            scanned.append(result)
            if result["outcome"] == "duplicate":
                duplicates += 1
            elif result["outcome"] == "pending":
                pending += 1
            elif result["outcome"] in ACCEPTED_OUTCOMES:
                accepted += 1
        self.connection.execute(
            """INSERT INTO receiving_scan_groups(session_id,scan_group,item_count,accepted_count,
                   pending_count,duplicate_count,created_by,created_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            (
                session_id, data["scan_group"], len(data["items"]), accepted, pending,
                duplicates, principal.user_id, now,
            ),
        )
        self.connection.execute(
            "UPDATE receiving_sessions SET updated_at=? WHERE id=?", (now, session_id)
        )
        self.audit.record(
            principal, "receiving.scan", "receiving_session", str(session_id),
            metadata={
                "scan_group": data["scan_group"],
                "items": len(data["items"]),
                "accepted": accepted,
                "pending": pending,
                "duplicates": duplicates,
            },
        )
        summary = self._group_summary(session_id, data["scan_group"])
        summary["scans"] = scanned
        summary["replayed"] = False
        return summary

    def _scan_one(
        self,
        principal: Principal,
        session: dict[str, Any],
        item: dict[str, Any],
        scan_group: str,
        device_label: str,
        now: str,
    ) -> dict[str, Any]:
        session_id = session["id"]
        code = item["scan_code"]
        seen = self.connection.execute(
            "SELECT id,outcome FROM receiving_scans WHERE session_id=? AND scan_code=? AND outcome<>'duplicate'",
            (session_id, code),
        ).fetchone()
        if seen is not None:
            cursor = self.connection.execute(
                """INSERT INTO receiving_scans(session_id,scan_code,outcome,scanned_by,scan_group,
                       device_label,note,created_at)
                   VALUES(?,?,'duplicate',?,?,?,?,?)""",
                (session_id, code, principal.user_id, scan_group, device_label, item.get("note") or "", now),
            )
            return dict(self.connection.execute("SELECT * FROM receiving_scans WHERE id=?", (cursor.lastrowid,)).fetchone())

        line_row = self.connection.execute(
            """SELECT * FROM receiving_expected_lines
               WHERE session_id=? AND sample_code=? AND version_removed IS NULL""",
            (session_id, code),
        ).fetchone()

        if line_row is None:
            scan_id = self._insert_scan(
                session_id, code, None, "pending", None, item, principal, scan_group, device_label, now
            )
            pending_id = self._raise_pending(
                principal, session, None, code, "unexpected_item", "high",
                f"清单外扫描条码：{code}", scan_id, now,
            )
            return self._scan_view(scan_id, {"pending_id": pending_id})

        line = dict(line_row)
        if line["status"] != "expected":
            cursor = self.connection.execute(
                """INSERT INTO receiving_scans(session_id,scan_code,outcome,matched_line_id,
                       scanned_by,scan_group,device_label,note,created_at)
                   VALUES(?,?,'duplicate',?,?,?,?,?,?)""",
                (session_id, code, line["id"], principal.user_id, scan_group, device_label, item.get("note") or "", now),
            )
            return dict(self.connection.execute("SELECT * FROM receiving_scans WHERE id=?", (cursor.lastrowid,)).fetchone())

        mismatch_notes: list[str] = []
        if item.get("sample_type") and item["sample_type"] != line["sample_type"]:
            mismatch_notes.append(f"样品种类不符：箱单 {line['sample_type']} / 实物 {item['sample_type']}")
        if item.get("unit") and item["unit"] != line["unit"]:
            mismatch_notes.append(f"单位不符：箱单 {line['unit']} / 实物 {item['unit']}")
        if item.get("quantity") is not None and abs(float(item["quantity"]) - float(line["quantity"])) > 1e-6:
            mismatch_notes.append(f"数量不符：箱单 {line['quantity']} / 实物 {item['quantity']}")
        if mismatch_notes:
            scan_id = self._insert_scan(
                session_id, code, line["id"], "pending", None, item, principal, scan_group, device_label, now
            )
            identity_mismatch = (item.get("sample_type") and item["sample_type"] != line["sample_type"]) or (
                item.get("unit") and item["unit"] != line["unit"]
            )
            reason = "label_conflict" if identity_mismatch else "paperwork_mismatch"
            pending_id = self._raise_pending(
                principal, session, line["id"], code, reason, "high",
                "；".join(mismatch_notes), scan_id, now,
            )
            return self._scan_view(scan_id, {"pending_id": pending_id})

        if self.samples.by_code(code) is not None:
            scan_id = self._insert_scan(
                session_id, code, line["id"], "pending", None, item, principal, scan_group, device_label, now
            )
            pending_id = self._raise_pending(
                principal, session, line["id"], code, "label_conflict", "critical",
                "样品编码在其他批次已经存在，疑似标签重复", scan_id, now,
            )
            return self._scan_view(scan_id, {"pending_id": pending_id})

        claimed = self.connection.execute(
            "UPDATE receiving_expected_lines SET status='received' WHERE id=? AND status='expected'",
            (line["id"],),
        )
        if claimed.rowcount != 1:
            raise ConflictError("明细已被其他接收员处理", context={"sample_code": code})
        sample = self.samples.create(
            {
                "sample_code": code,
                "batch_id": session["batch_id"],
                "sample_type": line["sample_type"],
                "quantity": item.get("quantity") or line["quantity"],
                "unit": item.get("unit") or line["unit"],
                "lifecycle_state": "received",
                "location_id": item.get("location_id"),
                "custody_user_id": principal.user_id,
                "lineage_depth": 0,
            },
            now,
        )
        scan_id = self._insert_scan(
            session_id, code, line["id"], "accepted", sample["id"], item, principal, scan_group, device_label, now
        )
        self.connection.execute(
            "UPDATE receiving_expected_lines SET resolved_scan_id=? WHERE id=?",
            (scan_id, line["id"]),
        )
        self.samples.append_event(
            sample["id"], "received.scan", principal.user_id, now,
            to_state="received", details={"session_id": session_id, "scan_group": scan_group},
        )
        return self._scan_view(scan_id)

    def _insert_scan(
        self, session_id, code, line_id, outcome, sample_id, item, principal, scan_group, device_label, now
    ) -> int:
        cursor = self.connection.execute(
            """INSERT INTO receiving_scans(session_id,scan_code,matched_line_id,outcome,sample_id,
                   sample_type,quantity,unit,location_id,scanned_by,scan_group,device_label,note,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                session_id, code, line_id, outcome, sample_id,
                item.get("sample_type"), item.get("quantity"), item.get("unit"), item.get("location_id"),
                principal.user_id, scan_group, device_label, item.get("note") or "", now,
            ),
        )
        return cursor.lastrowid

    def _scan_view(self, scan_id: int, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        result = dict(
            self.connection.execute("SELECT * FROM receiving_scans WHERE id=?", (scan_id,)).fetchone()
        )
        if extra:
            result.update(extra)
        return result

    # ----------------------------------------------------------- 拒收/待查

    def reject(self, principal: Principal, session_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        session = self._get_session(session_id)
        if session["state"] == "closed":
            raise ConflictError("批次已关闭")
        now = to_storage(self.clock.now())
        line_row = self.connection.execute(
            """SELECT * FROM receiving_expected_lines
               WHERE session_id=? AND sample_code=? AND version_removed IS NULL""",
            (session_id, data["sample_code"]),
        ).fetchone()
        line = dict(line_row) if line_row else None
        if line and line["status"] == "rejected":
            raise ConflictError("该明细已经拒收", context={"sample_code": data["sample_code"]})
        already = self.connection.execute(
            "SELECT id FROM receipt_rejections WHERE session_id=? AND sample_code=?",
            (session_id, data["sample_code"]),
        ).fetchone()
        if already:
            raise ConflictError("该样品已经存在拒收记录", context={"sample_code": data["sample_code"]})
        accepted_scan = None
        if line and line["status"] == "received":
            accepted_scan = self.connection.execute(
                """SELECT * FROM receiving_scans
                   WHERE matched_line_id=? AND outcome IN ('accepted','resolved_accepted')
                   ORDER BY id LIMIT 1""",
                (line["id"],),
            ).fetchone()
        sample = self.samples.by_code(data["sample_code"])
        sample_id = sample["id"] if sample and sample["batch_id"] == session["batch_id"] else None
        anomaly_id = None
        if data["severity"] in HIGH_SEVERITIES:
            anomaly_id = self._open_anomaly(
                principal, session, sample_id, data["reason_code"], data["severity"],
                f"拒收：{REASON_LABELS[data['reason_code']]}。{data['note']}".strip("。"),
                now,
            )
        cursor = self.connection.execute(
            """INSERT INTO receipt_rejections(session_id,line_id,sample_code,reason_code,severity,
                   note,anomaly_id,rejected_by,scan_id,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                session_id, line["id"] if line else None, data["sample_code"],
                data["reason_code"], data["severity"], data["note"], anomaly_id,
                principal.user_id, accepted_scan["id"] if accepted_scan else None, now,
            ),
        )
        rejection_id = cursor.lastrowid
        if line:
            self.connection.execute(
                "UPDATE receiving_expected_lines SET status='rejected' WHERE id=? AND status<>'rejected'",
                (line["id"],),
            )
        if accepted_scan is not None:
            self.connection.execute(
                "UPDATE receiving_scans SET outcome='resolved_rejected' WHERE id=?",
                (accepted_scan["id"],),
            )
        if sample_id is not None:
            self._quarantine(
                principal, session_id, sample_id, anomaly_id, data["reason_code"],
                data["severity"], data["note"] or "拒收后隔离", now,
            )
        self.audit.record(
            principal, "receiving.reject", "receipt_rejection", str(rejection_id),
            after={"sample_code": data["sample_code"], "reason_code": data["reason_code"], "anomaly_id": anomaly_id},
        )
        return self._rejection_view(rejection_id)

    def mark_pending(self, principal: Principal, session_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        session = self._get_session(session_id)
        if session["state"] == "closed":
            raise ConflictError("批次已关闭")
        now = to_storage(self.clock.now())
        line_row = self.connection.execute(
            """SELECT * FROM receiving_expected_lines
               WHERE session_id=? AND sample_code=? AND version_removed IS NULL""",
            (session_id, data["sample_code"]),
        ).fetchone()
        open_row = self.connection.execute(
            "SELECT id FROM receiving_pending_items WHERE session_id=? AND sample_code=? AND status='open'",
            (session_id, data["sample_code"]),
        ).fetchone()
        if open_row:
            raise ConflictError("该样品已有未决待查记录", context={"pending_id": open_row["id"]})
        pending_id = self._raise_pending(
            principal, session,
            dict(line_row)["id"] if line_row else None,
            data["sample_code"], data["reason_code"], data["severity"], data["note"], None, now,
        )
        self.audit.record(
            principal, "receiving.pending", "receiving_pending_item", str(pending_id),
            after={"sample_code": data["sample_code"], "reason_code": data["reason_code"]},
        )
        return self._pending_view(pending_id)

    def _raise_pending(
        self, principal, session, line_id, code, reason_code, severity, note, scan_id, now
    ) -> int:
        anomaly_id = None
        sample_row = self.samples.by_code(code)
        sample_id = (
            sample_row["id"]
            if sample_row and sample_row["batch_id"] == session["batch_id"]
            else None
        )
        if severity in HIGH_SEVERITIES:
            anomaly_id = self._open_anomaly(
                principal, session, sample_id, reason_code, severity,
                f"待查：{REASON_LABELS[reason_code]}。{note}".strip("。"), now,
            )
        cursor = self.connection.execute(
            """INSERT INTO receiving_pending_items(session_id,line_id,sample_code,reason_code,
                   severity,note,status,raised_by,anomaly_id,scan_id,created_at)
               VALUES(?,?,?,?,?,?,'open',?,?,?,?)""",
            (
                session["id"], line_id, code, reason_code, severity, note,
                principal.user_id, anomaly_id, scan_id, now,
            ),
        )
        pending_id = cursor.lastrowid
        if sample_id is not None and severity in HIGH_SEVERITIES:
            self._quarantine(
                principal, session["id"], sample_id, anomaly_id, reason_code, severity,
                note or REASON_LABELS[reason_code], now,
            )
        return pending_id

    def resolve_pending(self, principal: Principal, session_id: int, pending_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        session = self._get_session(session_id)
        if session["state"] == "closed":
            raise ConflictError("批次已关闭")
        pending = _row(
            self.connection.execute("SELECT * FROM receiving_pending_items WHERE id=? AND session_id=?", (pending_id, session_id)).fetchone(),
            "待查记录不存在",
        )
        if pending["status"] != "open":
            raise ConflictError("待查记录已经处理")
        now = to_storage(self.clock.now())

        if data["resolution"] == "received":
            sample = self._accept_pending(principal, session, pending, data, now)
            self.connection.execute(
                """UPDATE receiving_pending_items SET status='received',resolved_by=?,resolved_at=?
                   WHERE id=?""",
                (principal.user_id, now, pending_id),
            )
            if pending["anomaly_id"]:
                self.connection.execute(
                    """UPDATE anomaly_cases SET state='contained',sample_id=COALESCE(sample_id,?),
                       resolution=?,version=version+1,updated_at=? WHERE id=?""",
                    (sample["id"], f"待查件复核后收讫：{data['note']}".strip("："), now, pending["anomaly_id"]),
                )
            result = self._pending_view(pending_id)
            result["sample"] = sample
            self.audit.record(
                principal, "receiving.pending.resolve", "receiving_pending_item", str(pending_id),
                after={"resolution": "received", "sample_id": sample["id"]},
            )
            return result

        # resolution == rejected
        line_id = pending["line_id"]
        anomaly_id = pending["anomaly_id"]
        if anomaly_id is None and pending["severity"] in HIGH_SEVERITIES:
            anomaly_id = self._open_anomaly(
                principal, session, None, data["reject_reason_code"], pending["severity"],
                f"待查件判拒收：{data['note']}", now,
            )
        cursor = self.connection.execute(
            """INSERT INTO receipt_rejections(session_id,line_id,sample_code,reason_code,severity,
                   note,anomaly_id,rejected_by,scan_id,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (
                session_id, line_id, pending["sample_code"], data["reject_reason_code"],
                pending["severity"], data["note"], anomaly_id, principal.user_id,
                pending["scan_id"], now,
            ),
        )
        if line_id:
            self.connection.execute(
                "UPDATE receiving_expected_lines SET status='rejected' WHERE id=? AND status='expected'",
                (line_id,),
            )
        if pending["scan_id"]:
            self.connection.execute(
                "UPDATE receiving_scans SET outcome='resolved_rejected' WHERE id=?",
                (pending["scan_id"],),
            )
        self.connection.execute(
            """UPDATE receiving_pending_items SET status='rejected',resolved_by=?,resolved_at=?,
                   anomaly_id=COALESCE(anomaly_id,?) WHERE id=?""",
            (principal.user_id, now, anomaly_id, pending_id),
        )
        self.audit.record(
            principal, "receiving.pending.resolve", "receiving_pending_item", str(pending_id),
            after={"resolution": "rejected", "rejection_id": cursor.lastrowid},
        )
        return self._pending_view(pending_id)

    def _accept_pending(self, principal, session, pending, data, now) -> dict[str, Any]:
        session_id = session["id"]
        code = pending["sample_code"]
        line_id = pending["line_id"]
        line = None
        if line_id:
            line = dict(
                self.connection.execute(
                    "SELECT * FROM receiving_expected_lines WHERE id=?", (line_id,)
                ).fetchone()
            )
        sample_type = data.get("sample_type") or (line["sample_type"] if line else None)
        quantity = data.get("quantity") if data.get("quantity") is not None else (line["quantity"] if line else None)
        unit = data.get("unit") or (line["unit"] if line else None)
        if not sample_type or quantity is None or not unit:
            raise ValidationError("收讫必须补齐样品类型、数量和单位")
        existing = self.samples.by_code(code)
        if existing and existing["batch_id"] == session["batch_id"]:
            sample = existing
        else:
            if existing:
                raise ConflictError("样品编码已被其他批次占用", context={"sample_code": code})
            if line and line["status"] == "expected":
                claimed = self.connection.execute(
                    "UPDATE receiving_expected_lines SET status='received' WHERE id=? AND status='expected'",
                    (line_id,),
                )
                if claimed.rowcount != 1:
                    raise ConflictError("明细已被其他接收员处理", context={"sample_code": code})
            sample = self.samples.create(
                {
                    "sample_code": code,
                    "batch_id": session["batch_id"],
                    "sample_type": sample_type,
                    "quantity": quantity,
                    "unit": unit,
                    "lifecycle_state": "received",
                    "location_id": data.get("location_id"),
                    "custody_user_id": principal.user_id,
                    "lineage_depth": 0,
                },
                now,
            )
        if pending["scan_id"]:
            self.connection.execute(
                """UPDATE receiving_scans SET outcome='resolved_accepted',sample_id=?,
                       matched_line_id=COALESCE(matched_line_id,?),sample_type=?,quantity=?,unit=?,
                       location_id=? WHERE id=?""",
                (sample["id"], line_id, sample_type, quantity, unit, data.get("location_id"), pending["scan_id"]),
            )
        if line_id:
            self.connection.execute(
                "UPDATE receiving_expected_lines SET resolved_scan_id=COALESCE(resolved_scan_id,?) WHERE id=?",
                (pending["scan_id"], line_id),
            )
        self.samples.append_event(
            sample["id"], "received.pending_resolved", principal.user_id, now,
            to_state="received", details={"pending_id": pending["id"]},
        )
        active_hold = self.connection.execute(
            "SELECT id FROM receiving_holds WHERE sample_id=? AND active=1", (sample["id"],)
        ).fetchone()
        if active_hold:
            self.connection.execute(
                """UPDATE receiving_holds SET active=0,released_by=?,released_at=?,
                       release_note=? WHERE id=?""",
                (principal.user_id, now, f"待查件复核后收讫（pending {pending['id']}）", active_hold["id"]),
            )
            self.connection.execute(
                """UPDATE samples SET lifecycle_state='received',version=version+1,updated_at=?
                   WHERE id=? AND lifecycle_state='quarantined'""",
                (now, sample["id"]),
            )
            self.samples.append_event(
                sample["id"], "hold.released", principal.user_id, now,
                from_state="quarantined", to_state="received",
                details={"pending_id": pending["id"]},
            )
        return sample

    # ------------------------------------------------------------- 差异解释

    def explain_diff(self, principal: Principal, session_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        session = self._get_session(session_id)
        if session["state"] == "closed":
            raise ConflictError("批次已关闭")
        diff_key = f"{data['diff_type']}:{data['sample_code']}"
        reconciliation = self._reconciliation(session_id)
        valid_keys = {item["key"] for item in reconciliation["differences"]}
        if diff_key not in valid_keys:
            raise ValidationError("解释必须针对当前存在的差异", context={"diff_key": diff_key})
        now = to_storage(self.clock.now())
        self.connection.execute(
            """INSERT INTO receiving_resolutions(session_id,diff_key,diff_type,explanation,created_by,created_at)
               VALUES(?,?,?,?,?,?)
               ON CONFLICT(session_id,diff_key) DO UPDATE SET explanation=excluded.explanation,
                   created_by=excluded.created_by,created_at=excluded.created_at""",
            (session_id, diff_key, data["diff_type"], data["explanation"], principal.user_id, now),
        )
        self.audit.record(
            principal, "receiving.diff.explain", "receiving_resolution", diff_key,
            after={"explanation": data["explanation"]},
        )
        return {"diff_key": diff_key, "explanation": data["explanation"]}

    # ------------------------------------------------------------- 状态流转

    def reconcile(self, principal: Principal, session_id: int) -> dict[str, Any]:
        principal.require("receiving.manage")
        before = self._get_session(session_id)
        if before["state"] != "open":
            raise ConflictError("只有开放中的批次可以生成差异对账")
        now = to_storage(self.clock.now())
        self.connection.execute(
            "UPDATE receiving_sessions SET state='reconciling',updated_at=? WHERE id=? AND state='open'",
            (now, session_id),
        )
        reconciliation = self._reconciliation(session_id)
        digest = hashlib.sha256(
            json.dumps(reconciliation["differences"], ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        self.audit.record(
            principal, "receiving.reconcile", "receiving_session", str(session_id),
            before={"state": "open"}, after={"state": "reconciling"},
            metadata={"difference_count": len(reconciliation["differences"]), "digest": digest},
        )
        reconciliation["session"] = self._get_session(session_id)
        reconciliation["digest"] = digest
        return reconciliation

    def reopen(self, principal: Principal, session_id: int) -> dict[str, Any]:
        principal.require("receiving.manage")
        before = self._get_session(session_id)
        if before["state"] != "reconciling":
            raise ConflictError("只有差异复核中的批次可以退回继续扫描")
        now = to_storage(self.clock.now())
        self.connection.execute(
            "UPDATE receiving_sessions SET state='open',updated_at=? WHERE id=? AND state='reconciling'",
            (now, session_id),
        )
        self.connection.execute("DELETE FROM receiving_resolutions WHERE session_id=?", (session_id,))
        self.audit.record(
            principal, "receiving.reopen", "receiving_session", str(session_id),
            before={"state": "reconciling"}, after={"state": "open"},
        )
        return self.detail(principal, session_id)

    def close(self, principal: Principal, session_id: int) -> dict[str, Any]:
        principal.require("receiving.manage")
        before = self._get_session(session_id)
        if before["state"] != "reconciling":
            raise ConflictError("必须先完成差异复核才能关闭批次")
        reconciliation = self._reconciliation(session_id)
        blockers = reconciliation["blockers"]
        if blockers:
            raise ConflictError("关闭前必须解释所有差异并清空待查项", context={"blockers": blockers})
        now = to_storage(self.clock.now())

        held_ids = {
            row[0]
            for row in self.connection.execute(
                "SELECT sample_id FROM receiving_holds WHERE session_id=? AND active=1", (session_id,)
            ).fetchall()
        }
        accepted_samples = [
            dict(row)
            for row in self.connection.execute(
                """SELECT DISTINCT s.* FROM samples s
                   JOIN receiving_scans sc ON sc.sample_id=s.id
                   WHERE sc.session_id=? AND sc.outcome IN ('accepted','resolved_accepted')""",
                (session_id,),
            ).fetchall()
        ]
        warehoused: list[int] = []
        for sample in accepted_samples:
            if sample["id"] in held_ids:
                continue
            self.connection.execute(
                """UPDATE samples SET lifecycle_state='available',version=version+1,updated_at=?
                   WHERE id=? AND lifecycle_state='received'""",
                (now, sample["id"]),
            )
            self.samples.append_event(
                sample["id"], "warehoused", principal.user_id, now,
                from_state="received", to_state="available",
                details={"session_id": session_id},
            )
            warehoused.append(sample["id"])

        rejected_count = reconciliation["counts"]["rejected_count"]
        accepted_count = len(accepted_samples)
        accepted_ids = {sample["id"] for sample in accepted_samples}
        blocking_holds = held_ids & accepted_ids
        warehouse_eligible = 1 if not blocking_holds else 0
        self.connection.execute(
            "UPDATE receipt_batches SET accepted_count=?,rejected_count=?,status=?,updated_at=? WHERE id=?",
            (
                accepted_count, rejected_count,
                "quarantined" if held_ids else "closed", now, before["batch_id"],
            ),
        )
        summary = {
            "expected_count": reconciliation["counts"]["expected_count"],
            "accepted_count": accepted_count,
            "rejected_count": rejected_count,
            "warehoused_sample_ids": warehoused,
            "quarantined_sample_ids": sorted(held_ids),
            "difference_count": len(reconciliation["differences"]),
            "warehouse_eligible": bool(warehouse_eligible),
        }
        self.connection.execute(
            """UPDATE receiving_sessions SET state='closed',closed_by=?,closed_at=?,
                   warehouse_eligible=?,close_summary_json=?,updated_at=? WHERE id=?""",
            (principal.user_id, now, warehouse_eligible, json.dumps(summary, ensure_ascii=False), now, session_id),
        )
        self.audit.record(
            principal, "receiving.close", "receipt_batch", str(before["batch_id"]),
            after=summary,
        )
        return self.detail(principal, session_id)

    # ------------------------------------------------------------- 隔离解除

    def release_hold(self, principal: Principal, session_id: int, hold_id: int, note: str) -> dict[str, Any]:
        principal.require("anomalies.manage")
        session = self._get_session(session_id)
        hold = _row(
            self.connection.execute("SELECT * FROM receiving_holds WHERE id=? AND session_id=?", (hold_id, session_id)).fetchone(),
            "隔离记录不存在",
        )
        if not hold["active"]:
            raise ConflictError("隔离已经解除")
        now = to_storage(self.clock.now())
        self.connection.execute(
            """UPDATE receiving_holds SET active=0,released_by=?,released_at=?,release_note=?
               WHERE id=?""",
            (principal.user_id, now, note, hold_id),
        )
        target_state = "available" if session["state"] == "closed" else "received"
        self.connection.execute(
            """UPDATE samples SET lifecycle_state=?,version=version+1,updated_at=?
               WHERE id=? AND lifecycle_state='quarantined'""",
            (target_state, now, hold["sample_id"]),
        )
        self.samples.append_event(
            hold["sample_id"], "hold.released", principal.user_id, now,
            from_state="quarantined", to_state=target_state, details={"note": note},
        )
        self.audit.record(
            principal, "receiving.hold.release", "receiving_hold", str(hold_id),
            after={"sample_id": hold["sample_id"], "target_state": target_state},
        )
        return dict(
            self.connection.execute("SELECT * FROM receiving_holds WHERE id=?", (hold_id,)).fetchone()
        )

    # ----------------------------------------------------------------- 对账

    def _reconciliation(self, session_id: int) -> dict[str, Any]:
        session = self._get_session(session_id)
        active_lines = self._active_lines(session_id)
        scans = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receiving_scans WHERE session_id=? AND outcome<>'duplicate' ORDER BY id",
                (session_id,),
            ).fetchall()
        ]
        rejections = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receipt_rejections WHERE session_id=? ORDER BY id", (session_id,)
            ).fetchall()
        ]
        pending_open = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receiving_pending_items WHERE session_id=? AND status='open' ORDER BY id",
                (session_id,),
            ).fetchall()
        ]
        resolutions = {
            row["diff_key"]: dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receiving_resolutions WHERE session_id=?", (session_id,)
            ).fetchall()
        }

        accepted_scans = [sc for sc in scans if sc["outcome"] in ACCEPTED_OUTCOMES]
        rejected_scan_ids = {sc["id"] for sc in scans if sc["outcome"] in REJECTED_OUTCOMES}
        received_line_ids = {sc["matched_line_id"] for sc in accepted_scans if sc["matched_line_id"]}
        rejected_line_ids = {rj["line_id"] for rj in rejections if rj["line_id"]}

        differences: list[dict[str, Any]] = []
        for line in active_lines:
            if line["id"] in received_line_ids:
                continue
            if line["id"] in rejected_line_ids:
                continue
            key = f"missing:{line['sample_code']}"
            differences.append(
                {
                    "key": key,
                    "diff_type": "missing",
                    "line_no": line["line_no"],
                    "sample_code": line["sample_code"],
                    "expected_quantity": line["quantity"],
                    "unit": line["unit"],
                    "explanation": resolutions.get(key, {}).get("explanation"),
                    "explained": key in resolutions,
                }
            )
        for sc in scans:
            if sc["matched_line_id"] is not None:
                continue
            if sc["outcome"] in ACCEPTED_OUTCOMES:
                key = f"unexpected:{sc['scan_code']}"
                differences.append(
                    {
                        "key": key,
                        "diff_type": "unexpected",
                        "sample_code": sc["scan_code"],
                        "actual_quantity": sc["quantity"],
                        "unit": sc["unit"],
                        "explanation": resolutions.get(key, {}).get("explanation"),
                        "explained": key in resolutions,
                    }
                )

        blockers: list[dict[str, Any]] = [
            {"type": "pending_open", "pending_id": item["id"], "sample_code": item["sample_code"],
             "reason_code": item["reason_code"], "severity": item["severity"]}
            for item in pending_open
        ]
        blockers.extend(
            {"type": "unexplained_difference", **{k: diff[k] for k in ("key", "diff_type", "sample_code")}}
            for diff in differences
            if not diff["explained"]
        )

        duplicate_count = self.connection.execute(
            "SELECT COUNT(*) FROM receiving_scans WHERE session_id=? AND outcome='duplicate'",
            (session_id,),
        ).fetchone()[0]
        active_holds = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receiving_holds WHERE session_id=? AND active=1 ORDER BY id", (session_id,)
            ).fetchall()
        ]
        accepted_sample_ids = {sc["sample_id"] for sc in accepted_scans if sc["sample_id"]}
        blocking_holds = [h for h in active_holds if h["sample_id"] in accepted_sample_ids]
        high_open = self.connection.execute(
            """SELECT COUNT(*) FROM anomaly_cases WHERE batch_id=?
               AND severity IN ('high','critical') AND state NOT IN ('resolved','dismissed')""",
            (session["batch_id"],),
        ).fetchone()[0]
        counts = {
            "expected_count": len(active_lines),
            "accepted_count": len(accepted_scans),
            "rejected_count": len(rejections),
            "pending_open_count": len(pending_open),
            "duplicate_scan_count": duplicate_count,
            "active_hold_count": len(active_holds),
            "accounted_count": len(received_line_ids)
            + len([line for line in active_lines if line["id"] in rejected_line_ids]),
        }
        counts["shortage_count"] = counts["expected_count"] - counts["accounted_count"]
        warehouse_ready = not blockers and len(blocking_holds) == 0
        return {
            "counts": counts,
            "differences": differences,
            "blockers": blockers,
            "can_close": not blockers,
            "warehouse_eligible": warehouse_ready,
            "high_risk_open": bool(high_open),
        }

    def detail(self, principal: Principal, session_id: int) -> dict[str, Any]:
        self._require_read(principal)
        session = self._get_session(session_id)
        reconciliation = self._reconciliation(session_id)
        session["reconciliation"] = reconciliation
        session["manifest_versions"] = [
            {
                **dict(row),
                "manifest": json.loads(row["manifest_json"]),
            }
            for row in self.connection.execute(
                """SELECT id,version,expected_count,revision_reason,created_by,created_at,manifest_json
                   FROM receiving_manifest_versions WHERE session_id=? ORDER BY version""",
                (session_id,),
            ).fetchall()
        ]
        session["expected_lines"] = self._active_lines(session_id)
        session["scans"] = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receiving_scans WHERE session_id=? ORDER BY id", (session_id,)
            ).fetchall()
        ]
        session["scan_groups"] = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receiving_scan_groups WHERE session_id=? ORDER BY id", (session_id,)
            ).fetchall()
        ]
        session["rejections"] = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receipt_rejections WHERE session_id=? ORDER BY id", (session_id,)
            ).fetchall()
        ]
        session["pending_items"] = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receiving_pending_items WHERE session_id=? ORDER BY id", (session_id,)
            ).fetchall()
        ]
        session["holds"] = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receiving_holds WHERE session_id=? ORDER BY id", (session_id,)
            ).fetchall()
        ]
        session["explanations"] = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receiving_resolutions WHERE session_id=? ORDER BY id", (session_id,)
            ).fetchall()
        ]
        if session.get("close_summary_json"):
            session["close_summary"] = json.loads(session["close_summary_json"])
        session["anomalies"] = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM anomaly_cases WHERE batch_id=? ORDER BY id", (session["batch_id"],)
            ).fetchall()
        ]
        return session

    def list_open(self, principal: Principal) -> list[dict[str, Any]]:
        self._require_read(principal)
        rows = self.connection.execute(
            "SELECT * FROM receiving_sessions WHERE state<>'closed' ORDER BY id"
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["reconciliation"] = self._reconciliation(item["id"])
            result.append(item)
        return result

    # ------------------------------------------------------------- 内部工具

    def _require_read(self, principal: Principal) -> None:
        if not (principal.can("receiving.manage") or principal.can("samples.read")):
            from app.core.errors import PermissionDeniedError

            raise PermissionDeniedError("缺少权限：samples.read")

    def _get_session(self, session_id: int) -> dict[str, Any]:
        return _row(
            self.connection.execute("SELECT * FROM receiving_sessions WHERE id=?", (session_id,)).fetchone(),
            "接收会话不存在",
        )

    def _active_lines(self, session_id: int) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                """SELECT * FROM receiving_expected_lines
                   WHERE session_id=? AND version_removed IS NULL ORDER BY line_no""",
                (session_id,),
            ).fetchall()
        ]

    def _open_anomaly(self, principal, session, sample_id, reason_code, severity, description, now) -> int:
        cursor = self.connection.execute(
            """INSERT INTO anomaly_cases(case_code,sample_id,batch_id,anomaly_type,severity,state,
                   detected_by,description,created_at,updated_at)
               VALUES(?,?,?,?,?,'open',?,?,?,?)""",
            (
                f"ANM-{uuid.uuid4().hex[:12]}", sample_id, session["batch_id"],
                f"receiving:{reason_code}", severity, principal.user_id, description, now, now,
            ),
        )
        return cursor.lastrowid

    def _quarantine(self, principal, session_id, sample_id, anomaly_id, reason_code, severity, note, now) -> int:
        existing = self.connection.execute(
            "SELECT id FROM receiving_holds WHERE sample_id=? AND active=1", (sample_id,)
        ).fetchone()
        if existing:
            return existing["id"]
        cursor = self.connection.execute(
            """INSERT INTO receiving_holds(session_id,sample_id,anomaly_id,reason_code,severity,
                   note,active,raised_by,created_at)
               VALUES(?,?,?,?,?,?,'1',?,?)""",
            (session_id, sample_id, anomaly_id, reason_code, severity, note, principal.user_id, now),
        )
        self.connection.execute(
            """UPDATE samples SET lifecycle_state='quarantined',version=version+1,updated_at=?
               WHERE id=? AND lifecycle_state<>'quarantined'""",
            (now, sample_id),
        )
        self.samples.append_event(
            sample_id, "quarantined.hold", principal.user_id, now,
            to_state="quarantined",
            details={"hold_id": cursor.lastrowid, "anomaly_id": anomaly_id, "reason_code": reason_code},
        )
        return cursor.lastrowid

    def _group_summary(self, session_id: int, scan_group: str) -> dict[str, Any]:
        group = _row(
            self.connection.execute(
                "SELECT * FROM receiving_scan_groups WHERE session_id=? AND scan_group=?",
                (session_id, scan_group),
            ).fetchone(),
            "扫描分组不存在",
        )
        scans = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receiving_scans WHERE session_id=? AND scan_group=? ORDER BY id",
                (session_id, scan_group),
            ).fetchall()
        ]
        return {**group, "scans": scans}

    def _rejection_view(self, rejection_id: int) -> dict[str, Any]:
        return dict(
            self.connection.execute("SELECT * FROM receipt_rejections WHERE id=?", (rejection_id,)).fetchone()
        )

    def _pending_view(self, pending_id: int) -> dict[str, Any]:
        return dict(
            self.connection.execute("SELECT * FROM receiving_pending_items WHERE id=?", (pending_id,)).fetchone()
        )
