from __future__ import annotations

import sqlite3
import uuid
from typing import Any

from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.core.security import Principal
from app.samples.quarantine import apply_quarantine
from app.samples.repository import BatchRepository, SampleRepository
from app.services.audit import AuditService

QUANTITY_TOLERANCE = 1e-9


class ReceivingService:
    """接收复核：预期清单、分次扫描、拒收、差异解释与批次关闭的状态机。"""

    def __init__(self, connection: sqlite3.Connection, clock: Clock | None = None):
        self.connection = connection
        self.clock = clock or SystemClock()
        self.batches = BatchRepository(connection)
        self.samples = SampleRepository(connection)
        self.audit = AuditService(connection, self.clock)

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    def _batch(self, batch_id: int) -> dict[str, Any]:
        return self.batches.get(batch_id)

    def _require_mutable(self, batch: dict[str, Any], now: str) -> dict[str, Any]:
        """只有 open/reconciled 允许接收操作；reconciled 有新动作时自动退回 open。"""
        if batch["status"] == "reconciled":
            self.connection.execute(
                "UPDATE receipt_batches SET status='open',updated_at=? WHERE id=? AND status='reconciled'",
                (now, batch["id"]),
            )
            batch["status"] = "open"
        elif batch["status"] != "open":
            raise ConflictError(
                "批次当前状态禁止接收操作",
                context={"batch_id": batch["id"], "status": batch["status"]},
            )
        return batch

    def _refresh_counts(self, batch_id: int, now: str) -> None:
        self.connection.execute(
            """UPDATE receipt_batches SET
                   accepted_count=(SELECT COUNT(*) FROM samples WHERE batch_id=?),
                   rejected_count=(SELECT COALESCE(SUM(quantity),0) FROM receipt_rejections WHERE batch_id=?),
                   updated_at=?
               WHERE id=?""",
            (batch_id, batch_id, now, batch_id),
        )

    def _current_lines(self, batch_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM receipt_manifest_items WHERE batch_id=? AND is_current=1 ORDER BY line_key",
            (batch_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def _line_by_key(self, batch_id: int, line_key: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM receipt_manifest_items WHERE batch_id=? AND line_key=? AND is_current=1",
            (batch_id, line_key),
        ).fetchone()
        if row is None:
            raise NotFoundError("清单行不存在")
        return dict(row)

    def _claim_by_code(self, batch_id: int, item_code: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM receipt_item_claims WHERE batch_id=? AND item_code=?",
            (batch_id, item_code),
        ).fetchone()
        return dict(row) if row else None

    def _session(self, session_id: int) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM receipt_scan_sessions WHERE id=?", (session_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("扫描会话不存在")
        return dict(row)

    def _discrepancy(self, discrepancy_id: int) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM receipt_discrepancies WHERE id=?", (discrepancy_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError("差异记录不存在")
        return dict(row)

    def _add_discrepancy(
        self,
        *,
        batch_id: int,
        kind: str,
        item_code: str,
        now: str,
        line_key: str | None = None,
        manifest_item_id: int | None = None,
        scan_event_id: int | None = None,
        expected: float | None = None,
        observed: float | None = None,
        anomaly_id: int | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        cursor = self.connection.execute(
            """INSERT INTO receipt_discrepancies(
                   batch_id,kind,line_key,item_code,manifest_item_id,scan_event_id,
                   expected_quantity,observed_quantity,anomaly_id,note,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                batch_id, kind, line_key, item_code, manifest_item_id, scan_event_id,
                expected, observed, anomaly_id, note, now, now,
            ),
        )
        return dict(
            self.connection.execute(
                "SELECT * FROM receipt_discrepancies WHERE id=?", (cursor.lastrowid,)
            ).fetchone()
        )

    def _raise_anomaly(
        self,
        *,
        batch_id: int,
        sample_id: int | None,
        anomaly_type: str,
        description: str,
        detected_by: int,
        now: str,
    ) -> dict[str, Any]:
        """接收复核内部直接登记高风险异常并触发自动隔离。"""
        case_code = f"ANM-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO anomaly_cases(case_code,sample_id,batch_id,anomaly_type,severity,state,detected_by,description,created_at,updated_at)
               VALUES(?,?,?,?,'high','open',?,?,?,?)""",
            (case_code, sample_id, batch_id, anomaly_type, detected_by, description, now, now),
        )
        anomaly = dict(
            self.connection.execute("SELECT * FROM anomaly_cases WHERE id=?", (cursor.lastrowid,)).fetchone()
        )
        apply_quarantine(self.connection, anomaly, detected_by, now)
        return anomaly

    def _auto_explain_pending(
        self,
        *,
        batch_id: int,
        kinds: tuple[str, ...],
        line_key: str | None,
        explanation: str,
        explained_by: int,
        now: str,
    ) -> None:
        placeholders = ",".join("?" for _ in kinds)
        self.connection.execute(
            f"""UPDATE receipt_discrepancies
                SET state='explained',explanation=?,explained_by=?,explained_at=?,updated_at=?
                WHERE batch_id=? AND state='pending' AND line_key IS ? AND kind IN ({placeholders})""",
            (explanation, explained_by, now, now, batch_id, line_key, *kinds),
        )

    def _generate_shortages(self, batch_id: int, now: str) -> list[dict[str, Any]]:
        """为既未扫码也未拒收的清单行生成少件差异（幂等）。"""
        generated = []
        for line in self._current_lines(batch_id):
            if self._claim_by_code(batch_id, line["item_code"]):
                continue
            rejected = self.connection.execute(
                "SELECT 1 FROM receipt_rejections WHERE batch_id=? AND line_key=?",
                (batch_id, line["line_key"]),
            ).fetchone()
            if rejected:
                continue
            existing = self.connection.execute(
                """SELECT 1 FROM receipt_discrepancies
                   WHERE batch_id=? AND line_key=? AND kind='shortage'""",
                (batch_id, line["line_key"]),
            ).fetchone()
            if existing:
                continue
            generated.append(
                self._add_discrepancy(
                    batch_id=batch_id,
                    kind="shortage",
                    item_code=line["item_code"],
                    now=now,
                    line_key=line["line_key"],
                    manifest_item_id=line["id"],
                    expected=line["expected_quantity"],
                    observed=0,
                    note="复核时清单行未收到实物",
                )
            )
        return generated

    def _pending_discrepancies(self, batch_id: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM receipt_discrepancies WHERE batch_id=? AND state='pending' ORDER BY id",
            (batch_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # 预期清单（含版本化修正）
    # ------------------------------------------------------------------
    def declare_manifest(self, principal: Principal, batch_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        now = to_storage(self.clock.now())
        batch = self._require_mutable(self._batch(batch_id), now)
        lines = data["lines"]
        keys = [line["line_key"] or line["item_code"] for line in lines]
        codes = [line["item_code"] for line in lines]
        if len(set(keys)) != len(keys) or len(set(codes)) != len(codes):
            raise ValidationError("清单行号或物品编码在请求内重复")
        existing = self._current_lines(batch_id)
        existing_keys = {line["line_key"] for line in existing}
        existing_codes = {line["item_code"] for line in existing}
        if set(keys) & existing_keys or set(codes) & existing_codes:
            raise ConflictError("清单行已存在，请使用清单修正接口")
        for code in codes:
            if self.samples.by_code(code):
                raise ConflictError("物品编码已被现有样品占用", context={"item_code": code})
            if self._claim_by_code(batch_id, code):
                raise ConflictError("物品编码已被扫描记录占用", context={"item_code": code})
        created = []
        for line in lines:
            cursor = self.connection.execute(
                """INSERT INTO receipt_manifest_items(
                       batch_id,line_key,item_code,sample_type,expected_quantity,unit,note,
                       revision,is_current,change_reason,changed_by,created_at
                   ) VALUES(?,?,?,?,?,?,?,1,1,'初始登记',?,?)""",
                (
                    batch_id, line["line_key"] or line["item_code"], line["item_code"],
                    line["sample_type"], line["expected_quantity"], line["unit"],
                    line.get("note", ""), principal.user_id, now,
                ),
            )
            created.append(
                dict(self.connection.execute("SELECT * FROM receipt_manifest_items WHERE id=?", (cursor.lastrowid,)).fetchone())
            )
        self.audit.record(
            principal,
            "receiving.manifest.declare",
            "receipt_batch",
            str(batch_id),
            metadata={"line_count": len(created)},
        )
        return {"batch": self._batch(batch_id), "lines": created}

    def revise_manifest_line(
        self, principal: Principal, batch_id: int, line_key: str, data: dict[str, Any]
    ) -> dict[str, Any]:
        principal.require("receiving.manage")
        now = to_storage(self.clock.now())
        batch = self._require_mutable(self._batch(batch_id), now)
        current = self._line_by_key(batch_id, line_key)
        updated = {
            "item_code": data.get("item_code") or current["item_code"],
            "sample_type": data.get("sample_type") or current["sample_type"],
            "expected_quantity": data.get("expected_quantity") or current["expected_quantity"],
            "unit": data.get("unit") or current["unit"],
            "note": current["note"] if data.get("note") is None else data["note"],
        }
        if all(updated[field] == current[field] for field in updated):
            raise ValidationError("修正内容与当前版本一致，无需变更")
        claim = self._claim_by_code(batch_id, current["item_code"])
        if claim and updated["item_code"] != current["item_code"]:
            raise ConflictError("该清单行已扫码接收，禁止变更物品编码")
        if updated["item_code"] != current["item_code"]:
            if self.samples.by_code(updated["item_code"]):
                raise ConflictError("物品编码已被现有样品占用", context={"item_code": updated["item_code"]})
            if self._claim_by_code(batch_id, updated["item_code"]):
                raise ConflictError("物品编码已被扫描记录占用", context={"item_code": updated["item_code"]})
            collision = self.connection.execute(
                """SELECT 1 FROM receipt_manifest_items
                   WHERE batch_id=? AND item_code=? AND is_current=1 AND line_key<>?""",
                (batch_id, updated["item_code"], line_key),
            ).fetchone()
            if collision:
                raise ConflictError("物品编码已被本批次其他清单行占用", context={"item_code": updated["item_code"]})
        revision = current["revision"] + 1
        self.connection.execute(
            "UPDATE receipt_manifest_items SET is_current=0 WHERE id=?", (current["id"],)
        )
        cursor = self.connection.execute(
            """INSERT INTO receipt_manifest_items(
                   batch_id,line_key,item_code,sample_type,expected_quantity,unit,note,
                   revision,is_current,change_reason,changed_by,created_at
               ) VALUES(?,?,?,?,?,?,?,?,1,?,?,?)""",
            (
                batch_id, line_key, updated["item_code"], updated["sample_type"],
                updated["expected_quantity"], updated["unit"], updated["note"],
                revision, data["change_reason"], principal.user_id, now,
            ),
        )
        new_line = dict(
            self.connection.execute("SELECT * FROM receipt_manifest_items WHERE id=?", (cursor.lastrowid,)).fetchone()
        )
        # 清单修正后与实收一致的数量差异自动核销
        if updated["expected_quantity"] != current["expected_quantity"]:
            mismatches = self.connection.execute(
                """SELECT id,observed_quantity FROM receipt_discrepancies
                   WHERE batch_id=? AND line_key=? AND kind='quantity_mismatch' AND state='pending'""",
                (batch_id, line_key),
            ).fetchall()
            for mismatch in mismatches:
                if abs(float(mismatch["observed_quantity"]) - updated["expected_quantity"]) <= QUANTITY_TOLERANCE:
                    self.connection.execute(
                        """UPDATE receipt_discrepancies
                           SET state='explained',explanation=?,explained_by=?,explained_at=?,updated_at=? WHERE id=?""",
                        (
                            f"清单第 {revision} 版修正后与实收数量一致",
                            principal.user_id, now, now, mismatch["id"],
                        ),
                    )
        self.audit.record(
            principal,
            "receiving.manifest.revise",
            "receipt_batch",
            str(batch_id),
            before=current,
            after=new_line,
            metadata={"line_key": line_key, "revision": revision, "change_reason": data["change_reason"]},
        )
        return new_line

    def manifest(self, principal: Principal, batch_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        self._batch(batch_id)
        return {"batch_id": batch_id, "lines": self._current_lines(batch_id)}

    def manifest_revisions(self, principal: Principal, batch_id: int) -> list[dict[str, Any]]:
        principal.require("samples.read")
        self._batch(batch_id)
        rows = self.connection.execute(
            "SELECT * FROM receipt_manifest_items WHERE batch_id=? ORDER BY line_key,revision",
            (batch_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # 扫描会话（分次扫描、断点继续）
    # ------------------------------------------------------------------
    def open_session(self, principal: Principal, batch_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        now = to_storage(self.clock.now())
        batch = self._require_mutable(self._batch(batch_id), now)
        active = self.connection.execute(
            """SELECT id FROM receipt_scan_sessions
               WHERE batch_id=? AND receiver_user_id=? AND state='active'""",
            (batch_id, principal.user_id),
        ).fetchone()
        if active:
            raise ConflictError("当前接收员在该批次已有进行中的扫描会话")
        session_code = f"SCN-{uuid.uuid4().hex[:12]}"
        cursor = self.connection.execute(
            """INSERT INTO receipt_scan_sessions(session_code,batch_id,receiver_user_id,state,note,started_at,created_at,updated_at)
               VALUES(?,?,?,'active',?,?,?,?)""",
            (session_code, batch_id, principal.user_id, data.get("note", ""), now, now, now),
        )
        session = self._session(cursor.lastrowid)
        self.audit.record(principal, "receiving.session.open", "receipt_scan_session", str(session["id"]), after=session)
        return session

    def _transition_session(self, principal: Principal, session_id: int, expected: tuple[str, ...], target: str) -> dict[str, Any]:
        principal.require("receiving.manage")
        now = to_storage(self.clock.now())
        session = self._session(session_id)
        placeholders = ",".join("?" for _ in expected)
        paused_at = now if target == "paused" else None
        completed_at = now if target == "completed" else None
        cursor = self.connection.execute(
            f"""UPDATE receipt_scan_sessions
                SET state=?,paused_at=COALESCE(?,paused_at),completed_at=COALESCE(?,completed_at),updated_at=?
                WHERE id=? AND state IN ({placeholders})""",
            (target, paused_at, completed_at, now, session_id, *expected),
        )
        if cursor.rowcount != 1:
            raise ConflictError(
                "扫描会话状态不允许该操作",
                context={"session_id": session_id, "state": session["state"], "target": target},
            )
        updated = self._session(session_id)
        action = {"paused": "pause", "active": "resume", "completed": "complete"}[target]
        self.audit.record(
            principal,
            f"receiving.session.{action}",
            "receipt_scan_session",
            str(session_id),
            before=session,
            after=updated,
        )
        return updated

    def pause_session(self, principal: Principal, session_id: int) -> dict[str, Any]:
        return self._transition_session(principal, session_id, ("active",), "paused")

    def resume_session(self, principal: Principal, session_id: int) -> dict[str, Any]:
        return self._transition_session(principal, session_id, ("paused",), "active")

    def complete_session(self, principal: Principal, session_id: int) -> dict[str, Any]:
        return self._transition_session(principal, session_id, ("active", "paused"), "completed")

    def session_detail(self, principal: Principal, session_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        session = self._session(session_id)
        events = self.connection.execute(
            "SELECT * FROM receipt_scan_events WHERE session_id=? ORDER BY id",
            (session_id,),
        ).fetchall()
        session["scan_events"] = [dict(row) for row in events]
        return session

    # ------------------------------------------------------------------
    # 实收扫描
    # ------------------------------------------------------------------
    def scan(self, principal: Principal, session_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        now = to_storage(self.clock.now())
        session = self._session(session_id)
        batch_id = session["batch_id"]
        replay = self.connection.execute(
            "SELECT * FROM receipt_scan_events WHERE batch_id=? AND idempotency_key=?",
            (batch_id, data["idempotency_key"]),
        ).fetchone()
        if replay:
            return {"event": dict(replay), "replayed": True}
        if session["state"] != "active":
            raise ConflictError("扫描会话不在进行中，请先恢复会话")
        batch = self._require_mutable(self._batch(batch_id), now)

        item_code = data["item_code"]
        claim = self._claim_by_code(batch_id, item_code)
        created_discrepancies: list[dict[str, Any]] = []
        sample: dict[str, Any] | None = None
        anomaly: dict[str, Any] | None = None

        if claim:
            outcome = "duplicate"
            quantity = data["quantity"] or self._claimed_quantity(claim)
        else:
            existing_sample = self.samples.by_code(item_code)
            line = self._line_by_code(batch_id, item_code)
            if existing_sample:
                outcome = "label_conflict"
                quantity = data["quantity"] or 1
            elif line is None:
                outcome = "unexpected"
                if data["quantity"] is None:
                    raise ValidationError("计划外物品必须填写实收数量")
                quantity = data["quantity"]
            else:
                outcome = "matched"
                quantity = data["quantity"] if data["quantity"] is not None else line["expected_quantity"]
                sample = self.samples.create(
                    {
                        "sample_code": item_code,
                        "batch_id": batch_id,
                        "sample_type": line["sample_type"],
                        "quantity": quantity,
                        "unit": line["unit"],
                        "lifecycle_state": "received",
                        "custody_user_id": principal.user_id,
                        "lineage_depth": 0,
                    },
                    now,
                )

        cursor = self.connection.execute(
            """INSERT INTO receipt_scan_events(
                   batch_id,session_id,idempotency_key,item_code,quantity,outcome,
                   manifest_item_id,sample_id,damaged,note,scanned_by,created_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                batch_id, session_id, data["idempotency_key"], item_code, quantity, outcome,
                (line["id"] if outcome == "matched" else None),
                (sample["id"] if sample else None),
                int(data.get("damaged", False)), data.get("note", ""), principal.user_id, now,
            ),
        )
        event = dict(
            self.connection.execute("SELECT * FROM receipt_scan_events WHERE id=?", (cursor.lastrowid,)).fetchone()
        )

        if outcome != "duplicate":
            self.connection.execute(
                """INSERT INTO receipt_item_claims(batch_id,item_code,scan_event_id,manifest_item_id,sample_id,claimed_by,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (
                    batch_id, item_code, event["id"],
                    (line["id"] if outcome == "matched" else None),
                    (sample["id"] if sample else None),
                    principal.user_id, now,
                ),
            )

        if outcome == "matched":
            self.samples.append_event(
                sample["id"], "received", principal.user_id, now,
                to_state="received",
                details={"batch_id": batch_id, "scan_session_id": session_id, "manifest_item_id": line["id"]},
            )
            if abs(quantity - line["expected_quantity"]) > QUANTITY_TOLERANCE:
                created_discrepancies.append(
                    self._add_discrepancy(
                        batch_id=batch_id, kind="quantity_mismatch", item_code=item_code, now=now,
                        line_key=line["line_key"], manifest_item_id=line["id"], scan_event_id=event["id"],
                        expected=line["expected_quantity"], observed=quantity,
                        note="实收数量与清单预期不一致",
                    )
                )
            if data.get("damaged"):
                anomaly = self._raise_anomaly(
                    batch_id=batch_id, sample_id=sample["id"],
                    anomaly_type="包装破损",
                    description=f"接收扫描发现 {item_code} 包装破损，已自动隔离",
                    detected_by=principal.user_id, now=now,
                )
                created_discrepancies.append(
                    self._add_discrepancy(
                        batch_id=batch_id, kind="damaged", item_code=item_code, now=now,
                        line_key=line["line_key"], manifest_item_id=line["id"], scan_event_id=event["id"],
                        expected=line["expected_quantity"], observed=quantity,
                        anomaly_id=anomaly["id"], note="包装破损",
                    )
                )
            self._auto_explain_pending(
                batch_id=batch_id, kinds=("shortage",), line_key=line["line_key"],
                explanation="补扫接收，少件差异自动核销", explained_by=principal.user_id, now=now,
            )
        elif outcome == "unexpected":
            created_discrepancies.append(
                self._add_discrepancy(
                    batch_id=batch_id, kind="unexpected", item_code=item_code, now=now,
                    scan_event_id=event["id"], observed=quantity, note="实收物品不在预期清单中",
                )
            )
        elif outcome == "label_conflict":
            anomaly = self._raise_anomaly(
                batch_id=batch_id, sample_id=existing_sample["id"],
                anomaly_type="标签冲突",
                description=f"扫描编码 {item_code} 与现有样品 {existing_sample['sample_code']} 冲突，实物未登记",
                detected_by=principal.user_id, now=now,
            )
            created_discrepancies.append(
                self._add_discrepancy(
                    batch_id=batch_id, kind="label_conflict", item_code=item_code, now=now,
                    scan_event_id=event["id"], observed=quantity,
                    anomaly_id=anomaly["id"], note="扫描编码与系统现有样品冲突",
                )
            )

        self._refresh_counts(batch_id, now)
        self.audit.record(
            principal,
            "receiving.scan",
            "receipt_batch",
            str(batch_id),
            metadata={"item_code": item_code, "outcome": outcome, "session_id": session_id},
        )
        result: dict[str, Any] = {"event": event, "replayed": False, "discrepancies": created_discrepancies}
        if sample:
            result["sample"] = sample
        if anomaly:
            result["anomaly"] = anomaly
        return result

    def _line_by_code(self, batch_id: int, item_code: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM receipt_manifest_items WHERE batch_id=? AND item_code=? AND is_current=1",
            (batch_id, item_code),
        ).fetchone()
        return dict(row) if row else None

    def _claimed_quantity(self, claim: dict[str, Any]) -> float:
        row = self.connection.execute(
            "SELECT quantity FROM receipt_scan_events WHERE id=?", (claim["scan_event_id"],)
        ).fetchone()
        return float(row[0]) if row else 1

    # ------------------------------------------------------------------
    # 拒收
    # ------------------------------------------------------------------
    def reject(self, principal: Principal, batch_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        now = to_storage(self.clock.now())
        batch = self._require_mutable(self._batch(batch_id), now)
        line = self._line_by_key(batch_id, data["line_key"])
        if self._claim_by_code(batch_id, line["item_code"]):
            raise ConflictError("该清单行已扫码接收，不能拒收")
        existing = self.connection.execute(
            "SELECT 1 FROM receipt_rejections WHERE batch_id=? AND line_key=?",
            (batch_id, data["line_key"]),
        ).fetchone()
        if existing:
            raise ConflictError("该清单行已登记拒收")
        cursor = self.connection.execute(
            """INSERT INTO receipt_rejections(batch_id,manifest_item_id,line_key,item_code,quantity,reason,rejected_by,created_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            (
                batch_id, line["id"], line["line_key"], line["item_code"],
                data["quantity"], data["reason"], principal.user_id, now,
            ),
        )
        rejection = dict(
            self.connection.execute("SELECT * FROM receipt_rejections WHERE id=?", (cursor.lastrowid,)).fetchone()
        )
        self._auto_explain_pending(
            batch_id=batch_id, kinds=("shortage",), line_key=line["line_key"],
            explanation=f"清单行已拒收：{data['reason']}", explained_by=principal.user_id, now=now,
        )
        self._refresh_counts(batch_id, now)
        self.audit.record(principal, "receiving.reject", "receipt_batch", str(batch_id), after=rejection)
        return rejection

    # ------------------------------------------------------------------
    # 差异复核与解释
    # ------------------------------------------------------------------
    def reconcile(self, principal: Principal, batch_id: int) -> dict[str, Any]:
        principal.require("receiving.manage")
        now = to_storage(self.clock.now())
        batch = self._batch(batch_id)
        if batch["status"] == "reconciled":
            return {
                "batch": batch,
                "generated_shortages": [],
                "pending_discrepancies": self._pending_discrepancies(batch_id),
            }
        if batch["status"] != "open":
            raise ConflictError(
                "批次当前状态禁止差异复核",
                context={"batch_id": batch_id, "status": batch["status"]},
            )
        generated = self._generate_shortages(batch_id, now)
        pending = self._pending_discrepancies(batch_id)
        if not pending:
            self.connection.execute(
                "UPDATE receipt_batches SET status='reconciled',updated_at=? WHERE id=? AND status='open'",
                (now, batch_id),
            )
        result = self._batch(batch_id)
        self.audit.record(
            principal,
            "receiving.reconcile",
            "receipt_batch",
            str(batch_id),
            metadata={"generated_shortages": len(generated), "pending": len(pending)},
        )
        return {"batch": result, "generated_shortages": generated, "pending_discrepancies": pending}

    def explain_discrepancy(self, principal: Principal, discrepancy_id: int, data: dict[str, Any]) -> dict[str, Any]:
        principal.require("receiving.manage")
        now = to_storage(self.clock.now())
        discrepancy = self._discrepancy(discrepancy_id)
        if discrepancy["state"] != "pending":
            raise ConflictError("差异已解释，不能重复处理")
        anomaly_id = data.get("anomaly_id")
        if anomaly_id is not None:
            anomaly = self.connection.execute(
                "SELECT * FROM anomaly_cases WHERE id=?", (anomaly_id,)
            ).fetchone()
            if anomaly is None:
                raise NotFoundError("关联异常不存在")
            linked = anomaly["batch_id"] == discrepancy["batch_id"]
            if not linked and anomaly["sample_id"]:
                linked = self.connection.execute(
                    "SELECT 1 FROM samples WHERE id=? AND batch_id=?",
                    (anomaly["sample_id"], discrepancy["batch_id"]),
                ).fetchone() is not None
            if not linked:
                raise ValidationError("关联异常必须属于同一接收批次")
        cursor = self.connection.execute(
            """UPDATE receipt_discrepancies
               SET state='explained',explanation=?,explained_by=?,explained_at=?,anomaly_id=COALESCE(?,anomaly_id),updated_at=?
               WHERE id=? AND state='pending'""",
            (data["explanation"], principal.user_id, now, anomaly_id, now, discrepancy_id),
        )
        if cursor.rowcount != 1:
            raise ConflictError("差异已解释，不能重复处理")
        updated = self._discrepancy(discrepancy_id)
        self.audit.record(
            principal,
            "receiving.discrepancy.explain",
            "receipt_discrepancy",
            str(discrepancy_id),
            before=discrepancy,
            after=updated,
        )
        return updated

    def list_discrepancies(self, principal: Principal, batch_id: int, state: str | None) -> list[dict[str, Any]]:
        principal.require("samples.read")
        self._batch(batch_id)
        if state:
            rows = self.connection.execute(
                "SELECT * FROM receipt_discrepancies WHERE batch_id=? AND state=? ORDER BY id",
                (batch_id, state),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM receipt_discrepancies WHERE batch_id=? ORDER BY id",
                (batch_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # 对账报告与关闭
    # ------------------------------------------------------------------
    def _evaluate_storable(self, batch: dict[str, Any]) -> tuple[bool, list[str]]:
        batch_id = batch["id"]
        reasons: list[str] = []
        lines = self._current_lines(batch_id)
        if not lines:
            reasons.append("尚未登记预期清单")
        outstanding = 0
        for line in lines:
            claimed = self._claim_by_code(batch_id, line["item_code"])
            rejected = self.connection.execute(
                "SELECT 1 FROM receipt_rejections WHERE batch_id=? AND line_key=?",
                (batch_id, line["line_key"]),
            ).fetchone()
            if claimed or rejected:
                continue
            shortage = self.connection.execute(
                """SELECT state FROM receipt_discrepancies
                   WHERE batch_id=? AND line_key=? AND kind='shortage'""",
                (batch_id, line["line_key"]),
            ).fetchone()
            # 待解释的少件由“差异尚未解释”覆盖；已解释的少件视为已结清
            if shortage is None:
                outstanding += 1
        if outstanding:
            reasons.append(f"{outstanding} 条清单行既未接收也未拒收")
        pending = len(self._pending_discrepancies(batch_id))
        if pending:
            reasons.append(f"{pending} 条差异尚未解释")
        active_sessions = self.connection.execute(
            "SELECT COUNT(*) FROM receipt_scan_sessions WHERE batch_id=? AND state='active'",
            (batch_id,),
        ).fetchone()[0]
        if active_sessions:
            reasons.append(f"{active_sessions} 个扫描会话仍在进行中")
        if batch["status"] == "quarantined":
            reasons.append("批次处于隔离状态，需先了结高风险异常")
        return (not reasons, reasons)

    def review(self, principal: Principal, batch_id: int) -> dict[str, Any]:
        principal.require("samples.read")
        batch = self._batch(batch_id)
        lines = self._current_lines(batch_id)
        rejection_rows = self.connection.execute(
            "SELECT * FROM receipt_rejections WHERE batch_id=?", (batch_id,)
        ).fetchall()
        rejections = {row["line_key"]: dict(row) for row in rejection_rows}
        line_reports = []
        expected_by_unit: dict[str, float] = {}
        for line in lines:
            claim = self._claim_by_code(batch_id, line["item_code"])
            rejection = rejections.get(line["line_key"])
            if claim and claim["sample_id"]:
                receipt_status = "matched"
            elif rejection:
                receipt_status = "rejected"
            elif claim:
                receipt_status = "held"
            else:
                receipt_status = "outstanding"
            expected_by_unit[line["unit"]] = expected_by_unit.get(line["unit"], 0.0) + float(line["expected_quantity"])
            line_reports.append(
                {
                    "line_key": line["line_key"],
                    "item_code": line["item_code"],
                    "revision": line["revision"],
                    "sample_type": line["sample_type"],
                    "expected_quantity": line["expected_quantity"],
                    "unit": line["unit"],
                    "receipt_status": receipt_status,
                    "sample_id": claim["sample_id"] if claim else None,
                    "rejection_id": rejection["id"] if rejection else None,
                }
            )
        samples = [
            dict(row)
            for row in self.connection.execute(
                """SELECT id,sample_code,sample_type,quantity,unit,lifecycle_state
                   FROM samples WHERE batch_id=? ORDER BY sample_code""",
                (batch_id,),
            ).fetchall()
        ]
        received_by_unit: dict[str, float] = {}
        for sample in samples:
            received_by_unit[sample["unit"]] = received_by_unit.get(sample["unit"], 0.0) + float(sample["quantity"])
        discrepancies = [
            dict(row)
            for row in self.connection.execute(
                """SELECT d.*,a.case_code AS anomaly_case_code
                   FROM receipt_discrepancies d LEFT JOIN anomaly_cases a ON a.id=d.anomaly_id
                   WHERE d.batch_id=? ORDER BY d.id""",
                (batch_id,),
            ).fetchall()
        ]
        anomalies = [
            dict(row)
            for row in self.connection.execute(
                """SELECT a.id,a.case_code,a.anomaly_type,a.severity,a.state,a.sample_id,a.batch_id,
                          s.sample_code,a.created_at
                   FROM anomaly_cases a LEFT JOIN samples s ON s.id=a.sample_id
                   WHERE a.batch_id=? OR a.sample_id IN (SELECT id FROM samples WHERE batch_id=?)
                   ORDER BY a.id""",
                (batch_id, batch_id),
            ).fetchall()
        ]
        sessions = [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM receipt_scan_sessions WHERE batch_id=? ORDER BY id", (batch_id,)
            ).fetchall()
        ]
        scan_stats = dict(
            self.connection.execute(
                """SELECT COUNT(*) AS scan_event_count,
                          COALESCE(SUM(CASE WHEN outcome='duplicate' THEN 1 ELSE 0 END),0) AS duplicate_count,
                          COALESCE(SUM(CASE WHEN outcome='unexpected' THEN 1 ELSE 0 END),0) AS unexpected_count,
                          COALESCE(SUM(CASE WHEN outcome='label_conflict' THEN 1 ELSE 0 END),0) AS label_conflict_count
                   FROM receipt_scan_events WHERE batch_id=?""",
                (batch_id,),
            ).fetchone()
        )
        storable, reasons = self._evaluate_storable(batch)
        conclusion = "已入库" if batch["status"] == "closed" else ("可入库" if storable else "不可入库")
        return {
            "batch": batch,
            "manifest_lines": line_reports,
            "totals": {
                "expected_count": batch["expected_count"],
                "manifest_line_count": len(lines),
                "accepted_count": batch["accepted_count"],
                "rejected_count": batch["rejected_count"],
                "count_delta": batch["accepted_count"] + batch["rejected_count"] - batch["expected_count"],
                "expected_quantity_by_unit": expected_by_unit,
                "received_quantity_by_unit": received_by_unit,
                **{key: int(value) for key, value in scan_stats.items()},
            },
            "discrepancies": discrepancies,
            "pending_discrepancy_count": sum(1 for item in discrepancies if item["state"] == "pending"),
            "anomalies": anomalies,
            "quarantined_sample_codes": [s["sample_code"] for s in samples if s["lifecycle_state"] == "quarantined"],
            "scan_sessions": sessions,
            "samples": samples,
            "storable": storable,
            "storable_reasons": reasons,
            "conclusion": conclusion,
        }

    def close(self, principal: Principal, batch_id: int) -> dict[str, Any]:
        principal.require("receiving.manage")
        now = to_storage(self.clock.now())
        batch = self._batch(batch_id)
        if batch["status"] == "closed":
            report = self.review(principal, batch_id)
            report["replayed"] = True
            return report
        if batch["status"] not in {"open", "reconciled"}:
            raise ConflictError(
                "批次当前状态禁止关闭",
                context={"batch_id": batch_id, "status": batch["status"]},
            )
        self._generate_shortages(batch_id, now)
        # 暂停中的会话视为断点，关闭时自动完成；进行中的会话必须显式处理
        self.connection.execute(
            """UPDATE receipt_scan_sessions SET state='completed',completed_at=?,updated_at=?
               WHERE batch_id=? AND state='paused'""",
            (now, now, batch_id),
        )
        storable, reasons = self._evaluate_storable(self._batch(batch_id))
        if not storable:
            raise ConflictError("批次尚不能关闭", context={"reasons": reasons})
        cursor = self.connection.execute(
            "UPDATE receipt_batches SET status='closed',updated_at=? WHERE id=? AND status IN ('open','reconciled')",
            (now, batch_id),
        )
        if cursor.rowcount != 1:
            raise ConflictError("批次状态已变化，请刷新后重试")
        received = self.connection.execute(
            "SELECT * FROM samples WHERE batch_id=? AND lifecycle_state='received'", (batch_id,)
        ).fetchall()
        for row in received:
            sample = dict(row)
            self.connection.execute(
                "UPDATE samples SET lifecycle_state='available',version=version+1,updated_at=? WHERE id=? AND lifecycle_state='received'",
                (now, sample["id"]),
            )
            self.samples.append_event(
                sample["id"], "stored", principal.user_id, now,
                from_state="received", to_state="available",
                details={"batch_id": batch_id, "trigger": "receiving.close"},
            )
        self._refresh_counts(batch_id, now)
        self.audit.record(
            principal,
            "receiving.close",
            "receipt_batch",
            str(batch_id),
            metadata={"stored_sample_count": len(received)},
        )
        report = self.review(principal, batch_id)
        report["replayed"] = False
        return report
