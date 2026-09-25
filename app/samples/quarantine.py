from __future__ import annotations

import json
import sqlite3
from typing import Any

HIGH_RISK_SEVERITIES = ("high", "critical")
TERMINAL_ANOMALY_STATES = ("resolved", "dismissed")
TERMINAL_SAMPLE_STATES = ("destroyed", "consumed", "pending_destruction")


def _append_event(
    connection: sqlite3.Connection,
    sample_id: int,
    event_type: str,
    actor_user_id: int | None,
    now: str,
    *,
    from_state: str | None = None,
    to_state: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    connection.execute(
        """INSERT INTO sample_events(sample_id,event_type,actor_user_id,quantity_delta,from_state,to_state,details_json,correlation_id,occurred_at)
           VALUES(?,?,?,0,?,?,?,NULL,?)""",
        (
            sample_id, event_type, actor_user_id, from_state, to_state,
            json.dumps(details or {}, ensure_ascii=False), now,
        ),
    )


def apply_quarantine(
    connection: sqlite3.Connection,
    anomaly: dict[str, Any],
    actor_user_id: int | None,
    now: str,
) -> list[int]:
    """把高风险异常关联的样品置入隔离状态，返回本次新隔离的样品 id 列表。"""
    if anomaly["severity"] not in HIGH_RISK_SEVERITIES:
        return []
    if anomaly.get("sample_id"):
        rows = connection.execute(
            "SELECT * FROM samples WHERE id=?", (anomaly["sample_id"],)
        ).fetchall()
    elif anomaly.get("batch_id"):
        rows = connection.execute(
            "SELECT * FROM samples WHERE batch_id=?", (anomaly["batch_id"],)
        ).fetchall()
    else:
        return []
    quarantined: list[int] = []
    for row in rows:
        sample = dict(row)
        if sample["lifecycle_state"] in TERMINAL_SAMPLE_STATES:
            continue
        cursor = connection.execute(
            """INSERT OR IGNORE INTO anomaly_quarantines(anomaly_id,sample_id,previous_state,created_at)
               VALUES(?,?,?,?)""",
            (anomaly["id"], sample["id"], sample["lifecycle_state"], now),
        )
        if cursor.rowcount != 1:
            continue
        if sample["lifecycle_state"] == "quarantined":
            continue
        connection.execute(
            "UPDATE samples SET lifecycle_state='quarantined',version=version+1,updated_at=? WHERE id=?",
            (now, sample["id"]),
        )
        _append_event(
            connection,
            sample["id"],
            "quarantined",
            actor_user_id,
            now,
            from_state=sample["lifecycle_state"],
            to_state="quarantined",
            details={"anomaly_id": anomaly["id"], "case_code": anomaly["case_code"]},
        )
        quarantined.append(sample["id"])
    if anomaly.get("batch_id"):
        sync_batch_status(connection, anomaly["batch_id"], now)
    return quarantined


def release_quarantines(
    connection: sqlite3.Connection,
    anomaly_id: int,
    now: str,
) -> list[int]:
    """解除指定异常持有的隔离；样品不再被任何高风险异常持有时恢复原始状态。"""
    links = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM anomaly_quarantines WHERE anomaly_id=? AND released_at IS NULL",
            (anomaly_id,),
        ).fetchall()
    ]
    released: list[int] = []
    for link in links:
        connection.execute(
            "UPDATE anomaly_quarantines SET released_at=? WHERE id=?", (now, link["id"])
        )
        remaining = connection.execute(
            """SELECT COUNT(*) FROM anomaly_quarantines q
               JOIN anomaly_cases a ON a.id=q.anomaly_id
               WHERE q.sample_id=? AND q.released_at IS NULL
                 AND a.severity IN ('high','critical') AND a.state NOT IN ('resolved','dismissed')""",
            (link["sample_id"],),
        ).fetchone()[0]
        if remaining:
            continue
        sample = connection.execute(
            "SELECT * FROM samples WHERE id=?", (link["sample_id"],)
        ).fetchone()
        if sample is None or sample["lifecycle_state"] != "quarantined":
            continue
        first = connection.execute(
            "SELECT previous_state FROM anomaly_quarantines WHERE sample_id=? ORDER BY id LIMIT 1",
            (link["sample_id"],),
        ).fetchone()
        target = first[0] if first and first[0] != "quarantined" else "available"
        connection.execute(
            "UPDATE samples SET lifecycle_state=?,version=version+1,updated_at=? WHERE id=?",
            (target, now, link["sample_id"]),
        )
        _append_event(
            connection,
            link["sample_id"],
            "quarantine.released",
            None,
            now,
            from_state="quarantined",
            to_state=target,
            details={"anomaly_id": anomaly_id},
        )
        released.append(link["sample_id"])
    anomaly = connection.execute(
        "SELECT * FROM anomaly_cases WHERE id=?", (anomaly_id,)
    ).fetchone()
    if anomaly and anomaly["batch_id"]:
        sync_batch_status(connection, anomaly["batch_id"], now)
    return released


def sync_batch_status(connection: sqlite3.Connection, batch_id: int, now: str) -> None:
    """根据高风险异常与差异处理进度推导批次状态（open/reconciled/quarantined）。"""
    row = connection.execute(
        "SELECT * FROM receipt_batches WHERE id=?", (batch_id,)
    ).fetchone()
    if row is None:
        return
    batch = dict(row)
    if batch["status"] not in {"open", "reconciled", "quarantined"}:
        return
    active_high = connection.execute(
        """SELECT COUNT(*) FROM anomaly_cases
           WHERE batch_id=? AND severity IN ('high','critical') AND state NOT IN ('resolved','dismissed')""",
        (batch_id,),
    ).fetchone()[0]
    if active_high:
        target = "quarantined"
    elif batch["status"] == "quarantined":
        pending = connection.execute(
            "SELECT COUNT(*) FROM receipt_discrepancies WHERE batch_id=? AND state='pending'",
            (batch_id,),
        ).fetchone()[0]
        outstanding = connection.execute(
            """SELECT COUNT(*) FROM receipt_manifest_items m
               WHERE m.batch_id=? AND m.is_current=1
                 AND NOT EXISTS(SELECT 1 FROM receipt_item_claims c WHERE c.batch_id=m.batch_id AND c.item_code=m.item_code)
                 AND NOT EXISTS(SELECT 1 FROM receipt_rejections r WHERE r.batch_id=m.batch_id AND r.line_key=m.line_key)
                 AND NOT EXISTS(SELECT 1 FROM receipt_discrepancies d
                                WHERE d.batch_id=m.batch_id AND d.line_key=m.line_key
                                  AND d.kind='shortage' AND d.state='explained')""",
            (batch_id,),
        ).fetchone()[0]
        target = "reconciled" if pending == 0 and outstanding == 0 else "open"
    else:
        return
    if target != batch["status"]:
        connection.execute(
            "UPDATE receipt_batches SET status=?,updated_at=? WHERE id=? AND status=?",
            (target, now, batch_id, batch["status"]),
        )
