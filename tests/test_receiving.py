from __future__ import annotations


def _create_user(client, admin, username, roles):
    created = client.post(
        "/api/users",
        headers=admin["headers"],
        json={
            "username": username,
            "password": "Receiver!234",
            "display_name": username,
            "role_codes": roles,
        },
    )
    assert created.status_code == 201, created.text
    login = client.post(
        "/api/auth/login",
        json={"username": username, "password": "Receiver!234", "client_label": "tests"},
    )
    assert login.status_code == 200, login.text
    return {"headers": {"Authorization": f"Bearer {login.json()['token']}"}, "body": login.json()}


def _start_session(client, admin, batch_code="RCV-B-001", expected=3):
    response = client.post(
        "/api/receiving/sessions",
        headers=admin["headers"],
        json={
            "batch_code": batch_code,
            "project_code": "P-RCV",
            "expected_count": expected,
            "manifest_lines": [
                {"line_no": 1, "sample_code": f"{batch_code}-A", "sample_type": "水样", "quantity": 100, "unit": "mL"},
                {"line_no": 2, "sample_code": f"{batch_code}-B", "sample_type": "水样", "quantity": 100, "unit": "mL"},
                {"line_no": 3, "sample_code": f"{batch_code}-C", "sample_type": "土壤", "quantity": 50, "unit": "g"},
            ][:expected],
        },
    )
    assert response.status_code == 201, response.text
    return response.json()


def test_full_receive_reconcile_close_cycle(client, admin):
    session = _start_session(client, admin)
    session_id = session["id"]
    assert session["state"] == "open"
    assert session["manifest_versions"][0]["version"] == 1

    first = client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={
            "scan_group": "wave-1",
            "device_label": "PDA-01",
            "items": [
                {"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"},
                {"scan_code": "RCV-B-001-B", "quantity": 100, "unit": "mL"},
            ],
        },
    )
    assert first.status_code == 200, first.text
    assert first.json()["accepted_count"] == 2

    second = client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={"scan_group": "wave-2", "items": [{"scan_code": "RCV-B-001-C", "quantity": 50, "unit": "g"}]},
    )
    assert second.status_code == 200, second.text
    assert second.json()["accepted_count"] == 1

    reconciled = client.post(f"/api/receiving/sessions/{session_id}/reconcile", headers=admin["headers"])
    assert reconciled.status_code == 200, reconciled.text
    assert reconciled.json()["differences"] == []
    assert reconciled.json()["can_close"] is True

    closed = client.post(f"/api/receiving/sessions/{session_id}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text
    assert closed.json()["state"] == "closed"
    assert closed.json()["reconciliation"]["warehouse_eligible"] is True
    assert closed.json()["close_summary"]["accepted_count"] == 3

    samples = client.get("/api/samples?batch_id={}".format(closed.json()["batch_id"]), headers=admin["headers"]).json()
    assert {item["lifecycle_state"] for item in samples} == {"available"}

    again = client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={"scan_group": "wave-3", "items": [{"scan_code": "RCV-B-001-X"}]},
    )
    assert again.status_code == 409


def test_scan_group_replay_is_idempotent(client, admin):
    session = _start_session(client, admin)
    session_id = session["id"]
    payload = {
        "scan_group": "wave-replay",
        "items": [{"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"}],
    }
    first = client.post(f"/api/receiving/sessions/{session_id}/scans", headers=admin["headers"], json=payload)
    second = client.post(f"/api/receiving/sessions/{session_id}/scans", headers=admin["headers"], json=payload)
    assert first.status_code == second.status_code == 200
    assert first.json()["replayed"] is False
    assert second.json()["replayed"] is True
    detail = client.get(f"/api/receiving/sessions/{session_id}", headers=admin["headers"]).json()
    assert detail["reconciliation"]["counts"]["accepted_count"] == 1


def test_two_receivers_cannot_double_count(client, admin):
    receiver = _create_user(client, admin, "receiver-b", ["sample_manager"])
    session = _start_session(client, admin)
    session_id = session["id"]
    first = client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={"scan_group": "r1-wave", "items": [{"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"}]},
    )
    assert first.status_code == 200, first.text
    other = client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=receiver["headers"],
        json={"scan_group": "r2-wave", "items": [{"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"}]},
    )
    assert other.status_code == 200, other.text
    assert other.json()["duplicate_count"] == 1
    detail = client.get(f"/api/receiving/sessions/{session_id}", headers=admin["headers"]).json()
    counts = detail["reconciliation"]["counts"]
    assert counts["accepted_count"] == 1
    assert counts["duplicate_scan_count"] == 1


def test_missing_line_must_be_explained_before_close(client, admin):
    session = _start_session(client, admin)
    session_id = session["id"]
    client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={
            "scan_group": "wave-1",
            "items": [
                {"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"},
                {"scan_code": "RCV-B-001-B", "quantity": 100, "unit": "mL"},
            ],
        },
    )
    reconciled = client.post(f"/api/receiving/sessions/{session_id}/reconcile", headers=admin["headers"])
    assert reconciled.status_code == 200
    differences = reconciled.json()["differences"]
    assert [d["key"] for d in differences] == ["missing:RCV-B-001-C"]
    assert reconciled.json()["can_close"] is False

    blocked = client.post(f"/api/receiving/sessions/{session_id}/close", headers=admin["headers"])
    assert blocked.status_code == 409
    assert blocked.json()["error"]["context"]["blockers"][0]["type"] == "unexplained_difference"

    explained = client.post(
        f"/api/receiving/sessions/{session_id}/differences/explanations",
        headers=admin["headers"],
        json={"diff_type": "missing", "sample_code": "RCV-B-001-C", "explanation": "供应商确认漏发，下期补发"},
    )
    assert explained.status_code == 200, explained.text
    closed = client.post(f"/api/receiving/sessions/{session_id}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text
    assert closed.json()["close_summary"]["accepted_count"] == 2
    assert closed.json()["reconciliation"]["counts"]["shortage_count"] == 1


def test_label_conflict_becomes_pending_and_auto_quarantine(client, admin):
    session = _start_session(client, admin)
    session_id = session["id"]
    scanned = client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={
            "scan_group": "wave-1",
            "items": [{"scan_code": "RCV-B-001-A", "sample_type": "土壤", "quantity": 100, "unit": "mL"}],
        },
    )
    assert scanned.status_code == 200, scanned.text
    assert scanned.json()["pending_count"] == 1
    pending = scanned.json()["scans"][0]
    assert pending["outcome"] == "pending"

    detail = client.get(f"/api/receiving/sessions/{session_id}", headers=admin["headers"]).json()
    pending_items = detail["pending_items"]
    assert len(pending_items) == 1
    assert pending_items[0]["reason_code"] == "label_conflict"
    assert pending_items[0]["severity"] == "high"
    assert detail["anomalies"][0]["severity"] == "high"

    resolved = client.post(
        f"/api/receiving/sessions/{session_id}/pending/{pending_items[0]['id']}/resolutions",
        headers=admin["headers"],
        json={"resolution": "received", "sample_type": "水样", "quantity": 100, "unit": "mL", "note": "复核确认为水样"},
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["status"] == "received"
    sample = resolved.json()["sample"]
    assert sample["lifecycle_state"] == "received"


def test_unexpected_item_requires_explanation(client, admin):
    session = _start_session(client, admin)
    session_id = session["id"]
    scanned = client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={"scan_group": "wave-1", "items": [{"scan_code": "RCV-B-001-Z"}]},
    )
    assert scanned.json()["pending_count"] == 1
    detail = client.get(f"/api/receiving/sessions/{session_id}", headers=admin["headers"]).json()
    pending_id = detail["pending_items"][0]["id"]
    assert detail["pending_items"][0]["reason_code"] == "unexpected_item"

    resolved = client.post(
        f"/api/receiving/sessions/{session_id}/pending/{pending_id}/resolutions",
        headers=admin["headers"],
        json={"resolution": "received", "sample_type": "水样", "quantity": 20, "unit": "mL"},
    )
    assert resolved.status_code == 200, resolved.text

    client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={
            "scan_group": "wave-2",
            "items": [
                {"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"},
                {"scan_code": "RCV-B-001-B", "quantity": 100, "unit": "mL"},
                {"scan_code": "RCV-B-001-C", "quantity": 50, "unit": "g"},
            ],
        },
    )
    reconciled = client.post(f"/api/receiving/sessions/{session_id}/reconcile", headers=admin["headers"])
    keys = [d["key"] for d in reconciled.json()["differences"]]
    assert keys == ["unexpected:RCV-B-001-Z"]
    assert reconciled.json()["can_close"] is False

    client.post(
        f"/api/receiving/sessions/{session_id}/differences/explanations",
        headers=admin["headers"],
        json={"diff_type": "unexpected", "sample_code": "RCV-B-001-Z", "explanation": "供应商多发备件，登记留存"},
    )
    closed = client.post(f"/api/receiving/sessions/{session_id}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text
    assert closed.json()["close_summary"]["accepted_count"] == 4


def test_rejection_records_and_counts(client, admin):
    session = _start_session(client, admin)
    session_id = session["id"]
    rejected = client.post(
        f"/api/receiving/sessions/{session_id}/rejections",
        headers=admin["headers"],
        json={"sample_code": "RCV-B-001-C", "reason_code": "damaged_packaging", "severity": "medium", "note": "外箱破损"},
    )
    assert rejected.status_code == 201, rejected.text
    duplicate = client.post(
        f"/api/receiving/sessions/{session_id}/rejections",
        headers=admin["headers"],
        json={"sample_code": "RCV-B-001-C", "reason_code": "damaged_packaging", "severity": "medium"},
    )
    assert duplicate.status_code == 409

    client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={
            "scan_group": "wave-1",
            "items": [
                {"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"},
                {"scan_code": "RCV-B-001-B", "quantity": 100, "unit": "mL"},
            ],
        },
    )
    reconciled = client.post(f"/api/receiving/sessions/{session_id}/reconcile", headers=admin["headers"])
    assert reconciled.json()["differences"] == []
    assert reconciled.json()["counts"]["rejected_count"] == 1
    closed = client.post(f"/api/receiving/sessions/{session_id}/close", headers=admin["headers"])
    assert closed.status_code == 200
    assert closed.json()["close_summary"]["rejected_count"] == 1


def test_high_risk_quarantine_blocks_loan_and_consume(client, admin):
    session = _start_session(client, admin)
    session_id = session["id"]
    client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={
            "scan_group": "wave-1",
            "items": [
                {"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"},
                {"scan_code": "RCV-B-001-B", "quantity": 100, "unit": "mL"},
                {"scan_code": "RCV-B-001-C", "quantity": 50, "unit": "g"},
            ],
        },
    )
    rejected = client.post(
        f"/api/receiving/sessions/{session_id}/rejections",
        headers=admin["headers"],
        json={"sample_code": "RCV-B-001-B", "reason_code": "contamination", "severity": "critical", "note": "疑似污染"},
    )
    assert rejected.status_code == 201, rejected.text
    assert rejected.json()["anomaly_id"] is not None

    detail = client.get(f"/api/receiving/sessions/{session_id}", headers=admin["headers"]).json()
    holds = detail["holds"]
    assert len(holds) == 1 and holds[0]["active"] == 1
    sample_b = client.get(f"/api/samples/{holds[0]['sample_id']}", headers=admin["headers"]).json()
    assert sample_b["lifecycle_state"] == "quarantined"

    loan = client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": holds[0]["sample_id"], "borrower_user_id": admin["body"]["user"]["id"], "quantity": 10, "due_at": "2026-10-01T00:00:00+00:00"},
    )
    assert loan.status_code == 409
    consume = client.post(
        f"/api/samples/{holds[0]['sample_id']}/consumptions",
        headers=admin["headers"],
        json={"experiment_code": "EXP-X", "quantity": 5, "idempotency_key": "quar-001"},
    )
    assert consume.status_code == 409

    reconciled = client.post(f"/api/receiving/sessions/{session_id}/reconcile", headers=admin["headers"])
    assert reconciled.json()["can_close"] is True
    closed = client.post(f"/api/receiving/sessions/{session_id}/close", headers=admin["headers"])
    assert closed.status_code == 200
    assert closed.json()["close_summary"]["quarantined_sample_ids"] == [holds[0]["sample_id"]]

    released = client.post(
        f"/api/receiving/sessions/{session_id}/holds/{holds[0]['id']}/release",
        headers=admin["headers"],
        json={"note": "复测合格，解除隔离"},
    )
    assert released.status_code == 200, released.text
    assert released.json()["active"] == 0
    sample_after = client.get(f"/api/samples/{holds[0]['sample_id']}", headers=admin["headers"]).json()
    assert sample_after["lifecycle_state"] == "available"


def test_manifest_revision_keeps_versions_and_reasons(client, admin):
    session = _start_session(client, admin)
    session_id = session["id"]
    revised = client.put(
        f"/api/receiving/sessions/{session_id}/manifest",
        headers=admin["headers"],
        json={
            "expected_count": 4,
            "revision_reason": "供应商补发一件并更新箱单",
            "manifest_lines": [
                {"line_no": 1, "sample_code": "RCV-B-001-A", "sample_type": "水样", "quantity": 100, "unit": "mL"},
                {"line_no": 2, "sample_code": "RCV-B-001-B", "sample_type": "水样", "quantity": 100, "unit": "mL"},
                {"line_no": 3, "sample_code": "RCV-B-001-C", "sample_type": "土壤", "quantity": 50, "unit": "g"},
                {"line_no": 4, "sample_code": "RCV-B-001-D", "sample_type": "土壤", "quantity": 50, "unit": "g"},
            ],
        },
    )
    assert revised.status_code == 200, revised.text
    versions = revised.json()["manifest_versions"]
    assert [v["version"] for v in versions] == [1, 2]
    assert versions[1]["revision_reason"] == "供应商补发一件并更新箱单"
    assert revised.json()["current_version"] == 2

    client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={"scan_group": "wave-1", "items": [{"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"}]},
    )
    removed = client.put(
        f"/api/receiving/sessions/{session_id}/manifest",
        headers=admin["headers"],
        json={
            "expected_count": 3,
            "revision_reason": "尝试移除已接收明细",
            "manifest_lines": [
                {"line_no": 2, "sample_code": "RCV-B-001-B", "sample_type": "水样", "quantity": 100, "unit": "mL"},
                {"line_no": 3, "sample_code": "RCV-B-001-C", "sample_type": "土壤", "quantity": 50, "unit": "g"},
                {"line_no": 4, "sample_code": "RCV-B-001-D", "sample_type": "土壤", "quantity": 50, "unit": "g"},
            ],
        },
    )
    assert removed.status_code == 409


def test_open_pending_blocks_close_until_resolved(client, admin):
    session = _start_session(client, admin)
    session_id = session["id"]
    client.post(
        f"/api/receiving/sessions/{session_id}/pending",
        headers=admin["headers"],
        json={"sample_code": "RCV-B-001-C", "reason_code": "damaged_packaging", "severity": "medium", "note": "包装破损待确认"},
    )
    client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={
            "scan_group": "wave-1",
            "items": [
                {"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"},
                {"scan_code": "RCV-B-001-B", "quantity": 100, "unit": "mL"},
            ],
        },
    )
    client.post(f"/api/receiving/sessions/{session_id}/reconcile", headers=admin["headers"])
    blocked = client.post(f"/api/receiving/sessions/{session_id}/close", headers=admin["headers"])
    assert blocked.status_code == 409
    blocker_types = {b["type"] for b in blocked.json()["error"]["context"]["blockers"]}
    assert "pending_open" in blocker_types

    detail = client.get(f"/api/receiving/sessions/{session_id}", headers=admin["headers"]).json()
    pending_id = detail["pending_items"][0]["id"]
    resolved = client.post(
        f"/api/receiving/sessions/{session_id}/pending/{pending_id}/resolutions",
        headers=admin["headers"],
        json={"resolution": "rejected", "reject_reason_code": "damaged_packaging", "note": "破损无法修复"},
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["status"] == "rejected"
    closed = client.post(f"/api/receiving/sessions/{session_id}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text


def test_reopen_allows_resume_after_reconcile(client, admin):
    session = _start_session(client, admin)
    session_id = session["id"]
    client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={"scan_group": "wave-1", "items": [{"scan_code": "RCV-B-001-A", "quantity": 100, "unit": "mL"}]},
    )
    client.post(f"/api/receiving/sessions/{session_id}/reconcile", headers=admin["headers"])
    reopened = client.post(f"/api/receiving/sessions/{session_id}/reopen", headers=admin["headers"])
    assert reopened.status_code == 200
    assert reopened.json()["state"] == "open"
    resumed = client.post(
        f"/api/receiving/sessions/{session_id}/scans",
        headers=admin["headers"],
        json={
            "scan_group": "wave-2",
            "items": [
                {"scan_code": "RCV-B-001-B", "quantity": 100, "unit": "mL"},
                {"scan_code": "RCV-B-001-C", "quantity": 50, "unit": "g"},
            ],
        },
    )
    assert resumed.status_code == 200
    assert resumed.json()["accepted_count"] == 2
    client.post(f"/api/receiving/sessions/{session_id}/reconcile", headers=admin["headers"])
    closed = client.post(f"/api/receiving/sessions/{session_id}/close", headers=admin["headers"])
    assert closed.status_code == 200
    assert closed.json()["close_summary"]["accepted_count"] == 3


def test_permission_required_for_receiving(client, admin):
    researcher = _create_user(client, admin, "researcher-x", ["researcher"])
    session = _start_session(client, admin)
    denied = client.post(
        f"/api/receiving/sessions/{session['id']}/scans",
        headers=researcher["headers"],
        json={"scan_group": "wave-1", "items": [{"scan_code": "RCV-B-001-A"}]},
    )
    assert denied.status_code == 403
    readable = client.get(f"/api/receiving/sessions/{session['id']}", headers=researcher["headers"])
    assert readable.status_code == 200
