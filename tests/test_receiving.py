from __future__ import annotations

import threading


def make_batch(client, admin, code="RCV-2026-001", expected=2):
    response = client.post(
        "/api/samples/batches",
        headers=admin["headers"],
        json={"batch_code": code, "project_code": "P-RCV", "expected_count": expected},
    )
    assert response.status_code == 201, response.text
    return response.json()


def declare(client, admin, batch_id, lines):
    response = client.post(
        f"/api/receiving/batches/{batch_id}/manifest",
        headers=admin["headers"],
        json={"lines": lines},
    )
    assert response.status_code == 201, response.text
    return response.json()


def open_session(client, admin, batch_id):
    response = client.post(
        f"/api/receiving/batches/{batch_id}/scan-sessions",
        headers=admin["headers"],
        json={"note": "到货扫码"},
    )
    assert response.status_code == 201, response.text
    return response.json()


def scan(client, admin, session_id, item_code, key, quantity=None, damaged=False):
    payload = {"item_code": item_code, "idempotency_key": key, "damaged": damaged}
    if quantity is not None:
        payload["quantity"] = quantity
    return client.post(
        f"/api/receiving/scan-sessions/{session_id}/scans",
        headers=admin["headers"],
        json=payload,
    )


def make_receiver(client, admin, username):
    created = client.post(
        "/api/users",
        headers=admin["headers"],
        json={
            "username": username,
            "password": "Receiver!234",
            "display_name": f"接收员{username}",
            "role_codes": ["sample_manager"],
        },
    )
    assert created.status_code == 201, created.text
    login = client.post("/api/auth/login", json={"username": username, "password": "Receiver!234"})
    assert login.status_code == 200, login.text
    return {"headers": {"Authorization": f"Bearer {login.json()['token']}"}}


def two_line_batch(client, admin, code="RCV-2026-001"):
    batch = make_batch(client, admin, code=code)
    declare(
        client,
        admin,
        batch["id"],
        [
            {"line_key": "L1", "item_code": "ITEM-A", "sample_type": "土壤", "expected_quantity": 100, "unit": "g"},
            {"line_key": "L2", "item_code": "ITEM-B", "sample_type": "水样", "expected_quantity": 50, "unit": "mL"},
        ],
    )
    return batch


def test_manifest_revision_keeps_versions_and_reasons(client, admin):
    batch = two_line_batch(client, admin)
    revised = client.post(
        f"/api/receiving/batches/{batch['id']}/manifest/L1/revisions",
        headers=admin["headers"],
        json={"expected_quantity": 90, "change_reason": "供应商箱单更正"},
    )
    assert revised.status_code == 201, revised.text
    assert revised.json()["revision"] == 2
    assert revised.json()["change_reason"] == "供应商箱单更正"

    current = client.get(f"/api/receiving/batches/{batch['id']}/manifest", headers=admin["headers"])
    line_l1 = [line for line in current.json()["lines"] if line["line_key"] == "L1"][0]
    assert line_l1["expected_quantity"] == 90
    assert line_l1["revision"] == 2

    history = client.get(f"/api/receiving/batches/{batch['id']}/manifest/revisions", headers=admin["headers"])
    versions = [row for row in history.json() if row["line_key"] == "L1"]
    assert len(versions) == 2
    assert versions[0]["change_reason"] == "初始登记"
    assert versions[0]["is_current"] == 0
    assert versions[1]["is_current"] == 1

    no_change = client.post(
        f"/api/receiving/batches/{batch['id']}/manifest/L1/revisions",
        headers=admin["headers"],
        json={"expected_quantity": 90, "change_reason": "内容完全一致"},
    )
    assert no_change.status_code == 422


def test_full_flow_with_pause_resume_and_close(client, admin):
    batch = two_line_batch(client, admin)
    session = open_session(client, admin, batch["id"])

    first = scan(client, admin, session["id"], "ITEM-A", "scan-001")
    assert first.status_code == 201, first.text
    assert first.json()["event"]["outcome"] == "matched"
    assert first.json()["sample"]["lifecycle_state"] == "received"

    paused = client.post(f"/api/receiving/scan-sessions/{session['id']}/pause", headers=admin["headers"])
    assert paused.json()["state"] == "paused"
    blocked = scan(client, admin, session["id"], "ITEM-B", "scan-002")
    assert blocked.status_code == 409

    resumed = client.post(f"/api/receiving/scan-sessions/{session['id']}/resume", headers=admin["headers"])
    assert resumed.json()["state"] == "active"
    second = scan(client, admin, session["id"], "ITEM-B", "scan-002")
    assert second.status_code == 201
    assert second.json()["event"]["outcome"] == "matched"

    # 未入库的样品禁止消耗
    consume = client.post(
        f"/api/samples/{first.json()['sample']['id']}/consumptions",
        headers=admin["headers"],
        json={"experiment_code": "EXP-X", "quantity": 1, "idempotency_key": "exp-x-1"},
    )
    assert consume.status_code == 409

    client.post(f"/api/receiving/scan-sessions/{session['id']}/complete", headers=admin["headers"])
    reconciled = client.post(f"/api/receiving/batches/{batch['id']}/reconcile", headers=admin["headers"])
    assert reconciled.status_code == 200, reconciled.text
    assert reconciled.json()["batch"]["status"] == "reconciled"
    assert reconciled.json()["pending_discrepancies"] == []

    review = client.get(f"/api/receiving/batches/{batch['id']}/review", headers=admin["headers"])
    assert review.json()["storable"] is True
    assert review.json()["conclusion"] == "可入库"
    assert review.json()["totals"]["count_delta"] == 0

    closed = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text
    assert closed.json()["batch"]["status"] == "closed"
    assert closed.json()["conclusion"] == "已入库"
    states = {sample["sample_code"]: sample["lifecycle_state"] for sample in closed.json()["samples"]}
    assert states == {"ITEM-A": "available", "ITEM-B": "available"}

    replayed = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert replayed.status_code == 200
    assert replayed.json()["replayed"] is True


def test_scan_replay_and_duplicate_do_not_double_count(client, admin):
    batch = two_line_batch(client, admin)
    receiver_one = make_receiver(client, admin, "receiver1")
    receiver_two = make_receiver(client, admin, "receiver2")
    session_one = open_session(client, receiver_one, batch["id"])
    session_two = open_session(client, receiver_two, batch["id"])

    first = scan(client, receiver_one, session_one["id"], "ITEM-A", "scan-100")
    assert first.json()["event"]["outcome"] == "matched"
    replay = scan(client, receiver_one, session_one["id"], "ITEM-A", "scan-100")
    assert replay.status_code == 201
    assert replay.json()["replayed"] is True
    assert replay.json()["event"]["id"] == first.json()["event"]["id"]

    other = scan(client, receiver_two, session_two["id"], "ITEM-A", "scan-200")
    assert other.status_code == 201
    assert other.json()["event"]["outcome"] == "duplicate"
    assert "sample" not in other.json()

    review = client.get(f"/api/receiving/batches/{batch['id']}/review", headers=admin["headers"])
    assert review.json()["totals"]["accepted_count"] == 1
    assert review.json()["totals"]["duplicate_count"] == 1
    assert len(review.json()["samples"]) == 1


def test_concurrent_receivers_claim_item_once(client, admin):
    batch = two_line_batch(client, admin, code="RCV-CONCURRENT")
    receiver_one = make_receiver(client, admin, "receiver-a")
    receiver_two = make_receiver(client, admin, "receiver-b")
    session_one = open_session(client, receiver_one, batch["id"])
    session_two = open_session(client, receiver_two, batch["id"])

    outcomes = []

    def scan_with(receiver, session_id, key):
        response = scan(client, receiver, session_id, "ITEM-A", key)
        assert response.status_code == 201, response.text
        outcomes.append(response.json()["event"]["outcome"])

    threads = [
        threading.Thread(target=scan_with, args=(receiver_one, session_one["id"], "race-1")),
        threading.Thread(target=scan_with, args=(receiver_two, session_two["id"], "race-2")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(outcomes) == ["duplicate", "matched"]
    review = client.get(f"/api/receiving/batches/{batch['id']}/review", headers=admin["headers"])
    assert review.json()["totals"]["accepted_count"] == 1
    assert len(review.json()["samples"]) == 1


def test_same_receiver_cannot_hold_two_active_sessions(client, admin):
    batch = two_line_batch(client, admin)
    open_session(client, admin, batch["id"])
    second = client.post(
        f"/api/receiving/batches/{batch['id']}/scan-sessions",
        headers=admin["headers"],
        json={},
    )
    assert second.status_code == 409


def test_unexpected_item_must_be_explained_before_close(client, admin):
    batch = make_batch(client, admin, code="RCV-UNEXPECTED", expected=1)
    declare(client, admin, batch["id"], [{"item_code": "ITEM-A", "sample_type": "土壤", "expected_quantity": 100, "unit": "g"}])
    session = open_session(client, admin, batch["id"])
    scan(client, admin, session["id"], "ITEM-A", "scan-001")
    extra = scan(client, admin, session["id"], "ITEM-XYZ", "scan-002", quantity=3)
    assert extra.json()["event"]["outcome"] == "unexpected"
    discrepancy_id = extra.json()["discrepancies"][0]["id"]
    client.post(f"/api/receiving/scan-sessions/{session['id']}/complete", headers=admin["headers"])

    blocked = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert blocked.status_code == 409
    assert any("差异" in reason for reason in blocked.json()["error"]["context"]["reasons"])

    explained = client.post(
        f"/api/receiving/discrepancies/{discrepancy_id}/explain",
        headers=admin["headers"],
        json={"explanation": "供应商多发赠品，已退回物流"},
    )
    assert explained.status_code == 200, explained.text
    assert explained.json()["state"] == "explained"

    closed = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text
    assert closed.json()["conclusion"] == "已入库"
    assert closed.json()["totals"]["unexpected_count"] == 1


def test_shortage_generated_by_reconcile_blocks_close(client, admin):
    batch = two_line_batch(client, admin)
    session = open_session(client, admin, batch["id"])
    scan(client, admin, session["id"], "ITEM-A", "scan-001")
    client.post(f"/api/receiving/scan-sessions/{session['id']}/complete", headers=admin["headers"])

    reconciled = client.post(f"/api/receiving/batches/{batch['id']}/reconcile", headers=admin["headers"])
    assert reconciled.json()["batch"]["status"] == "open"
    shortages = reconciled.json()["generated_shortages"]
    assert len(shortages) == 1
    assert shortages[0]["kind"] == "shortage"
    assert shortages[0]["item_code"] == "ITEM-B"

    blocked = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert blocked.status_code == 409

    explained = client.post(
        f"/api/receiving/discrepancies/{shortages[0]['id']}/explain",
        headers=admin["headers"],
        json={"explanation": "供应商确认漏发，下周补发并另建批次"},
    )
    assert explained.status_code == 200
    closed = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text
    assert closed.json()["totals"]["count_delta"] == -1


def test_late_arrival_scan_auto_clears_shortage(client, admin):
    batch = two_line_batch(client, admin)
    session = open_session(client, admin, batch["id"])
    scan(client, admin, session["id"], "ITEM-A", "scan-001")
    client.post(f"/api/receiving/batches/{batch['id']}/reconcile", headers=admin["headers"])
    pending = client.get(
        f"/api/receiving/batches/{batch['id']}/discrepancies?state=pending", headers=admin["headers"]
    )
    assert len(pending.json()) == 1

    late = scan(client, admin, session["id"], "ITEM-B", "scan-002")
    assert late.json()["event"]["outcome"] == "matched"
    pending_after = client.get(
        f"/api/receiving/batches/{batch['id']}/discrepancies?state=pending", headers=admin["headers"]
    )
    assert pending_after.json() == []
    explained = client.get(
        f"/api/receiving/batches/{batch['id']}/discrepancies?state=explained", headers=admin["headers"]
    )
    assert explained.json()[0]["explanation"] == "补扫接收，少件差异自动核销"


def test_damaged_scan_quarantines_and_blocks_until_resolved(client, admin):
    batch = two_line_batch(client, admin)
    session = open_session(client, admin, batch["id"])
    damaged = scan(client, admin, session["id"], "ITEM-A", "scan-001", damaged=True)
    assert damaged.status_code == 201, damaged.text
    sample = damaged.json()["sample"]
    anomaly = damaged.json()["anomaly"]
    assert anomaly["severity"] == "high"

    detail = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"])
    assert detail.json()["lifecycle_state"] == "quarantined"

    consume = client.post(
        f"/api/samples/{sample['id']}/consumptions",
        headers=admin["headers"],
        json={"experiment_code": "EXP-D", "quantity": 1, "idempotency_key": "exp-d-1"},
    )
    assert consume.status_code == 409
    loan = client.post(
        "/api/samples/loans",
        headers=admin["headers"],
        json={"sample_id": sample["id"], "borrower_user_id": admin["body"]["user"]["id"], "quantity": 1, "due_at": "2026-10-01T00:00:00+00:00"},
    )
    assert loan.status_code == 409

    # 批次被高风险异常隔离，扫描与关闭都被阻止
    assert scan(client, admin, session["id"], "ITEM-B", "scan-002").status_code == 409
    blocked = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert blocked.status_code == 409

    no_resolution = client.post(
        f"/api/samples/anomalies/{anomaly['id']}/transitions",
        headers=admin["headers"],
        json={"state": "resolved"},
    )
    assert no_resolution.status_code == 422

    damaged_discrepancy = [
        item for item in damaged.json()["discrepancies"] if item["kind"] == "damaged"
    ][0]
    client.post(
        f"/api/receiving/discrepancies/{damaged_discrepancy['id']}/explain",
        headers=admin["headers"],
        json={"explanation": "破损包装已更换并重新封存", "anomaly_id": anomaly["id"]},
    )
    resolved = client.post(
        f"/api/samples/anomalies/{anomaly['id']}/transitions",
        headers=admin["headers"],
        json={"state": "resolved", "resolution": "更换包装后复检合格"},
    )
    assert resolved.status_code == 200, resolved.text
    assert resolved.json()["released_sample_ids"] == [sample["id"]]
    detail_after = client.get(f"/api/samples/{sample['id']}", headers=admin["headers"])
    assert detail_after.json()["lifecycle_state"] == "received"

    # 隔离解除后继续断点扫描，补扫自动核销少件差异
    second = scan(client, admin, session["id"], "ITEM-B", "scan-002")
    assert second.status_code == 201, second.text
    client.post(f"/api/receiving/scan-sessions/{session['id']}/complete", headers=admin["headers"])
    closed = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text
    assert closed.json()["conclusion"] == "已入库"


def test_label_conflict_quarantines_existing_sample(client, admin):
    location = client.post(
        "/api/samples/locations",
        headers=admin["headers"],
        json={"code": "LC-01", "building": "楼", "room": "室", "cabinet": "柜", "shelf": "层", "sensitivity": "normal", "capacity_units": 10},
    ).json()
    first_batch = make_batch(client, admin, code="RCV-SOURCE", expected=1)
    existing = client.post(
        "/api/samples",
        headers=admin["headers"],
        json={"sample_code": "DUP-CODE", "batch_id": first_batch["id"], "sample_type": "土壤", "quantity": 10, "unit": "g", "location_id": location["id"]},
    ).json()

    batch = make_batch(client, admin, code="RCV-CONFLICT", expected=1)
    declare(client, admin, batch["id"], [{"item_code": "ITEM-OK", "sample_type": "水样", "expected_quantity": 20, "unit": "mL"}])
    session = open_session(client, admin, batch["id"])
    conflict = scan(client, admin, session["id"], "DUP-CODE", "scan-900", quantity=5)
    assert conflict.json()["event"]["outcome"] == "label_conflict"
    anomaly = conflict.json()["anomaly"]

    existing_detail = client.get(f"/api/samples/{existing['id']}", headers=admin["headers"])
    assert existing_detail.json()["lifecycle_state"] == "quarantined"

    review = client.get(f"/api/receiving/batches/{batch['id']}/review", headers=admin["headers"])
    assert review.json()["totals"]["label_conflict_count"] == 1
    assert review.json()["anomalies"][0]["case_code"] == anomaly["case_code"]
    assert review.json()["discrepancies"][0]["anomaly_case_code"] == anomaly["case_code"]

    # 批次隔离期间禁止继续扫描
    assert scan(client, admin, session["id"], "ITEM-OK", "scan-901").status_code == 409

    discrepancy_id = review.json()["discrepancies"][0]["id"]
    client.post(
        f"/api/receiving/discrepancies/{discrepancy_id}/explain",
        headers=admin["headers"],
        json={"explanation": "供应商错贴标签，实物已退回", "anomaly_id": anomaly["id"]},
    )
    client.post(
        f"/api/samples/anomalies/{anomaly['id']}/transitions",
        headers=admin["headers"],
        json={"state": "resolved", "resolution": "确认错贴标签，原样品恢复可用"},
    )
    restored = client.get(f"/api/samples/{existing['id']}", headers=admin["headers"])
    assert restored.json()["lifecycle_state"] == "available"

    scan(client, admin, session["id"], "ITEM-OK", "scan-901")
    client.post(f"/api/receiving/scan-sessions/{session['id']}/complete", headers=admin["headers"])
    closed = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text


def test_quantity_mismatch_resolved_by_manifest_revision(client, admin):
    batch = make_batch(client, admin, code="RCV-QTY", expected=1)
    declare(client, admin, batch["id"], [{"item_code": "ITEM-A", "sample_type": "土壤", "expected_quantity": 100, "unit": "g"}])
    session = open_session(client, admin, batch["id"])
    scanned = scan(client, admin, session["id"], "ITEM-A", "scan-001", quantity=80)
    assert scanned.json()["discrepancies"][0]["kind"] == "quantity_mismatch"
    client.post(f"/api/receiving/scan-sessions/{session['id']}/complete", headers=admin["headers"])

    blocked = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert blocked.status_code == 409

    revised = client.post(
        f"/api/receiving/batches/{batch['id']}/manifest/ITEM-A/revisions",
        headers=admin["headers"],
        json={"expected_quantity": 80, "change_reason": "供应商确认箱单数量笔误"},
    )
    assert revised.status_code == 201, revised.text
    pending = client.get(
        f"/api/receiving/batches/{batch['id']}/discrepancies?state=pending", headers=admin["headers"]
    )
    assert pending.json() == []

    closed = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text


def test_rejection_explains_missing_line(client, admin):
    batch = two_line_batch(client, admin)
    session = open_session(client, admin, batch["id"])
    scan(client, admin, session["id"], "ITEM-A", "scan-001")
    client.post(f"/api/receiving/scan-sessions/{session['id']}/complete", headers=admin["headers"])

    rejected = client.post(
        f"/api/receiving/batches/{batch['id']}/rejections",
        headers=admin["headers"],
        json={"line_key": "L2", "reason": "运输途中泄漏，整件拒收"},
    )
    assert rejected.status_code == 201, rejected.text

    reconciled = client.post(f"/api/receiving/batches/{batch['id']}/reconcile", headers=admin["headers"])
    assert reconciled.json()["generated_shortages"] == []
    assert reconciled.json()["batch"]["status"] == "reconciled"

    closed = client.post(f"/api/receiving/batches/{batch['id']}/close", headers=admin["headers"])
    assert closed.status_code == 200, closed.text
    assert closed.json()["totals"]["rejected_count"] == 1
    assert closed.json()["totals"]["count_delta"] == 0
    line_states = {line["line_key"]: line["receipt_status"] for line in closed.json()["manifest_lines"]}
    assert line_states == {"L1": "matched", "L2": "rejected"}


def test_batch_linked_high_anomaly_quarantines_whole_batch(client, admin):
    batch = two_line_batch(client, admin)
    session = open_session(client, admin, batch["id"])
    scan(client, admin, session["id"], "ITEM-A", "scan-001")

    anomaly = client.post(
        "/api/samples/anomalies",
        headers=admin["headers"],
        json={"batch_id": batch["id"], "anomaly_type": "冷链中断", "severity": "critical", "description": "运输温度记录缺失"},
    )
    assert anomaly.status_code == 201, anomaly.text
    assert batch["id"] and anomaly.json()["quarantined_sample_ids"]

    blocked_scan = scan(client, admin, session["id"], "ITEM-B", "scan-002")
    assert blocked_scan.status_code == 409
    review = client.get(f"/api/receiving/batches/{batch['id']}/review", headers=admin["headers"])
    assert review.json()["batch"]["status"] == "quarantined"
    assert review.json()["storable"] is False

    client.post(
        f"/api/samples/anomalies/{anomaly.json()['id']}/transitions",
        headers=admin["headers"],
        json={"state": "dismissed", "resolution": "补全温度记录后确认无风险"},
    )
    review_after = client.get(f"/api/receiving/batches/{batch['id']}/review", headers=admin["headers"])
    assert review_after.json()["batch"]["status"] == "open"
    resumed = scan(client, admin, session["id"], "ITEM-B", "scan-002")
    assert resumed.status_code == 201


def test_receiving_requires_permission(client, admin):
    created = client.post(
        "/api/users",
        headers=admin["headers"],
        json={"username": "viewer", "password": "Viewer!23456", "display_name": "只读用户", "role_codes": ["auditor"]},
    )
    assert created.status_code == 201
    login = client.post("/api/auth/login", json={"username": "viewer", "password": "Viewer!23456"})
    headers = {"Authorization": f"Bearer {login.json()['token']}"}
    batch = make_batch(client, admin, code="RCV-PERM", expected=1)
    denied = client.post(
        f"/api/receiving/batches/{batch['id']}/manifest",
        headers=headers,
        json={"lines": [{"item_code": "ITEM-A", "sample_type": "土壤", "expected_quantity": 1, "unit": "g"}]},
    )
    assert denied.status_code == 403
    allowed = client.get(f"/api/receiving/batches/{batch['id']}/review", headers=headers)
    assert allowed.status_code == 200
