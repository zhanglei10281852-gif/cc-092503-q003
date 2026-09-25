# 科研样品全生命周期管理服务

这是一个面向科研机构样品库、实验室和课题组的模块化后端，集中管理样品接收、分装、借用、归还、消耗、销毁、库存盘点、谱系事件、保管位置、异常记录、登录权限、审计以及可恢复后台任务。项目使用 FastAPI 与 SQLite，所有运行数据保存在单个本地数据库文件中，不依赖另行部署的数据库、缓存或消息队列。

## 已有能力

- 身份与权限：支持引导管理员、登录、会话、用户、角色和细粒度权限。
- 批次与二维码：接收批次保存项目、数量和稳定二维码载荷。
- 样品档案：登记样品、数量、单位、保管位置和生命周期状态。
- 分装谱系：一次事务内扣减母样、创建子样、记录损耗与事件链。
- 借用归还：保存借用数量、到期时间、部分归还和最终归还状态。
- 实验消耗：使用幂等键登记消耗，防止重复请求二次扣减。
- 位置脱敏：普通权限只能看到受限位置的替代码，授权人员可查看精确位置。
- 双人审批：高风险操作要求申请人与审批人分离，并累计不同审批人的决定。
- 异常追踪：异常可以关联样品或接收批次，保存严重度和处理状态。
- 接收复核：箱单、实收扫描、拒收项、待查项与差异解释纳入同一批次状态机（开放 → 差异复核 → 关闭），支持分次扫描与断点继续，关闭前必须解释全部差异。
- 高风险隔离：高风险异常自动把相关样品放入隔离状态并阻止借用、消耗与分装，解除需授权人员登记原因。
- 审计与任务：关键身份及业务操作留痕，后台任务支持去重、领取与完成。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/samples.db`，可用 `SAMPLE_DATABASE_PATH` 指定其他路径。

## 初始化与完整性检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 测试

```bash
python -m pytest
```

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
```

## 接收复核流程

1. `POST /api/receiving/sessions` 开启接收批次并登记预期箱单（可含逐行明细）。
2. `POST /api/receiving/sessions/{id}/scans` 分次提交实收扫描；同一 `scan_group` 重复提交按幂等回放，不同接收员扫同一条码自动去重，不会重复计数。
3. `POST /api/receiving/sessions/{id}/rejections` 登记拒收；`POST .../pending` 登记待查；待查件通过 `POST .../pending/{pid}/resolutions` 判为收讫或拒收。
4. `PUT /api/receiving/sessions/{id}/manifest` 修正箱单，每次修正保存完整版本与原因，已接收/已拒收明细不可移除。
5. `POST /api/receiving/sessions/{id}/reconcile` 生成差异对账；少件与清单外实物必须通过 `POST .../differences/explanations` 登记解释，否则无法关闭。
6. `POST /api/receiving/sessions/{id}/close` 关闭批次：无阻碍项的样品转为可入库，返回数量对账、异常关联与是否可入库结论；`POST .../reopen` 可退回继续扫描。
7. 高风险（high/critical）异常自动生成异常案并把相关样品隔离，借用、消耗、分装均被阻止；`POST .../holds/{hid}/release` 由具备异常管理权限的人员解除。
