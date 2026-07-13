# AI 制片工作室 · 工作流内核

[![CI](https://github.com/macro-CMPL/ai-studio/actions/workflows/ci.yml/badge.svg)](https://github.com/macro-CMPL/ai-studio/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-blue.svg)](https://www.python.org/)

一个 AI 视频制片流水线的**事件溯源工作流内核**。表面是像素工作室,内核是一条可重放、
可崩溃恢复、带精确产物血缘与预算账本的多阶段生产线。

> 关注点不是"多个 Agent 依次调用",而是**工作流正确性**:动态实例图、精确 ArtifactRef
> 绑定、Lineage 失效传播、QC 回流、幂等 Provider、追加式预算账本、确定性重放。

## 核心设计(已锁定)

- **executor_kind × control_role 正交建模**:AGENT / TRANSFORM / PROVIDER / HUMAN
  × PRODUCER / EVALUATOR / GATE。
- **不可变产物版本**:`artifact_id`(版本唯一)/ `series_id`(逻辑系列)/ `revision` /
  `supersedes_id`。三维状态:验收(PROPOSED/ACCEPTED/REJECTED)、时效(CURRENT/SUPERSEDED)、
  依赖(FRESH/STALE)。
- **幂等外部副作用**:`operation_id = UUIDv5(attempt_id, logical_operation_key)`。
  技术重试不重复扣费;业务返工(新 attempt)允许重新生成。
- **追加式预算账本**:RESERVE / CAPTURE / RELEASE / ADJUSTMENT;失败但被扣费仍 CAPTURE。
- **确定性**:历史重放只走 `evolve()`,不触碰任何 Port;canonical-JSON digest 断言一致。

## 里程碑

- [x] **M1 契约与领域对象** — 枚举、不可变产物 + payload 判别联合、Stage 模板、
      TaskAttempt/ProviderOperation、预算账本条目、canonical 序列化与摘要。
- [x] **M2 Event/Command/Process Manager 内核** — 泛型 Decider(decide/evolve)+
      ProcessManager(react 返回新状态+命令);EventEnvelope/CommandEnvelope;
      稳定 event_key 派生确定性 event_id;事务性写缓冲 UoW(校验-全部→应用-全部,
      真实乐观并发 + 幂等冲突检测);ProcessedCommand 结果去重;三阶段独立 Tick
      (Command Worker / Event Pump / Outbox Relay)+ 公平 Driver;崩溃恢复接缝。
- [x] **M3 Artifact Lineage 与动态 Stage 展开** — 三类流(Project/TaskAttempt/ArtifactSeries)
      + 四个 PM(Expansion/Publish/Lineage/Recompute);ArtifactVersion 剥离状态(状态改由
      事件+投影);SeriesDecider 单调分配 revision;LineagePM 维护消费边 → 显式 ArtifactMarkedStale
      事实 → RecomputePM 选择性重算;两步展开 + partition 级序列 + 稳定 attempt_id 幂等。
      Golden:剧本→分镜(2 shot)→动态展开→出图→取代 shot_02 上游→仅 shot_02 失效重算,shot_01 不动。
- [ ] M4 幂等 Provider 与 BudgetLedger
- [ ] M5 完整 QC 回流 Golden Scenario(P0A 垂直切片)

之后:P0B(SQLite + 独立 Fake Provider + 灰色窗口崩溃恢复);P1+ 接入 durable 引擎与真实模型。

## 开发

```bash
python -m pip install -e ".[dev]"
python -m pytest -q          # 测试
python -m ruff check src tests
python -m mypy src           # strict
```

要求 Python ≥ 3.11。
