# OrderBy（Sort）算子完整流程详解

本文档在源码层面说明 Velox 中 `**OrderBy` 算子** 与 `**SortBuffer**`、`**RowContainer**`、`**PrefixSort**`、`**SortInputSpiller` / `SortOutputSpiller**`、`**SpillPartition` / `SpillMergeStream` / `TreeOfLosers**` 协作完成 **全量排序** 与 **可选磁盘 spill** 的完整路径，并强调 **行式（`RowContainer`）与列式（`RowVector`）** 及 **列序（计划输出 vs 容器/spill 物理列序）** 的对应关系。

与 `**velox/docs/develop/HashAggregationSpillDetailed.md**` 对照阅读时：聚合算子的行存承载的是 **group 状态 + accumulator**；OrderBy 的行存承载的是 **待排序的整行数据**（无聚合列），但 **spill 物化、序列化、多路归并** 的基础设施与 `SpillerBase` 体系相同。

相关源码主要位于：

- `velox/exec/OrderBy.{h,cpp}`
- `velox/exec/SortBuffer.{h,cpp}`
- `velox/exec/RowContainer.{h,cpp}`
- `velox/exec/PrefixSort.{h,cpp}`
- `velox/exec/Spiller.{h,cpp}`（`SortInputSpiller`、`SortOutputSpiller`、`SpillerBase`）
- `velox/exec/Spill.{h,cpp}`（`SpillPartition::createOrderedReader*`、`SpillMergeStream`）
- `velox/exec/OperatorUtils.cpp`（`gatherCopy`）
- `velox/exec/LocalPlanner.cpp`（计划节点到算子）
- `velox/core/PlanNode.h`（`OrderByNode`、`LocalMergeNode`、`MergeExchangeNode`）
- `velox/exec/Merge.{h,cpp}`（`Merge`、`LocalMerge`、`MergeExchange`、`SourceMerger`、`SourceStream`）

---

## 1. 计划层：`OrderByNode` 与执行算子名

- **计划节点**：`core::OrderByNode`（`velox/core/PlanNode.h`），`name()` 为 `"OrderBy"`。
- **执行算子**：`exec::OrderBy`（`velox/exec/OrderBy.*`），`Operator` 构造里 `operatorType` 为 `"OrderBy"`。
- **是否允许 spill**：`OrderByNode::canSpill` 依赖 `QueryConfig::orderBySpillEnabled()`（`velox/core/QueryConfig.h` 中 `kOrderBySpillEnabled`，默认 `true`）；算子是否带 `SpillConfig` 还取决于上层 **全局 spill 开关** 与 `DriverCtx::makeSpillConfig` 的拼装逻辑（与聚合一致，此处不展开）。

`**isPartial**`：见 **第 2 节「Partial OrderBy 与全局归并」**；单算子行为仍以 `**OrderBy` + `SortBuffer**` 为主（第 3 节起）。

---

## 2. Partial OrderBy 与全局归并（`LocalMerge` / `MergeExchange`）

本节说明：`**OrderByNode::isPartial() == true` 时，Velox 不会在 `OrderBy` 内做跨并行度 / 跨任务的归并**；归并由计划中的 `**LocalMergeNode**` 或 `**MergeExchangeNode**` 与执行层的 `**LocalMerge` / `MergeExchange**` 完成。`isPartial` 与 `**maxDrivers**`、**多 pipeline** 强相关。

### 2.1 语义与计划形状

- **Partial `OrderBy**`：每个 **独立数据源 / driver** 上运行的 `OrderBy` 只保证 **该源输出的行在排序键上非递减（或非递增，取决于 `SortOrder`）**，即 **多个有序子序列（sorted runs）**，**不要求**全局有序。
- **Final 归并**：必须把上述子序列 **k 路归并** 成单一全局有序流。计划上通常表现为：
  - **同进程、多 pipeline**：`LocalMergeNode`（多 `sources`）← 各 source 子树末端为 `**OrderBy(..., isPartial=true)**`，再 `**CallbackSink` → `MergeSource` 队列`** 喂给 **`LocalMerge`**。
  - **跨任务 / shuffle**：上游各 task 做 partial sort 后，下游 `**MergeExchangeNode`** 通过 `**MergeExchange**` 从远端拉 **已按相同键排序的页**，在 **单线程 `Merge`** 中做同样的 **k 路归并**。

**重要**：`isPartial` **只是节点上的布尔标记**；`**core` 层不会自动插入 `LocalMerge`**。协调器 / planner（或测试里的 `PlanBuilder`）必须显式添加 `localMerge(...)` 或 `mergeExchange(...)`，且 `**LocalMergeNode` / `MergeExchangeNode` 的 `sortingKeys` / `sortingOrders` 应与 partial `OrderBy` 一致**，否则归并语义与 SQL 不一致。

**示例（`velox/exec/tests/MergeTest.cpp` 中 `testLocalMerge`）**：为每个输入源建 `PlanBuilder().values(...).orderBy({key}, /*isPartial=*/true)`，再 `**PlanBuilder::localMerge({key}, std::move(sources))`** 得到单一 `LocalMergeNode`；`params.maxDrivers = numInputSources` 使 **每个 source 一条 driver pipeline**。

### 2.2 `maxDrivers`：partial 时允许多线程排序

`LocalPlanner::maxDrivers`（`velox/exec/LocalPlanner.cpp`）遍历 pipeline 上的 plan nodes：

```cpp
} else if (auto orderBy = std::dynamic_pointer_cast<const core::OrderByNode>(node)) {
  // final orderby must run single-threaded
  if (!orderBy->isPartial()) {
    return 1;
  }
}
```

- `**isPartial() == false`（final OrderBy）**：该 `DriverFactory` 的 `**maxDrivers` 被限制为 1**，全量排序在 **单 driver** 内完成（与 `SortBuffer` 收齐数据再排序模型一致）。
- `**isPartial() == true`**：**不**因 OrderBy 单独把 `maxDrivers` 压成 1；可与上游并行 scan / filter 等组合，使 **多个 pipeline 各自产出局部有序 batch**。

相对地，`**LocalMergeNode` / `MergeExchangeNode` / `MergeJoinNode`** 等 **始终** 把 `maxDriversForConsumer` 置为 **1**（归并算子单线程）。

### 2.3 本地归并：多 pipeline + `CallbackSink` + `LocalMerge`

#### 2.3.1 为何 `LocalMerge` 的 source 必须是独立 pipeline

`detail::mustStartNewPipeline(planNode, sourceId)`（`LocalPlanner.cpp`）：

- 对 `**LocalMergeNode`**：**任意 source** 都 `**return true`** —— **每个 source 子计划各自一条 pipeline**，不能与普通单源链混在同一条 `planNodes` 链上拼接。

#### 2.3.2 Consumer 侧：`LocalMerge` 算子

- **计划 → 算子**：`LocalPlanner` 在遇到 `LocalMergeNode` 时 `**std::make_unique<LocalMerge>(...)`**（`velox/exec/LocalPlanner.cpp`）。
- `**LocalMerge` 构造**（`velox/exec/Merge.cpp`）：`VELOX_CHECK_EQ(driverId, 0, "LocalMerge needs to run single-threaded")`；可选 spill：`LocalMergeNode::canSpill` → `QueryConfig::localMergeSpillEnabled()`；若配置了 spill executor，则读取 `**localMergeMaxNumMergeSources`** 作为 `**maxNumMergeSources_**`（控制 **每次并行拉起的 merge source 个数**，见下）。

#### 2.3.3 Producer 侧：`CallbackSink` 写入 `MergeSource`

`makeOperatorSupplier(LocalMergeNode)`（`LocalPlanner.cpp`）为 **每个 LocalMerge 的 source** 返回一个 **supplier**，其创建的算子是 `**CallbackSink`**：

1. `**ctx->task->addLocalMergeSource(splitGroupId, localMerge->id(), outputType, queueSize)**` 得到 `**MergeSource**`（队列大小来自 `**QueryConfig::localMergeSourceQueueSize()**`）。
2. `**consumerCb**`：`mergeSource->enqueue(std::move(input), future)`，把该 pipeline 上 **partial `OrderBy` 产出的 `RowVector`** 推入队列。
3. `**startCb**`：`mergeSource->started(future)`，与 merge 侧启动同步。

因此 **数据路径** 为：**各 pipeline：`… → OrderBy → CallbackSink` →（队列）→ `LocalMerge::MergeSource`**。

#### 2.3.4 归并算法：`Merge` → `SourceMerger` → `TreeOfLosers<SourceStream>`

**公共基类 `Merge`**（`velox/exec/Merge.h` / `Merge.cpp`）为 `**LocalMerge` 与 `MergeExchange` 共用**：

1. `**Merge` 构造**：将 `LocalMergeNode` / `MergeExchangeNode` 的 `**sortingKeys` / `sortingOrders`** 转成 `**std::vector<SpillSortKey> sortingKeys_**`（`channel` + `CompareFlags`，与 `OrderBy` 一致）。
2. `**addMergeSources**`：
  - `**LocalMerge::addMergeSources**`：从 `**Task::getLocalMergeSources(splitGroupId, planNodeId)**` 取回 **与本 `LocalMerge` 计划节点 id 对应的所有 `MergeSource` 共享指针**（与各 source pipeline 上 `addLocalMergeSource` 时注册的一致）。  
  - `**MergeExchange::addMergeSources`**：仅在 `**driverId == 0**` 的实例上通过 `**getSplitOrFuture**` 收集 `**RemoteConnectorSplit**`，再 `**MergeSource::createMergeExchangeSource(...)**` 为 **每个远端 task** 建一个 source（多实例 pipeline 时注释写明 **仅 pipeline 0 负责 merge**）。
3. `**maybeStartNextMergeSourceGroup`**：从 `**numStartedSources_**` 起，每次最多取 `**maxNumMergeSources_**` 个 `**MergeSource***`（默认 `**std::numeric_limits<uint32_t>::max()**`，即一次拉满；`**LocalMerge**` 在配置了 spill executor 时把 `**maxNumMergeSources_**` 设为 `**QueryConfig::localMergeMaxNumMergeSources()**`，从而 **分多轮** 归并）。当 `**sources_.size() > maxNumMergeSources_`** 时 `**Merge::needSpill()**` 为真，`**getOutputFromSource**` 可在输出 batch 上触发 `**MergeSpiller**`（见 §2.3.5）。
4. `**SourceMerger**`：内部 `**TreeOfLosers<SourceStream>**`；`**SourceStream::operator<**` 按 `**sortingKeys_**` 对 **当前行** 做 **逐键 `BaseVector::compare`**（与有序归并比较器语义一致）。
5. `**SourceMerger::getOutput**`：反复 `**merger_->next()**` 取当前全局最小流，`**setOutputRow` / `pop` / `copyToOutput**` 把 **各流当前 batch 中已选行** 拷到输出 `**RowVector`**（列式）；batch 边界与 `**SortBuffer::getOutputWithSpill**` 里对 `**isEndOfBatch**` 的处理类似 —— **先拷再 `pop` 拉下一批**。

**与 partial `OrderBy` 的衔接**：每个 `**OrderBy` 输出 batch 内部** 已有序；`**SourceStream`** 在 **单条 queue 流内** 仍按序消费；`**TreeOfLosers`** 在 **多条流之间** 做 **标准有序归并**，得到 **全局有序** 输出。

#### 2.3.5 `LocalMerge` 的 spill（与 `OrderBy` 路径对比）

若开启 `**local_merge_spill_enabled`** 且 `**Merge` 构造** 带 `**SpillConfig`**：

- `**Merge::getOutputFromSource**` 在 `**sourceMerger_->getOutput**` 得到 batch 后，若 `**needSpill()**` 可 `**Merge::spill()**` → `**MergeSpiller**`（`**NoRowContainerSpiller**`，`rowType` = **输出 `outputType_`**）直接把 **列式 `output_`** 写入 spill 文件（**无 `RowContainer` 中转**）。
- 一组 source 用完后 `**finishMergeSourceGroup`**：`**mergeOutputSpiller_->finishSpill**`；若发生过 spill，则 `**setupSpillMerger**` → `**SpillMerger**`：为 **每组 spill 文件** 建 `**ConcatFilesSpillBatchStream`** + `**MergeSource` 队列**，再套一层 `**SourceMerger`（仍是 `TreeOfLosers`）** 读回并归并。

这与 `**OrderBy` + `SortBuffer`** 的 **「行存 + `SortInputSpiller` + `SpillMergeStream`」** 是 **不同 spill 形态**（LocalMerge 更偏 **直接 spill 输出 batch**），但 **归并树与排序键比较** 思想一致。

### 2.4 `MergeExchange`：跨 task 的全局归并

- **计划节点**：`MergeExchangeNode`（`velox/core/PlanNode.h`），带 `**outputType`**、`**sortingKeys` / `sortingOrders**`、`**serdeKind**`。
- **执行算子**：`MergeExchange`（`velox/exec/Merge.cpp`），继承 `**Merge`**；构造时注册 **shuffle 用的 `VectorSerde*` / `serdeOptions*`**（压缩等来自 `QueryConfig::shuffleCompressionKind()` 等）。
- **数据来源**：`**MergeSource::createMergeExchangeSource`**，按 **远端 task id** 拉取 **已序列化页**，反序列化为 `**RowVector`** 后进入与 `**LocalMerge` 相同的 `SourceStream` / `SourceMerger` 路径**。

调度上 `**driverId != 0`** 的 `**MergeExchange::addMergeSources` 直接返回**，避免多实例重复拉 split；**仅 pipeline 0** 建全量 `**sources_`**。

### 2.5 小结表（partial 全链路）


| 阶段     | 计划                            | 执行                                          | 数据形态                                                   |
| ------ | ----------------------------- | ------------------------------------------- | ------------------------------------------------------ |
| 局部排序   | `OrderByNode(isPartial=true)` | 多 pipeline 上多个 `**OrderBy` + `SortBuffer**` | 列 → 行 → 排序指针 → 列式输出 batch（**每 batch 内键有序**）            |
| 送入归并   | `LocalMerge` 的 source 边       | `**CallbackSink` → `MergeSource::enqueue`** | 列式 `RowVector` 入队                                      |
| 全局归并   | `LocalMergeNode`              | `**LocalMerge`（driver 0 单线程）**              | 多路 `**SourceStream`** + `**TreeOfLosers**`，列式比较 + 列式输出 |
| 跨 task | `MergeExchangeNode`           | `**MergeExchange**`                         | 序列化页 ↔ 列式 batch，同上 `**SourceMerger**`                  |


---

## 3. 总体架构与数据形态

### 3.1 三层对象


| 组件                                                          | 作用                                                                                                                                                                                             |
| ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `**OrderBy**`                                               | 算子生命周期、`SpillConfig` 透传、`addInput` / `noMoreInput` / `getOutput` / `reclaim`；将 `OrderByNode` 的排序键表达式解析为 **列 channel** 与 `**CompareFlags`**；委托 `**SortBuffer**`。                                |
| `**SortBuffer**`                                            | 用 `**RowContainer**` 存全量输入；在 `**noMoreInput**` 时完成 **内存排序** 或 **收尾 spill + `finishSpill`**；在 `**getOutput**` 中按批产出 `**RowVector**`（纯内存 `**extractColumn**` 或 spill `**merge + gatherCopy**`）。  |
| `**SortInputSpiller` / `SortOutputSpiller` + `SpillState**` | 将 `**RowContainer` 行**（或已排序的 `**char*` 子序列**）批式物化为 `**RowVector`**，按 `SpillState` 写文件；读阶段由 `**SpillPartition::createOrderedReader**` 得到 `**TreeOfLosers<SpillMergeStream>**` 做 **全局有序 k 路归并**。 |


### 3.2 行式 vs 列式（全文核心）

1. **运行时累积态**：每行输入对应 `**RowContainer` 内一条紧凑行**（key 区放排序键列、dependent 区放其余列；变长数据走 `**HashStringAllocator`** 等）。这是 **行式、按行交错存储**。
2. **排序态**：不物理搬动行缓冲排序，而是维护 `**std::vector<char*> sortedRows_`**（或 spill 后为空），对 **行指针** 排序；`PrefixSort` 还会在临时前缀缓冲区里放 **归一化键 + 行地址**。
3. **写盘前**：`SpillerBase::extractSpill` 将一批 `**char*`** 投影为 `**RowVector**`（子列顺序 = `**spillerStoreType_**` = 容器逻辑列序），再走 **Presto VectorSerde** 等序列化路径写入 spill 文件（与聚合 spill 相同基础设施）。
4. **读盘后**：各 `**FileSpillMergeStream`** 反序列化为 `**RowVector` batch**（列布局与写盘时 **RowType** 一致）；`**SpillMergeStream::compare`** 使用构造 `SpillState` 时登记的 `**SpillSortKey**` 与 `**CompareFlags**`，保证 **跨文件的全序** 与写盘时 **每 run 内有序** 一致。
5. **算子输出**：`output_` 的类型为计划的 `**input_`/`outputType_`（列序与上游计划一致）**；从 **容器/spill 物理列序** 到 **计划列序** 的映射由 `**columnMap_`（`IdentityProjection` 列表）** 完成。

**一句话**：OrderBy 路径本质是 `**RowVector`（列）→ `RowContainer`（行）→ 排序后的 `char*` 序列 → `RowVector`（列）**；若 spill 则中间插入 `**RowVector` ↔ 磁盘页** 与 **merge 树**。

### 3.3 `OrderBy` 内：输入输出均为列存，中间态为行存

**结论**：算子从上游收到的 `**RowVectorPtr` 是标准列式 batch**。全量数据在 `**SortBuffer::data_`（`RowContainer`）** 中以 **行** 存储；排序比较读 **行内列槽** 或 **前缀缓冲**；输出时再把 **有序行子集** 写回 **列式 `RowVector`**。

**与 HashAggregation 的类比**：

- 聚合：`addRawInput` 是 **列向量行** → **group 行指针** 上的累加器。
- OrderBy：`store` 是 **列向量行** → **每个输入行新建一行**（无哈希探测、无累加器），随后只对 **行指针数组** 排序。

---

## 4. 列序锚点：`columnMap_` 与 `spillerStoreType_`

### 4.1 `RowContainer` 的物理列布局

`SortBuffer` 构造时（`velox/exec/SortBuffer.cpp`）：

- `**sortedColumnTypes`**：按 `**sortColumnIndices` 的顺序**，把排序键对应类型填入 `**RowContainer` 的 key 区**（`nullableKeys=true`，与通用「可空 key」容器一致）。
- `**nonSortedColumnTypes`**：按 **原始 `input_` 列下标递增** 扫描，跳过已作为排序键的列，依次追加到 **dependent 区**。
- `**useListRowIndex=true`**：维护 `**rowPointers_**`，使 `listRows` / spill 列举时 **不必扫描整块分配与 free 位图**，与 `SortInputSpiller` 等路径对齐（见 `RowContainer` 头文件注释）。

### 4.2 `IdentityProjection` 列表 `columnMap_`

`IdentityProjection` 定义（`velox/exec/Operator.h`）：`**inputChannel`** = 数据在 **目标布局** 中的列下标；`**outputChannel`** = 数据在 **计划 `RowVector`（输入/输出）** 中的列下标。

`SortBuffer` 中：

- 对每个排序键 `i`：`IdentityProjection(i, sortColumnIndices[i])`  
→ `**RowContainer` key 第 `i` 列** 对应 **输入 `RowVector` 的第 `sortColumnIndices[i]` 子列**。
- 对每个非排序列：`(nonSortedIndex++, 原始列下标 i)`  
→ `**RowContainer` dependent 列** 对应 **输入同名列**。

因此：

- `**addInput`**：`DecodedVector` 从 `**inputRow->childAt(outputChannel)**` 解码，`store` 写入 `**inputChannel**` 槽位。
- `**getOutputWithoutSpill**`：`extractColumn` 从 `**inputChannel**` 读到 `**output_->childAt(outputChannel)**`。
- `**getOutputWithSpill` 的 `gatherCopy**`：spill batch 子列下标与 **容器列下标** 一致，同样用 `**columnMap_`**：向 `**outputChannel**` 子列拷贝，源为 `**inputChannel**` 子列。

### 4.3 `spillerStoreType_`

`ROW(sortedSpillColumnNames, sortedSpillColumnTypes)`：**列名/类型顺序** 与 `**RowContainer::columnTypes()` 遍历顺序一致** —— **先全部排序键列（与 `sortColumnIndices` 顺序一致），再其余列（按原始表列下标递增）**。

**重要**：磁盘上每个 spill batch 的 **子列顺序** 绑定 `**spillerStoreType_` / 容器逻辑列序**，**不**绑定 `OrderByNode` 里 **排序键表达式在 SQL 中的书写顺序以外的「输出列展示顺序」** —— 输出列序始终由 `**input_` RowType** 决定，物理重排只发生在 **容器与 spill 文件** 内。

---

## 5. 算子壳：`OrderBy` 与 `SortBuffer` 的绑定

本节对应 `**velox/exec/OrderBy.{h,cpp}`**：`OrderBy` 只做 **计划字段解析、Operator 基类初始化、委托 `SortBuffer`**；**不**直接操作 `RowContainer` / spill 细节（见第 6 节起）。

### 5.1 构造：基类 `Operator`、排序键解析、`SortBuffer` 参数

**初始化列表**先调用 `**Operator`** 构造（`velox/exec/Operator.cpp`），传入 `**outputType_**` = `orderByNode->outputType()`（与计划一致）、`**planNodeId**`、`**operatorType**` = `"OrderBy"`，以及 **可选 `SpillConfig`**：

- `**spillConfig_**` 仅当 `**orderByNode->canSpill(driverCtx->queryConfig())**` 为真时由 `**driverCtx->makeSpillConfig(operatorId)**` 生成，否则为 `**std::nullopt**`。`canSpill` 在计划层由 `**OrderByNode::canSpill**` 绑定 `**orderBySpillEnabled()**`（见第 1 节与第 13 节）。
- 因此 `**Operator::canSpill()**` 与 `**canReclaim()**`（默认 `**canReclaim() == canSpill()**`，见 `velox/exec/Operator.h`）在 **未开启 order-by spill** 时为 **false**，`reclaim` 不会被仲裁当作可回收对象（除非上层另有策略）。

**函数体**顺序（`velox/exec/OrderBy.cpp`）：

```33:71:velox/exec/OrderBy.cpp
OrderBy::OrderBy(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::OrderByNode>& orderByNode)
    : Operator(
          driverCtx,
          orderByNode->outputType(),
          operatorId,
          orderByNode->id(),
          "OrderBy",
          orderByNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId)
              : std::nullopt) {
  maxOutputRows_ = outputBatchRows(std::nullopt);
  VELOX_CHECK(pool()->trackUsage());
  std::vector<column_index_t> sortColumnIndices;
  std::vector<CompareFlags> sortCompareFlags;
  sortColumnIndices.reserve(orderByNode->sortingKeys().size());
  sortCompareFlags.reserve(orderByNode->sortingKeys().size());
  for (int i = 0; i < orderByNode->sortingKeys().size(); ++i) {
    const auto channel =
        exprToChannel(orderByNode->sortingKeys()[i].get(), outputType_);
    VELOX_CHECK(
        channel != kConstantChannel,
        "OrderBy doesn't allow constant sorting keys");
    sortColumnIndices.push_back(channel);
    sortCompareFlags.push_back(
        fromSortOrderToCompareFlags(orderByNode->sortingOrders()[i]));
  }
  sortBuffer_ = std::make_unique<SortBuffer>(
      outputType_,
      sortColumnIndices,
      sortCompareFlags,
      pool(),
      &nonReclaimableSection_,
      driverCtx->prefixSortConfig(),
      spillConfig_.has_value() ? &(spillConfig_.value()) : nullptr,
      spillStats_.get());
}
```

要点说明：

1. `**maxOutputRows_ = outputBatchRows(std::nullopt)**`
  构造阶段尚无行宽估计，走 `**Operator::outputBatchRows**`（`velox/exec/Operator.cpp`）：无 `**averageRowSize**` 时直接返回 `**QueryConfig::preferredOutputBatchRows()**`。在 `**noMoreInput**` 之后会第二次赋值（见 §5.2）。
2. `**VELOX_CHECK(pool()->trackUsage())**`
  要求算子 **leaf pool** 开启用量跟踪，便于内存仲裁与统计。
3. `**exprToChannel(sortingKeys()[i].get(), outputType_)`**（声明在 `velox/exec/Operator.h`）
  将 `**FieldAccessTypedExpr**` 等解析为 **输出 schema 上的列下标**；`**kConstantChannel`** 表示常量排序键，**显式禁止**。
4. `**fromSortOrderToCompareFlags`**（`OrderBy.cpp` 匿名命名空间）生成 `**CompareFlags**`：`nullsFirst`、`ascending`、`**equalsOnly = false**`、`**NullHandlingMode::kNullAsValue**`（与 `CompareFlags` 注释中「null 当值、顺序由 nullsFirst 决定」一致，`velox/common/base/CompareFlags.h`）。
5. `**SortBuffer**` 收到 `**&nonReclaimableSection_**`：与 `**memory::ReclaimableSectionGuard**` 配合，在 `**SortBuffer::ensureInputFits**` 等路径 `**maybeReserve**` 时标记 **可仲裁段**（见第 6 节）。`**prefixSortConfig`** 来自 `**DriverCtx**`，透传给 `**PrefixSort::sort` / `maxRequiredBytes**`。`**spillStats_**` 为 `**Operator**` 成员，与 `**SpillerBase**` 共享计数。

### 5.2 `addInput` / `noMoreInput` / `getOutput` / `reclaim` / `close`

与源码一一对应的行为如下。

`**needsInput()**`（`OrderBy.h`）：`**return !finished_**`。一旦 `**getOutput**` 取完最后一批（`nullptr`）置 `**finished_**`，上游不再 `**addInput**`。

`**addInput**`：

```73:76:velox/exec/OrderBy.cpp
void OrderBy::addInput(RowVectorPtr input) {
  loadLazyReclaimable(input);
  sortBuffer_->addInput(input);
}
```

- `**loadLazyReclaimable**`（`Operator::loadLazyReclaimable`，`Operator.cpp`）：在 `**ReclaimableSectionGuard(this)**` 下调用 `**vector->loadedVector()**`，保证 **Lazy 子列** 在写入 `**RowContainer`** 前已物化，且该加载段对 **reclaimer** 可识别。

`**noMoreInput`**：

```92:96:velox/exec/OrderBy.cpp
void OrderBy::noMoreInput() {
  Operator::noMoreInput();
  sortBuffer_->noMoreInput();
  maxOutputRows_ = outputBatchRows(sortBuffer_->estimateOutputRowSize());
}
```

- `**Operator::noMoreInput()**`（`Operator.h`）：置 `**noMoreInput_ = true**`，供 `**getOutput**` 与基类其它逻辑使用。
- `**sortBuffer_->noMoreInput()**`：触发 **排序或 spill 收尾**（第 7 节）。
- `**maxOutputRows_`**：调用 `**Operator::outputBatchRows(sortBuffer_->estimateOutputRowSize())**`（`velox/exec/Operator.cpp`）。`**estimateOutputRowSize()**` 来自 `**SortBuffer::estimateOutputRowSize()**`，即 `**SortBuffer` 内维护的 `estimatedOutputRowSize_**`（由 `**updateEstimatedOutputRowSize**` 在 `**addInput` / spill 路径** 更新）；可能为 `**std::nullopt`**（尚无估计）或 `**0**`（`RowContainer::estimateRowSize` 无值或为 0）。

```312:329:velox/exec/Operator.cpp
vector_size_t Operator::outputBatchRows(
    std::optional<uint64_t> averageRowSize) const {
  const auto& queryConfig = operatorCtx_->task()->queryCtx()->queryConfig();
  if (!averageRowSize.has_value()) {
    return queryConfig.preferredOutputBatchRows();
  }

  if (averageRowSize.value() == 0) {
    return queryConfig.maxOutputBatchRows();
  }

  const uint64_t batchSize =
      queryConfig.preferredOutputBatchBytes() / averageRowSize.value();
  if (batchSize > queryConfig.maxOutputBatchRows()) {
    return queryConfig.maxOutputBatchRows();
  }
  return std::max<vector_size_t>(batchSize, 1);
}
```

`**QueryConfig` 中的三项及默认值**（`velox/core/QueryConfig.h`；可通过 **session 配置键** 覆盖）：


| 访问函数                              | Session 键（`QueryConfig::k*`）         | 默认值（未配置时 `get(..., default)`） |
| --------------------------------- | ------------------------------------ | ----------------------------- |
| `**preferredOutputBatchRows()`**  | `**"preferred_output_batch_rows"**`  | **1024**                      |
| `**preferredOutputBatchBytes()`** | `**"preferred_output_batch_bytes"**` | **10 MiB**（`10UL << 20`）      |
| `**maxOutputBatchRows()`**        | `**"max_output_batch_rows"**`        | **10000**                     |


**分支语义（与 `OrderBy` 两条路径对应）**：

1. `**OrderBy` 构造**：`**maxOutputRows_ = outputBatchRows(std::nullopt)`** → 恒为 `**preferredOutputBatchRows()**`（默认 **1024**），此时 **尚无行宽估计**。
2. `**noMoreInput` 之后**：若 `**estimateOutputRowSize()`** 为 `**nullopt**`，同上；若有值且 **> 0**：`**batchSize = floor(preferredOutputBatchBytes / rowSize)`**，再 `**min(batchSize, maxOutputBatchRows)**`，且 **至少 1 行**；若估计为 **0**：直接 `**maxOutputBatchRows()`**（默认 **10000**）。

因此：**控制「按字节估算批大小」的是 `preferred_output_batch_bytes` 与 `max_output_batch_rows`；无估计时只看 `preferred_output_batch_rows`**。

`**getOutput**`：

```98:106:velox/exec/OrderBy.cpp
RowVectorPtr OrderBy::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  RowVectorPtr output = sortBuffer_->getOutput(maxOutputRows_);
  finished_ = (output == nullptr);
  return output;
}
```

- **在 `noMoreInput_` 之前** 始终 `**nullptr`**（blocking 算子：先收齐再出）。
- `**finished_**` 仅在 `**getOutput` 返回 `nullptr**` 时置真（含 **无输入行**、**已吐完** 两种情况）。

`**reclaim`**：

```78:90:velox/exec/OrderBy.cpp
void OrderBy::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK(canReclaim());
  VELOX_CHECK(!nonReclaimableSection_);

  // TODO: support fine-grain disk spilling based on 'targetBytes' after
  // having row container memory compaction support later.
  sortBuffer_->spill();

  // Release the minimum reserved memory.
  pool()->release();
}
```

- `**targetBytes` / `stats**` 当前 **未传入 `SortBuffer`**，与注释 TODO 一致；实际回收量由 `**sortBuffer_->spill()**` + `**pool()->release()**` 决定。

`**close**`：

```108:111:velox/exec/OrderBy.cpp
void OrderBy::close() {
  Operator::close();
  sortBuffer_.reset();
}
```

- `**Operator::close**` 会 `**recordSpillStats**`、`**pool()->release()**` 等；再 `**reset**` `**sortBuffer_**` 释放排序状态。


| 方法            | 源码要点                                                                                                        |
| ------------- | ----------------------------------------------------------------------------------------------------------- |
| `addInput`    | `**loadLazyReclaimable**` → `**sortBuffer_->addInput**`                                                     |
| `noMoreInput` | `**Operator::noMoreInput**` → `**sortBuffer_->noMoreInput**` → `**outputBatchRows(estimateOutputRowSize)**` |
| `getOutput`   | `**!noMoreInput_                                                                                            |
| `reclaim`     | `**canReclaim()**` 且非 `**nonReclaimableSection_**` → `**sortBuffer_->spill()**` → `**pool()->release()**`   |
| `close`       | `**Operator::close()**` → `**sortBuffer_.reset()**`                                                         |


### 5.3 本地计划挂载

`LocalPlanner` 在展开 pipeline 时，对 `**OrderByNode**` 实例化 `**OrderBy**`（与其它算子同一分支风格）：

```629:633:velox/exec/LocalPlanner.cpp
    } else if (
        auto orderByNode =
            std::dynamic_pointer_cast<const core::OrderByNode>(planNode)) {
      operators.push_back(
          std::make_unique<OrderBy>(id, ctx.get(), orderByNode));
```

- `**id**`：本 pipeline 内递增的 **operator id**；`**ctx`**：当前 `**DriverCtx**`（含 `**queryConfig**`、`**prefixSortConfig**`、`**task**` 等）。`**isPartial()**` 不改变此处类型，仍构造同一 `**OrderBy**` 类；并行度由 `**maxDrivers**` 逻辑区分（第 2 节）。

---

## 6. 输入阶段：`SortBuffer::addInput` 与 `ensureInputFits`

实现文件：`**velox/exec/SortBuffer.cpp**`；行存写入：`**velox/exec/RowContainer.cpp**`（`RowContainer::store`、`newRow`）。

### 6.1 `addInput`：源码结构与数据流

完整函数如下（含测试挂钩）：

```86:108:velox/exec/SortBuffer.cpp
void SortBuffer::addInput(const VectorPtr& input) {
  velox::common::testutil::TestValue::adjust(
      "facebook::velox::exec::SortBuffer::addInput", this);

  VELOX_CHECK(!noMoreInput_);
  ensureInputFits(input);

  const SelectivityVector allRows(input->size());
  std::vector<char*> rows(input->size());
  for (int row = 0; row < input->size(); ++row) {
    rows[row] = data_->newRow();
  }
  const auto* inputRow = input->as<RowVector>();
  for (const auto& columnProjection : columnMap_) {
    DecodedVector decoded(
        *inputRow->childAt(columnProjection.outputChannel), allRows);
    data_->store(
        decoded,
        folly::Range(rows.data(), input->size()),
        columnProjection.inputChannel);
  }
  numInputRows_ += allRows.size();
}
```

逐步说明：

1. `**TestValue::adjust**`：仅测试/注入点，生产路径可忽略。
2. `**VELOX_CHECK(!noMoreInput_)**`：与 `**noMoreInput**` 之后 `**SortBuffer**` 状态机一致；`**noMoreInput_` 为真后不得再 `addInput**`。
3. `**ensureInputFits(input)**`（§6.2）：在 **本 batch 写入 `RowContainer` 之前** 做 **预留 / 可选 spill**；`**spillConfig_ == nullptr`** 时整个函数为空操作，**不会** `spill()`。
4. `**SelectivityVector allRows(input->size())`**：默认构造等价于 **选中 `[0, size)` 全部行**，供 `**DecodedVector`** 对整 batch 解码。
5. `**newRow` 循环**：对输入 `**input->size()`** 中每一逻辑行调用 `**data_->newRow()**`，得到 `**rows[i]**` 指向 `**RowContainer**` 内一条 **已初始化** 的行缓冲。`SortBuffer` 构造时 `**useListRowIndex=true`**，新行会进入 `**rowPointers_**` 等快速枚举结构（见 `**RowContainer**` 头文件注释）。
6. **按列 `store`**：
  - `**input**` 必须为 `**RowVector**`（`**as<RowVector>()**`）；  
  - 对每个 `**IdentityProjection`（`columnMap_`）**：从 `**childAt(outputChannel)`** 取 **计划列序** 上的子列，`**DecodedVector`** 解开 dictionary/constant 等包装；  
  - `**store(decoded, Range(rows), inputChannel)**` 把 **同一批 `rows` 在容器列 `inputChannel`** 上填满 —— 即 **「列向量的一段连续逻辑行」→「多行行缓冲的同一列槽位」**（列存 → 行存）。  
   **列循环、行固定**：每列一次批量写，避免按行扫描所有类型。

`**RowContainer::store`（批量，三参数）** 核心分支：

```547:575:velox/exec/RowContainer.cpp
void RowContainer::store(
    const DecodedVector& decoded,
    folly::Range<char**> rows,
    int32_t column) {
  VELOX_CHECK_GE(decoded.size(), rows.size());
  const bool isKey = column < keyTypes_.size();
  if ((isKey && !nullableKeys_) || !decoded.mayHaveNulls()) {
    VELOX_DYNAMIC_TYPE_DISPATCH(
        storeNoNullsBatch,
        typeKinds_[column],
        decoded,
        rows,
        isKey,
        offsets_[column],
        column);
  } else {
    const auto& rowColumn = rowColumns_[column];
    VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
        storeWithNullsBatch,
        typeKinds_[column],
        decoded,
        rows,
        isKey,
        rowColumn.offset(),
        rowColumn.nullByte(),
        rowColumn.nullMask(),
        column);
  }
}
```

- **SortBuffer** 使用的 `**RowContainer(sortedColumnTypes, nonSortedColumnTypes, /*useListRowIndex=*/true, pool)`** 在内部转发里 `**nullableKeys_ == true**`（`velox/exec/RowContainer.h` 该构造函数注释为 `**true, // nullableKeys**`）。批量 `**store**` 的首分支条件为 `**(isKey && !nullableKeys_) || !decoded.mayHaveNulls()**`（`RowContainer.cpp`）：对 **key 列** 因 `**!nullableKeys_` 为假**，是否走 `**storeNoNullsBatch`** 完全取决于 `**!decoded.mayHaveNulls()**`；若 `**decoded.mayHaveNulls()**` 为真则走 `**storeWithNullsBatch**`。  
- `**VELOX_DYNAMIC_TYPE_DISPATCH(_ALL)**`：按 `**typeKinds_[column]**` 静态分派到各类型的 `**store*Batch**`；变长类型（VARCHAR、ARRAY 等）通过 `**ContainerRowSerde**` 写入 `**HashStringAllocator**`，行内保留 `**StringView` / `string_view**` 等引用。  
- SortBuffer 使用上述 **三参数批量 `store`**，与聚合等路径中 **按列批量写入 `RowContainer`** 的用法一致。

### 6.2 `ensureInputFits`：条件分支与 `spill()` 入口

#### 6.2.1 `ensureInputFits` 全文逻辑

```204:263:velox/exec/SortBuffer.cpp
void SortBuffer::ensureInputFits(const VectorPtr& input) {
  // Check if spilling is enabled or not.
  if (spillConfig_ == nullptr) {
    return;
  }

  const int64_t numRows = data_->numRows();
  if (numRows == 0) {
    // 'data_' is empty. Nothing to spill.
    return;
  }

  auto [freeRows, outOfLineFreeBytes] = data_->freeSpace();
  const auto outOfLineBytes =
      data_->stringAllocator().retainedSize() - outOfLineFreeBytes;
  const int64_t flatInputBytes = input->estimateFlatSize();

  // Test-only spill path.
  if (numRows > 0 && testingTriggerSpill(pool_->name())) {
    spill();
    return;
  }

  const auto currentMemoryUsage = pool_->usedBytes();
  const auto minReservationBytes =
      currentMemoryUsage * spillConfig_->minSpillableReservationPct / 100;
  const auto availableReservationBytes = pool_->availableReservation();
  const int64_t estimatedIncrementalBytes =
      data_->sizeIncrement(input->size(), outOfLineBytes ? flatInputBytes : 0);
  if (availableReservationBytes > minReservationBytes) {
    // If we have enough free rows for input rows and enough variable length
    // free space for the vector's flat size, no need for spilling.
    if (freeRows > input->size() &&
        (outOfLineBytes == 0 || outOfLineFreeBytes >= flatInputBytes)) {
      return;
    }

    // If the current available reservation in memory pool is 2X the
    // estimatedIncrementalBytes, no need to spill.
    if (availableReservationBytes > 2 * estimatedIncrementalBytes) {
      return;
    }
  }

  // Try reserving targetIncrementBytes more in memory pool, if succeed, no
  // need to spill.
  const auto targetIncrementBytes = std::max<int64_t>(
      estimatedIncrementalBytes * 2,
      currentMemoryUsage * spillConfig_->spillableReservationGrowthPct / 100);
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_->maybeReserve(targetIncrementBytes)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve " << succinctBytes(targetIncrementBytes)
               << " for memory pool " << pool()->name()
               << ", usage: " << succinctBytes(pool()->usedBytes())
               << ", reservation: " << succinctBytes(pool()->reservedBytes());
}
```

语义按执行顺序归纳：


| 步骤  | 条件                                                                                                                                                                                                                                                                                                       | 行为                                                                                                                                                                                                               |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A   | `**spillConfig_ == nullptr**`                                                                                                                                                                                                                                                                            | 立即 `**return**`（无 spill、不预留）。                                                                                                                                                                                    |
| B   | `**data_->numRows() == 0**`                                                                                                                                                                                                                                                                              | `**return**`（容器无已占用行，本 batch 写入不依赖「腾挪旧行」）。                                                                                                                                                                       |
| C   | 否则                                                                                                                                                                                                                                                                                                       | 取 `**freeSpace()**` → `**freeRows**`、`**outOfLineFreeBytes**`；`**outOfLineBytes**` = 已占用变长总量 − 空闲变长；`**flatInputBytes**` = `**input->estimateFlatSize()**`。                                                      |
| D   | `**testingTriggerSpill(pool_->name())**`                                                                                                                                                                                                                                                                 | **直接 `spill()`** 并 `**return**`（测试路径）。                                                                                                                                                                           |
| E   | 计算 `**minReservationBytes**` = `**usedBytes * minSpillableReservationPct / 100**`；`**estimatedIncrementalBytes**` = `**data_->sizeIncrement(input->size(), outOfLineBytes ? flatInputBytes : 0)**` —— 若当前容器 **无已占用变长**（`**outOfLineBytes == 0`**），第二参数为 **0**，不把本 batch 的 flat 估算并入增量（与注释「有足够变长空间」分支配合）。 |                                                                                                                                                                                                                  |
| F   | `**availableReservationBytes > minReservationBytes`** 时                                                                                                                                                                                                                                                  | 若 `**freeRows > input->size()**` 且（**无变长** 或 `**outOfLineFreeBytes >= flatInputBytes`**）→ `**return**`；否则若 `**availableReservationBytes > 2 * estimatedIncrementalBytes**` → `**return**`（认为 pool 仍够大，先不 spill）。 |
| G   | 否则计算 `**targetIncrementBytes**` = `**max(estimatedIncrementalBytes * 2, usedBytes * spillableReservationGrowthPct / 100)**`；在 `**ReclaimableSectionGuard(nonReclaimableSection_)**` 下 `**pool_->maybeReserve(targetIncrementBytes)**`，成功则 `**return**`。                                                  |                                                                                                                                                                                                                  |
| H   | `**maybeReserve` 失败**                                                                                                                                                                                                                                                                                    | 仅 `**LOG(WARNING)`**，**不在本函数内调用 `spill()`**。                                                                                                                                                                     |


因此：**输入路径上「主动」spill 主要来自测试注入 `testingTriggerSpill`**；`**maybeReserve` 失败**只打日志，依赖 **内存仲裁调用 `Operator::reclaim` → `OrderBy::reclaim` → `SortBuffer::spill()`** 等从外向内释放（见第 5 节 `**reclaim**`、第 11 节 ASCII）。这与 `**GroupingSet::ensureInputFits**` 一类「失败即 spill」的写法 **不同**，阅读聚合 spill 文档时勿混淆。

#### 6.2.2 `SortBuffer::spill()` 入口（与 `addInput` 的衔接）

```183:197:velox/exec/SortBuffer.cpp
void SortBuffer::spill() {
  VELOX_CHECK_NOT_NULL(
      spillConfig_, "spill config is null when SortBuffer spill is called");

  // Check if sort buffer is empty or not, and skip spill if it is empty.
  if (data_->numRows() == 0) {
    return;
  }
  updateEstimatedOutputRowSize();

  if (sortedRows_.empty()) {
    spillInput();
  } else {
    spillOutput();
  }
}
```

- `**ensureInputFits` 内**若触发测试 spill，此时 `**sortedRows_` 仍为空**（排序尚未发生），走 `**spillInput()`** → `**SortInputSpiller**`（第 8 节）。  
- **输出阶段**若已 `**listRows` + `PrefixSort::sort`** 填过 `**sortedRows_**`，再 `**spill()**` 则走 `**spillOutput()**`（第 9 节）。

---

## 7. 输入结束：`SortBuffer::noMoreInput`

源码：`**velox/exec/SortBuffer.cpp**`；排序入口：`**velox/exec/PrefixSort.{h,cpp}**`。

### 7.1 `noMoreInput`：完整源码与分支语义

```110:149:velox/exec/SortBuffer.cpp
void SortBuffer::noMoreInput() {
  velox::common::testutil::TestValue::adjust(
      "facebook::velox::exec::SortBuffer::noMoreInput", this);
  VELOX_CHECK(!noMoreInput_);
  VELOX_CHECK_NULL(outputSpiller_);

  // It may trigger spill, make sure it's triggered before noMoreInput_ is set.
  ensureSortFits();

  noMoreInput_ = true;

  // No data.
  if (numInputRows_ == 0) {
    return;
  }

  if (inputSpiller_ == nullptr) {
    VELOX_CHECK_EQ(numInputRows_, data_->numRows());
    updateEstimatedOutputRowSize();
    // Sort the pointers to the rows in RowContainer (data_) instead of sorting
    // the rows.
    // TODO: Reuse 'RowContainer::rowPointers_'.
    sortedRows_.resize(numInputRows_);
    RowContainerIterator iter;
    data_->listRows(&iter, numInputRows_, sortedRows_.data());
    PrefixSort::sort(
        data_.get(), sortCompareFlags_, prefixSortConfig_, pool_, sortedRows_);
  } else {
    // Spill the remaining in-memory state to disk if spilling has been
    // triggered on this sort buffer. This is to simplify query OOM prevention
    // when producing output as we don't support to spill during that stage as
    // for now.
    spill();

    finishSpill();
  }

  // Releases the unused memory reservation after procesing input.
  pool_->release();
}
```


| 顺序  | 语句                                     | 作用                                                                                                                                                           |
| --- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1   | `**VELOX_CHECK(!noMoreInput_)**`       | 禁止重复 `**noMoreInput**`。                                                                                                                                      |
| 2   | `**VELOX_CHECK_NULL(outputSpiller_)**` | **输出阶段 tail spill** 与 **输入结束收尾** 互斥：若已建 `**outputSpiller_`**，不应再进入该 `**noMoreInput**` 主路径（避免状态机冲突）。                                                          |
| 3   | `**ensureSortFits()**`（§7.3）           | 在置 `**noMoreInput_**` 之前，为 **排序指针数组 + PrefixSort 临时区** 尝试 `**maybeReserve`**；测试可 `**spill()**`。                                                              |
| 4   | `**noMoreInput_ = true**`              | 此后 `**addInput**` 非法（见第 6 节）；`**getOutput**` 可工作。                                                                                                            |
| 5   | `**numInputRows_ == 0**`               | 无行则 **不做排序 / spill 收尾**，直接 `**return`**（与 `**getOutput**` 恒 `nullptr` 一致）。                                                                                   |
| 6a  | `**inputSpiller_ == nullptr**`         | **纯内存路径**：断言计数一致 → 更新行宽估计 → `**listRows` 填满 `sortedRows_`** → `**PrefixSort::sort**`（§7.2）。                                                                  |
| 6b  | `**else**`                             | **曾发生输入 spill**：再 `**spill()`**（`spillInput` 内 `**data_->clear()**` 等，见第 8 节）刷掉剩余行，再 `**finishSpill**` 把 `**SpillState**` 收进 `**spillPartitionSet_**`（§8.5）。 |
| 7   | `**pool_->release()**`                 | 释放 **本轮未使用** 的 pool 预留（与 `**ensureSortFits` / `maybeReserve`** 对称）。                                                                                          |


**注释语义**：若 `**inputSpiller_` 已存在**，说明 **输出阶段不能再依赖「边 `getOutput` 边 spill」**；因此在 `**noMoreInput`** 时 **强制把剩余 `data_` 写盘** 并 `**finishSpill`**，后续 **只通过 merge 读盘输出**（第 10 节）。

### 7.2 `PrefixSort::sort`：`SortBuffer` 内存排序路径

**调用点**（上节源码 **L135–L136**）：`**PrefixSort::sort(data_.get(), sortCompareFlags_, prefixSortConfig_, pool_, sortedRows_)`** —— `**sortedRows_**` 为 `**std::vector<char*, StlAllocator<char*>>**`，元素为 `**RowContainer**` 行指针；`**pool_**` 为本算子 **leaf pool**（与 **前缀缓冲分配** 同源）。

**静态入口**（`velox/exec/PrefixSort.h`）：

```130:151:velox/exec/PrefixSort.h
  FOLLY_ALWAYS_INLINE static void sort(
      const RowContainer* rowContainer,
      const std::vector<CompareFlags>& compareFlags,
      const velox::common::PrefixSortConfig& config,
      memory::MemoryPool* pool,
      std::vector<char*, memory::StlAllocator<char*>>& rows) {
    if (rowContainer->numRows() < config.minNumRows) {
      stdSort(rows, rowContainer, compareFlags);
      return;
    }
    const auto sortLayout =
        generateSortLayout(rowContainer, compareFlags, config);
    // All keys can not normalize, skip the binary string compare opt.
    // Putting this outside sort-internal helps with stdSort.
    if (!sortLayout.hasNormalizedKeys) {
      stdSort(rows, rowContainer, compareFlags);
      return;
    }

    PrefixSort prefixSort(rowContainer, sortLayout, pool);
    prefixSort.sortInternal(rows);
  }
```

**分支 A — `stdSort`（`PrefixSort.cpp`，非 `gfx::timsort`）**：

```332:346:velox/exec/PrefixSort.cpp
void PrefixSort::stdSort(
    std::vector<char*, memory::StlAllocator<char*>>& rows,
    const RowContainer* rowContainer,
    const std::vector<CompareFlags>& compareFlags) {
  std::sort(
      rows.begin(), rows.end(), [&](const char* leftRow, const char* rightRow) {
        for (auto i = 0; i < compareFlags.size(); ++i) {
          if (auto result = rowContainer->compare(
                  leftRow, rightRow, i, compareFlags[i])) {
            return result < 0;
          }
        }
        return false;
      });
}
```

触发条件：`**numRows < config.minNumRows**`，或 `**generateSortLayout` 后 `!hasNormalizedKeys**`。比较器按 **键下标 `0..compareFlags.size()-1`** 调 `**RowContainer::compare**`，与 `**SortBuffer**` 里 `**RowContainer` key 列顺序**（排序键前缀）一致。

**分支 B — `sortInternal`（前缀排序）** 概要（`velox/exec/PrefixSort.cpp`）：

```360:408:velox/exec/PrefixSort.cpp
void PrefixSort::sortInternal(
    std::vector<char*, memory::StlAllocator<char*>>& rows) {
  const auto numRows = rows.size();
  const auto entrySize = sortLayout_.entrySize;
  memory::ContiguousAllocation prefixBufferAlloc;
  // Allocates prefix sort buffer.
  {
    const auto numPages =
        memory::AllocationTraits::numPages(numRows * entrySize);
    pool_->allocateContiguous(numPages, prefixBufferAlloc);
  }
  char* prefixBuffer = prefixBufferAlloc.data<char>();

  // Extracts rows, and stores the serialized normalized keys plus the row
  // address (in row container) to prefix sort buffer.
  for (auto i = 0; i < rows.size(); ++i) {
    extractRowAndEncodePrefixKeys(rows[i], prefixBuffer + entrySize * i);
  }

  // Sort rows with the normalized prefix keys.
  {
    const auto swapBuffer = AlignedBuffer::allocate<char>(entrySize, pool_);
    PrefixSortRunner sortRunner(entrySize, swapBuffer->asMutable<char>());
    auto* prefixBufferStart = prefixBuffer;
    auto* prefixBufferEnd = prefixBuffer + numRows * entrySize;
    ...
    if (sortLayout_.hasNonNormalizedKey ||
        sortLayout_.nonPrefixSortStartIndex < sortLayout_.numNormalizedKeys) {
      sortRunner.quickSort(
          prefixBufferStart, prefixBufferEnd, [&](char* lhs, char* rhs) {
            return comparePartNormalizedKeys(lhs, rhs);
          });
    } else {
      sortRunner.quickSort(
          prefixBufferStart, prefixBufferEnd, [&](char* lhs, char* rhs) {
            return compareAllNormalizedKeys(lhs, rhs);
          });
    }
  }

  // Output sorted row addresses.
  for (auto i = 0; i < rows.size(); ++i) {
    rows[i] = getRowAddrFromPrefixBuffer(prefixBuffer + i * entrySize);
  }
}
```

- `**allocateContiguous**`：为 **每行一条长度为 `entrySize` 的前缀记录** 分配连续内存；`**extractRowAndEncodePrefixKeys`** 写入 **归一化键 +（必要时）非归一化指针 + 原行地址**。  
- `**PrefixSortRunner::quickSort`**：在 **前缀数组** 上排序；`**comparePartNormalizedKeys`** 在前缀相等或部分键未编码进前缀时 **回退 `RowContainer::compare`**（与头文件注释一致）。  
- **末循环**：把 `**rows[i]`** 恢复为 **排序后的 `RowContainer` 行指针**（**物理行缓冲未移动**，仅重排指针）。

`**PrefixSort::maxRequiredBytes`**（`ensureSortFits` 使用）：与 `**sortInternal**` 同构估算 **前缀页 + swap buffer**（`pageBytes(numPages) + preferredSize(entrySize+padding) + 2*alignment`），见 `**PrefixSort.cpp` `maxRequiredBytes()`**。

### 7.3 `ensureSortFits`：排序前内存预留

```297:331:velox/exec/SortBuffer.cpp
void SortBuffer::ensureSortFits() {
  // Check if spilling is enabled or not.
  if (spillConfig_ == nullptr) {
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill(pool_->name())) {
    spill();
    return;
  }

  if (numInputRows_ == 0 || inputSpiller_ != nullptr) {
    return;
  }

  // The memory for std::vector sorted rows and prefix sort required buffer.
  const auto sortBufferToReserve =
      numInputRows_ * sizeof(char*) +
      PrefixSort::maxRequiredBytes(
          data_.get(), sortCompareFlags_, prefixSortConfig_, pool_);
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_->maybeReserve(sortBufferToReserve)) {
      return;
    }
  }

  LOG(WARNING) << fmt::format(
      "Failed to reserve {} for memory pool {}, usage: {}, reservation: {}",
      succinctBytes(sortBufferToReserve),
      pool_->name(),
      succinctBytes(pool_->usedBytes()),
      succinctBytes(pool_->reservedBytes()));
}
```

- `**spillConfig_ == nullptr**`：整个函数 **no-op**（与 `**ensureInputFits`** 一致：无 spill 配置时不预留排序专用内存）。  
- `**numInputRows_ == 0 || inputSpiller_ != nullptr**`：无数据可排，或 **已进入输入 spill 模式**，不再为本阶段 `**PrefixSort::sort`** 预留 `**sortedRows_` + 前缀区**（曾 spill 分支会在 `**noMoreInput`** 里 `**spill()**` 而非依赖此处排序预留）。  
- `**sortBufferToReserve**`：**指针数组** `numInputRows_ * sizeof(char*)` **加上** `**PrefixSort::maxRequiredBytes(...)`**（若行数低于 `**prefixSortConfig_.minNumRows**` 等，`maxRequiredBytes` 内可能返回 **0**，见 `PrefixSort.cpp`）。  
- `**maybeReserve` 失败**：仅 `**LOG(WARNING)`**，**不**在此处 `**spill()`**；与 `**ensureInputFits**` 相同，**后续仍可能执行 `PrefixSort::sort`**（若实际分配由 `**allocateContiguous**` 在排序时触发 OOM，则依赖上层策略）。

### 7.4 Spill run 内排序（与 `SortBuffer` 路径对照）

`**SpillerBase::ensureSorted**`（`velox/exec/Spiller.cpp`）在 `**writeSpill**` 前对 `**run.rows**` 排序：

```224:256:velox/exec/Spiller.cpp
void SpillerBase::ensureSorted(SpillRun& run) {
  // The spill data of a hash join doesn't need to be sorted.
  if (run.sorted || !needSort()) {
    return;
  }

  uint64_t sortTimeNs{0};
  {
    NanosecondTimer timer(&sortTimeNs);

    if (!state_.prefixSortConfig().has_value()) {
      gfx::timsort(
          run.rows.begin(),
          run.rows.end(),
          [&](const char* left, const char* right) {
            return container_->compareRows(left, right, compareFlags_) < 0;
          });
    } else {
      PrefixSort::sort(
          container_,
          compareFlags_,
          state_.prefixSortConfig().value(),
          memory::spillMemoryPool(),
          run.rows);
    }

    run.sorted = true;
  }

  // NOTE: Always set a non-zero sort time to avoid flakiness in tests which
  // check sort time.
  updateSpillSortTime(std::max<uint64_t>(1, sortTimeNs));
}
```

- `**SortInputSpiller**`：`**needSort() == true**`，每个 **spill run** 在写盘前 **必须** 有序。  
- **与 `SortBuffer::noMoreInput` 的差异**：此处若 `**SpillState` 未带 `prefixSortConfig`**，使用 `**gfx::timsort` + `compareRows**`；若带了 `**prefixSortConfig**`，则调用 `**PrefixSort::sort**`，但 `**memory::spillMemoryPool()**` 与 `**SortBuffer**` 使用的 `**pool_**` 不同。  
- `**runSpill**` 中 `**needSort()**` 为真时，`**lastRun**` 分支会对分区调用 `**state_.finishFile(partitionId)**`（见 `**Spiller.cpp` `runSpill**`），保证 **下一 run 新文件**，从而 **merge 时按文件/run 有序**。

---

## 8. Spill：输入阶段 `SortInputSpiller`

涉及 `**velox/exec/SortBuffer.cpp`**（`spillInput`、`finishSpill`）、`**velox/exec/Spiller.{h,cpp}**`（`SortInputSpiller`、`SpillerBase`）、`**velox/exec/Spill.cpp**`（`SpillState::makeSortingKeys`）。

### 8.1 `SortBuffer::spillInput`：创建 `SortInputSpiller` 与首轮 `spill`

```347:356:velox/exec/SortBuffer.cpp
void SortBuffer::spillInput() {
  if (inputSpiller_ == nullptr) {
    VELOX_CHECK(!noMoreInput_);
    const auto sortingKeys = SpillState::makeSortingKeys(sortCompareFlags_);
    inputSpiller_ = std::make_unique<SortInputSpiller>(
        data_.get(), spillerStoreType_, sortingKeys, spillConfig_, spillStats_);
  }
  inputSpiller_->spill();
  data_->clear();
}
```

- **首次进入**：`**VELOX_CHECK(!noMoreInput_)`** —— 输入阶段 spill 不应在 `**noMoreInput_` 已为真** 时新建 `**inputSpiller_`**（与 `**noMoreInput**` 里「曾 spill 再 `spill()`」路径兼容：彼时 `**inputSpiller_` 已非空**，本块跳过构造）。  
- `**makeSortingKeys(sortCompareFlags_)`**（`velox/exec/Spill.cpp`）：为每个 `**sortCompareFlags_[i]**` 生成 `**SpillSortKey(i, flags)**`，**列下标 `i` 从 0 递增**，与 `**spillerStoreType_` / `RowContainer` 中「排序键前缀列」物理顺序** 一致（见第 4 节）。

```155:163:velox/exec/Spill.cpp
std::vector<SpillSortKey> SpillState::makeSortingKeys(
    const std::vector<CompareFlags>& compareFlags) {
  std::vector<SpillSortKey> sortingKeys;
  sortingKeys.reserve(compareFlags.size());
  for (column_index_t i = 0; i < compareFlags.size(); ++i) {
    sortingKeys.emplace_back(i, compareFlags[i]);
  }
  return sortingKeys;
}
```

- `**inputSpiller_->spill()**` 后 `**data_->clear()**`：**当前 `RowContainer` 内所有行** 释放；**计数 `numInputRows_` 不减少**（仍表示全局逻辑行数，供输出阶段 `**getOutput`** 与 merge 终止条件使用）。

### 8.2 `SortInputSpiller` 与 `SpillerBase` 构造参数对应表

`**SortInputSpiller` 初始化列表**（`velox/exec/Spiller.h`）：

```263:278:velox/exec/Spiller.h
  SortInputSpiller(
      RowContainer* container,
      RowTypePtr rowType,
      const std::vector<SpillSortKey>& sortingKeys,
      const common::SpillConfig* spillConfig,
      folly::Synchronized<velox::common::SpillStats>* spillStats)
      : SpillerBase(
            container,
            std::move(rowType),
            HashBitRange{},
            sortingKeys,
            std::numeric_limits<uint64_t>::max(),
            spillConfig->maxSpillRunRows,
            std::nullopt,
            spillConfig,
            spillStats) {}
```

`**SpillerBase` 形参顺序**（`velox/exec/Spiller.cpp`）及 **SortBuffer 传入值**：

```31:69:velox/exec/Spiller.cpp
SpillerBase::SpillerBase(
    RowContainer* container,
    RowTypePtr rowType,
    HashBitRange bits,
    const std::vector<SpillSortKey>& sortingKeys,
    uint64_t targetFileSize,
    uint64_t maxSpillRunRows,
    std::optional<SpillPartitionId> parentId,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<velox::common::SpillStats>* spillStats)
    : container_(container),
      executor_(spillConfig->executor),
      bits_(bits),
      rowType_(rowType),
      maxSpillRunRows_(maxSpillRunRows),
      parentId_(parentId),
      spillStats_(spillStats),
      compareFlags_([&sortingKeys]() {
        std::vector<CompareFlags> compareFlags;
        compareFlags.reserve(sortingKeys.size());
        for (const auto& [_, flags] : sortingKeys) {
          compareFlags.push_back(flags);
        }
        return compareFlags;
      }()),
      state_(
          spillConfig->getSpillDirPathCb,
          spillConfig->updateAndCheckSpillLimitCb,
          spillConfig->fileNamePrefix,
          sortingKeys,
          targetFileSize,
          spillConfig->writeBufferSize,
          spillConfig->compressionKind,
          spillConfig->prefixSortConfig,
          memory::spillMemoryPool(),
          spillStats,
          spillConfig->fileCreateConfig) {
  TestValue::adjust("facebook::velox::exec::SpillerBase", this);
}
```


| `SpillerBase` 参数                 | `SortInputSpiller` 传入               | 含义                                                                                                                |
| -------------------------------- | ----------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `**container**`                  | `**data_.get()**`                   | 待扫描的 `**RowContainer**`。                                                                                          |
| `**rowType**`                    | `**spillerStoreType_**`             | spill `**RowVector**` schema；与 `**extractSpill**` 子列顺序一致。                                                         |
| `**bits**`                       | `**HashBitRange{}**`                | **0 个有效分区位** → `**numPartitions()==1`**，`fillSpillRuns` 中 `**partitionNum` 恒为 0**。                                |
| `**sortingKeys`**                | `**makeSortingKeys(...)**`          | 写入 `**SpillState**`，供 **merge 比较** 与（若配置）前缀排序。                                                                    |
| `**targetFileSize`**             | `**UINT64_MAX**`                    | **不按「单文件目标字节」切分** spill 向量批；实际批大小由 `**writeSpill`** 内 `**kTargetBatchRows/Bytes**` 控制。                            |
| `**maxSpillRunRows**`            | `**spillConfig_->maxSpillRunRows**` | `**fillSpillRuns**` 单轮最多收集行数；超过则 `**lastRun=false**`，外层 `**do { fillSpill; runSpill } while (!lastRun)**` 继续扫迭代器。 |
| `**parentId**`                   | `**std::nullopt**`                  | 无嵌套分区 id；`**finishSpill**` 里 `**wholePartitionId**` 即子分区 id。                                                      |
| `**spillConfig` / `spillStats**` | 透传                                  | `**SpillState**` 拿 **路径回调、压缩、`prefixSortConfig`、`spillMemoryPool()`** 等。                                          |


`**SortInputSpiller::spill**` 仅转发：

```432:434:velox/exec/Spiller.cpp
void SortInputSpiller::spill() {
  SpillerBase::spill(nullptr);
}
```

`**startRowIter == nullptr**`：`SpillerBase::spill` 使用 **默认 `RowContainerIterator`**，从 **容器内全部仍占用行** 开始 `**listRows`**（与聚合 **InputSpiller 整表 spill** 语义一致）。

### 8.3 `SpillerBase::spill` → `fillSpillRuns` → `runSpill` → `writeSpill`

**总控循环**：

```71:86:velox/exec/Spiller.cpp
void SpillerBase::spill(const RowContainerIterator* startRowIter) {
  VELOX_CHECK(!finalized_);

  RowContainerIterator rowIter;
  if (startRowIter != nullptr) {
    rowIter = *startRowIter;
  }

  bool lastRun{false};
  do {
    lastRun = fillSpillRuns(&rowIter);
    runSpill(lastRun);
  } while (!lastRun);

  checkEmptySpillRuns();
}
```

`**fillSpillRuns**`（节选）：每批最多 `**kHashBatchSize = 4096**` 行 `**listRows**`；`**isSinglePartition**` 时 **不做 hash**，所有行进入 `**SpillPartitionId(0)`** 的 `**SpillRun::rows**`；`**maxSpillRunRows_ > 0**` 且 `**totalRows >= maxSpillRunRows_**` 时 `**break**` 且 `**lastRun = false**`，下一轮继续 `**listRows**`。

`**runSpill**`：对每个非空 `**spillRun**` 投递 `**writeSpill(partitionId)**`（可 `**executor_` 并行 `prepare**`）；完成后 `**run.clear()**`；若 `**needSort()**`（`**SortInputSpiller` 为 true**），`**state_.finishFile(partitionId)`** —— **每个已排序 run 结束后关文件**，下次 `**appendToPartition`** 新开文件，保证 **单文件内行序与 run 序一致**，便于 **有序 merge**。

`**writeSpill`**：

```198:216:velox/exec/Spiller.cpp
std::unique_ptr<SpillerBase::SpillStatus> SpillerBase::writeSpill(
    const SpillPartitionId& id) {
  constexpr int32_t kTargetBatchBytes = 1 << 18; // 256K
  constexpr int32_t kTargetBatchRows = 64;

  RowVectorPtr spillVector;
  auto& run = spillRuns_.at(id);
  try {
    ensureSorted(run);
    size_t written = 0;
    while (written < run.rows.size()) {
      extractSpillVector(
          run.rows, kTargetBatchRows, kTargetBatchBytes, spillVector, written);
      state_.appendToPartition(id, spillVector);
    }
    return std::make_unique<SpillStatus>(id, written, nullptr);
```

- `**ensureSorted**`： `**SortInputSpiller::needSort()==true**`，对 `**run.rows**` 排序（`**timsort` 或 `PrefixSort::sort**`，见第 7.4 节）。  
- `**extractSpillVector**`：在 `**maxRows`/`maxBytes**` 约束下切片 `**run.rows**`，调用 `**extractSpill**` 物化 `**spillVector**`，再 `**state_.appendToPartition**` 序列化写盘。

### 8.4 `extractSpill` / `extractSpillVector`：行指针 → 列式 batch

`**extractSpillVector**` 累加 `**rowSize**` 直到超过 `**kTargetBatchBytes**`（至少一行）：

```258:285:velox/exec/Spiller.cpp
int64_t SpillerBase::extractSpillVector(
    SpillRows& rows,
    int32_t maxRows,
    int64_t maxBytes,
    RowVectorPtr& spillVector,
    size_t& nextBatchIndex) {
  uint64_t extractNs{0};
  auto limit = std::min<size_t>(rows.size() - nextBatchIndex, maxRows);
  VELOX_CHECK(!rows.empty());
  int32_t numRows = 0;
  int64_t bytes = 0;
  {
    NanosecondTimer timer(&extractNs);
    for (; numRows < limit; ++numRows) {
      bytes += container_->rowSize(rows[nextBatchIndex + numRows]);
      if (bytes > maxBytes) {
        // Increment because the row that went over the limit is part
        // of the result. We must spill at least one row.
        ++numRows;
        break;
      }
    }
    extractSpill(folly::Range(&rows[nextBatchIndex], numRows), spillVector);
    nextBatchIndex += numRows;
  }
  updateSpillExtractVectorTime(extractNs);
  return bytes;
}
```

`**extractSpill**`：

```287:309:velox/exec/Spiller.cpp
void SpillerBase::extractSpill(
    folly::Range<char**> rows,
    RowVectorPtr& resultPtr) {
  if (resultPtr == nullptr) {
    resultPtr = BaseVector::create<RowVector>(
        rowType_, rows.size(), memory::spillMemoryPool());
  } else {
    resultPtr->prepareForReuse();
    resultPtr->resize(rows.size());
  }

  auto* result = resultPtr.get();
  const auto& types = container_->columnTypes();
  for (auto i = 0; i < types.size(); ++i) {
    container_->extractColumn(rows.data(), rows.size(), i, result->childAt(i));
  }
  const auto& accumulators = container_->accumulators();
  column_index_t accumulatorColumnOffset = types.size();
  for (auto i = 0; i < accumulators.size(); ++i) {
    accumulators[i].extractForSpill(
        rows, result->childAt(i + accumulatorColumnOffset));
  }
}
```

- `**RowVector**` 建在 `**memory::spillMemoryPool()**` 上，与 `**SortBuffer**` 的 `**pool_**` 分离。  
- **OrderBy**：`**accumulators_` 为空**，`**extractForSpill`** 循环 **不执行**；仅 `**extractColumn`** 按 `**container_->columnTypes()**` 下标与 `**rowType_`（`spillerStoreType_`）** 对齐。  
- 语义：**一批 `char*` 行** → **一批列向量 `RowVector`** → `**SpillState::appendToPartition**` 序列化。

### 8.5 `finishSpill`：`SpillerBase` 与 `SortBuffer` 衔接

`**SpillerBase::finishSpill**` 把 `**SpillState**` 内各分区 **finish 成文件列表**，并入 `**SpillPartitionSet`**：

```335:355:velox/exec/Spiller.cpp
void SpillerBase::finishSpill(SpillPartitionSet& partitionSet) {
  finalizeSpill();

  for (const auto& partitionId : state_.spilledPartitionIdSet()) {
    auto wholePartitionId = partitionId;
    if (parentId_.has_value()) {
      wholePartitionId =
          SpillPartitionId(parentId_.value(), partitionId.partitionNumber());
    }
    if (partitionSet.count(wholePartitionId) == 0) {
      partitionSet.emplace(
          wholePartitionId,
          std::make_unique<SpillPartition>(
              wholePartitionId, state_.finish(partitionId)));
    } else {
      partitionSet[wholePartitionId]->addFiles(state_.finish(partitionId));
    }
  }
}
```

`**SortBuffer::finishSpill**`（在 `**noMoreInput` 曾 spill 分支** 或 `**spillOutput`** 末尾调用）：

```460:477:velox/exec/SortBuffer.cpp
void SortBuffer::finishSpill() {
  VELOX_CHECK_NULL(spillMerger_);
  VELOX_CHECK(spillPartitionSet_.empty());
  VELOX_CHECK_EQ(
      !!(outputSpiller_ != nullptr) + !!(inputSpiller_ != nullptr),
      1,
      "inputSpiller_ {}, outputSpiller_ {}",
      inputSpiller_ == nullptr ? "set" : "null",
      outputSpiller_ == nullptr ? "set" : "null");
  if (inputSpiller_ != nullptr) {
    VELOX_CHECK(!inputSpiller_->finalized());
    inputSpiller_->finishSpill(spillPartitionSet_);
  } else {
    VELOX_CHECK(!outputSpiller_->finalized());
    outputSpiller_->finishSpill(spillPartitionSet_);
  }
  VELOX_CHECK_EQ(spillPartitionSet_.size(), 1);
}
```

- 要求 `**spillMerger_` 尚未创建**、`**spillPartitionSet_` 为空**（避免重复 finish）。  
- **恰好一个 spiller 非空**；`**finishSpill` 前 `inputSpiller_` 不得 `finalized()`**（`**finalizeSpill**` 置位）。  
- `**VELOX_CHECK_EQ(spillPartitionSet_.size(), 1)**`：**单分区** 与 `**HashBitRange{}`** 一致；后续 `**createOrderedReader**` 只处理该分区（第 10 节）。

---

## 9. Spill：输出阶段 `SortOutputSpiller`

### 9.1 `SortBuffer::spill()`：何时走 `spillOutput`

```183:198:velox/exec/SortBuffer.cpp
void SortBuffer::spill() {
  VELOX_CHECK_NOT_NULL(
      spillConfig_, "spill config is null when SortBuffer spill is called");

  // Check if sort buffer is empty or not, and skip spill if it is empty.
  if (data_->numRows() == 0) {
    return;
  }
  updateEstimatedOutputRowSize();

  if (sortedRows_.empty()) {
    spillInput();
  } else {
    spillOutput();
  }
}
```

- `**sortedRows_.empty()**` → `**spillInput()**`（第 8 节）：**内存态仍为 `RowContainer` 行**，尚未 `**listRows` + `PrefixSort::sort`**。  
- `**sortedRows_` 非空** → `**spillOutput()`**：**已完成内存排序**（`**noMoreInput`** 里 `**PrefixSort::sort**` 已执行），`**sortedRows_[i]**` 为 **全局序下的行指针**；此时 `**data_` 仍持有对应行缓冲**（直至 `**spillOutput` 末尾 `data_->clear()`**），`**spill()` 开头 `data_->numRows() == 0` 检查** 针对的是 **容器是否还有可 spill 行** —— 若排序后 **行仍在 `data_` 中**，`**numRows() > 0`** 才会进入 `**spillOutput**`。

典型触发：`**ensureOutputFits**`（第 10.1 节）里 `**testingTriggerSpill**` → `**spill()**`；或 **仲裁 `reclaim` → `OrderBy::reclaim` → `sortBuffer_->spill()`**；发生在 `**getOutput**` 已部分消耗 `**sortedRows_**`（`**numOutputRows_ > 0**`）之后时，**只 spill 剩余尾部**（见 §9.2）。

### 9.2 `SortBuffer::spillOutput` 与 `SortOutputSpiller`

```358:381:velox/exec/SortBuffer.cpp
void SortBuffer::spillOutput() {
  if (hasSpilled()) {
    // Already spilled.
    return;
  }
  if (numOutputRows_ == sortedRows_.size()) {
    // All the output has been produced.
    return;
  }

  outputSpiller_ = std::make_unique<SortOutputSpiller>(
      data_.get(), spillerStoreType_, spillConfig_, spillStats_);
  auto spillRows = SpillerBase::SpillRows(
      sortedRows_.begin() + numOutputRows_,
      sortedRows_.end(),
      *memory::spillMemoryPool());
  outputSpiller_->spill(spillRows);
  data_->clear();
  sortedRows_.clear();
  sortedRows_.shrink_to_fit();
  // Finish right after spilling as the output spiller only spills at most
  // once.
  finishSpill();
}
```

- `**hasSpilled()**`（`175:181:velox/exec/SortBuffer.cpp`）：若 `**inputSpiller_ != nullptr**` 已为 true；**输出 tail spill** 要求 `**inputSpiller_` 为空**（`**VELOX_CHECK_NULL(outputSpiller_)`** 在 `**noMoreInput**`），故 **仅纯内存排序 + 输出阶段首次 spill** 可走本路径；`**hasSpilled()` 为 true** 时 **直接 return**（避免重复）。  
- `**numOutputRows_ == sortedRows_.size()`**：**已吐完**，无尾部可 spill。  
- `**SpillRows`**：分配器为 `**spillMemoryPool()**`，`**[begin+numOutputRows_, end)**` 指向 **尚未输出的、全局序后缀** 的 `**char*`**。  
- `**data_->clear()**`：释放 `**RowContainer**` 中行；`**sortedRows_.clear()**` 后 **merge 输出不再走 `extractColumn` from `sortedRows_`**，仅 `**getOutputWithSpill**`。  
- `**finishSpill()**`：把 `**outputSpiller_**` 的 `**SpillState**` 收口到 `**spillPartitionSet_**`（§8.5），供 `**createOrderedReader**`。

`**SortOutputSpiller` 构造**（`velox/exec/Spiller.cpp`）：`**sortingKeys` 为 `{}`**，故 `**SpillerBase::compareFlags_` 为空**，`**needSort()` 返回 `false`**。

```436:450:velox/exec/Spiller.cpp
SortOutputSpiller::SortOutputSpiller(
    RowContainer* container,
    RowTypePtr rowType,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<velox::common::SpillStats>* spillStats)
    : SpillerBase(
          container,
          std::move(rowType),
          HashBitRange{},
          {},
          std::numeric_limits<uint64_t>::max(),
          spillConfig->maxSpillRunRows,
          std::nullopt,
          spillConfig,
          spillStats) {}
```

`**SortOutputSpiller::spill` / `runSpill**`：

```452:483:velox/exec/Spiller.cpp
void SortOutputSpiller::spill(SpillRows& rows) {
  VELOX_CHECK(!finalized_);
  VELOX_CHECK(!rows.empty());

  VELOX_CHECK_EQ(bits_.numPartitions(), 1);
  checkEmptySpillRuns();
  uint64_t execTimeNs{0};
  {
    NanosecondTimer timer(&execTimeNs);
    auto& spillRun = createOrGetSpillRun(SpillPartitionId(0));
    spillRun.rows =
        SpillRows(rows.begin(), rows.end(), spillRun.rows.get_allocator());
    for (const auto* row : rows) {
      spillRun.numBytes += container_->rowSize(row);
    }
    markSeenPartitionsSpilled();
  }

  updateSpillFillTime(execTimeNs);
  runSpill(true);

  checkEmptySpillRuns();
}

void SortOutputSpiller::runSpill(bool lastRun) {
  SpillerBase::runSpill(lastRun);
  if (lastRun) {
    for (const auto& [partitionId, spillRun] : spillRuns_) {
      state_.finishFile(partitionId);
    }
  }
}
```

- **单次调用**内 `**runSpill(true)`**：`**writeSpill**` 中 `**ensureSorted**` 因 `**needSort()==false**` **立即返回**（`Spiller.cpp` `**ensureSorted`**），**不**对 `**run.rows`** 重排 —— 依赖 `**spillRows` 本身已全局有序**。  
- `**SortOutputSpiller::runSpill` 覆写**：`**lastRun==true`** 时对 **所有分区 `finishFile`**；与 `**needSort()==true**` 时在 `**SpillerBase::runSpill` 每分区 `finishFile**` 的分工不同（输出 spiller **在覆写里统一关文件**）。

### 9.3 `hasSpilled` 与后续 `getOutput` 分支

```175:181:velox/exec/SortBuffer.cpp
bool SortBuffer::hasSpilled() const {
  if (inputSpiller_ != nullptr) {
    VELOX_CHECK_NULL(outputSpiller_);
    return true;
  }
  return outputSpiller_ != nullptr;
}
```

- **输入 spill**：`**inputSpiller_` 非空** → `**hasSpilled()==true`**，且 `**outputSpiller_` 必为空**。  
- **仅输出 tail spill**：`**outputSpiller_` 非空** → `**hasSpilled()==true`**，`**sortedRows_**` 已被 `**clear**`，`**getOutput**` 走 `**getOutputWithSpill**`。

---

## 10. 输出阶段：`getOutput`

### 10.1 总控：`getOutput` 与 `ensureOutputFits`

```151:173:velox/exec/SortBuffer.cpp
RowVectorPtr SortBuffer::getOutput(vector_size_t maxOutputRows) {
  SCOPE_EXIT {
    pool_->release();
  };

  VELOX_CHECK(noMoreInput_);

  if (numOutputRows_ == numInputRows_) {
    return nullptr;
  }
  VELOX_CHECK_GT(maxOutputRows, 0);
  VELOX_CHECK_GT(numInputRows_, numOutputRows_);
  const vector_size_t batchSize =
      std::min<uint64_t>(numInputRows_ - numOutputRows_, maxOutputRows);
  ensureOutputFits(batchSize);
  prepareOutput(batchSize);
  if (hasSpilled()) {
    getOutputWithSpill();
  } else {
    getOutputWithoutSpill();
  }
  return output_;
}
```

- `**SCOPE_EXIT**`：每轮 `**getOutput**` 结束 `**pool_->release()**`，与 `**ensureOutputFits` / `prepareOutput` 的 `maybeReserve**` 对称。  
- `**numOutputRows_ == numInputRows_**`：**无剩余行**（含 **0 行输入** 时在 `**noMoreInput`** 后 `**getOutput` 首次即 nullptr**）。

`**ensureOutputFits`**：

```265:295:velox/exec/SortBuffer.cpp
void SortBuffer::ensureOutputFits(vector_size_t batchSize) {
  VELOX_CHECK_GT(batchSize, 0);
  // Check if spilling is enabled or not.
  if (spillConfig_ == nullptr) {
    return;
  }

  // Test-only spill path.
  if (testingTriggerSpill(pool_->name())) {
    spill();
    return;
  }

  if (!estimatedOutputRowSize_.has_value() || hasSpilled()) {
    return;
  }

  const uint64_t outputBufferSizeToReserve =
      estimatedOutputRowSize_.value() * batchSize * 1.2;
  {
    memory::ReclaimableSectionGuard guard(nonReclaimableSection_);
    if (pool_->maybeReserve(outputBufferSizeToReserve)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve "
               << succinctBytes(outputBufferSizeToReserve)
               << " for memory pool " << pool_->name()
               << ", usage: " << succinctBytes(pool_->usedBytes())
               << ", reservation: " << succinctBytes(pool_->reservedBytes());
}
```

- `**hasSpilled()**` 为真时 **直接 return**：输出 batch 与 **merge** 的内存策略 **不再依赖** 此处 `**estimatedOutputRowSize_ * 1.2`** 预留。  
- `**maybeReserve` 失败**：仅 **WARNING**，**不**在本函数内 `**spill()`**（与 `**ensureInputFits**` 一致）；测试路径 `**testingTriggerSpill**` 会 `**spill()**`，可能转入 `**spillOutput()**`（§9）。

### 10.2 `prepareOutput` 与 `prepareOutputWithSpill`

```383:401:velox/exec/SortBuffer.cpp
void SortBuffer::prepareOutput(vector_size_t batchSize) {
  if (output_ != nullptr) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, batchSize);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = std::static_pointer_cast<RowVector>(
        BaseVector::create(input_, batchSize, pool_));
  }

  if (hasSpilled()) {
    spillSources_.resize(batchSize);
    spillSourceRows_.resize(batchSize);
    prepareOutputWithSpill();
  }

  VELOX_CHECK_GT(output_->size(), 0);
  VELOX_CHECK_LE(output_->size() + numOutputRows_, numInputRows_);
}
```

`**prepareOutputWithSpill**`（**首次**建 merge 树；之后 `**spillPartitionSet_` 已 clear**，仅 `**spillMerger_` 复用**）：

```479:490:velox/exec/SortBuffer.cpp
void SortBuffer::prepareOutputWithSpill() {
  VELOX_CHECK(hasSpilled());
  if (spillMerger_ != nullptr) {
    VELOX_CHECK(spillPartitionSet_.empty());
    return;
  }

  VELOX_CHECK_EQ(spillPartitionSet_.size(), 1);
  spillMerger_ = spillPartitionSet_.begin()->second->createOrderedReader(
      *spillConfig_, pool(), spillStats_);
  spillPartitionSet_.clear();
}
```

`**SpillPartition::createOrderedReader**`：

```501:550:velox/exec/Spill.cpp
std::unique_ptr<TreeOfLosers<SpillMergeStream>>
SpillPartition::createOrderedReader(
    const common::SpillConfig& spillConfig,
    memory::MemoryPool* pool,
    folly::Synchronized<common::SpillStats>* spillStats) {
  const auto numMaxMergeFiles = spillConfig.numMaxMergeFiles;
  VELOX_CHECK_NE(numMaxMergeFiles, 1);
  if (numMaxMergeFiles == 0 || files_.size() <= numMaxMergeFiles) {
    return createOrderedReaderInternal(
        spillConfig.readBufferSize, pool, spillStats);
  }
  ...
  return createOrderedReaderInternal(
      spillConfig.readBufferSize, pool, spillStats);
}
```

- `**numMaxMergeFiles == 1**`：**非法**（`VELOX_CHECK_NE`）。  
- **文件数过多**：先 `**mergeSpillFiles`** 多轮合并，再 `**createOrderedReaderInternal**`（每文件 `**FileSpillMergeStream**` → `**TreeOfLosers<SpillMergeStream>**`；见 `**Spill.cpp` `createOrderedReaderInternal**`）。

`**SortBuffer**` 传入 `**pool()**`（算子 **leaf pool**）与 `**spillStats_`**，用于 **读盘缓冲与统计**。

### 10.3 无 spill：`getOutputWithoutSpill`

```403:413:velox/exec/SortBuffer.cpp
void SortBuffer::getOutputWithoutSpill() {
  VELOX_DCHECK_EQ(numInputRows_, sortedRows_.size());
  for (const auto& columnProjection : columnMap_) {
    data_->extractColumn(
        sortedRows_.data() + numOutputRows_,
        output_->size(),
        columnProjection.inputChannel,
        output_->childAt(columnProjection.outputChannel));
  }
  numOutputRows_ += output_->size();
}
```

- `**extractColumn**`：从 `**RowContainer**` 的 `**inputChannel**`（**容器/spill 物理列序**）读到 `**output_->childAt(outputChannel)`**（**计划 `input_` 列序**），与 `**addInput` 的 `store`** 互为逆映射（第 4、6 节）。  
- `**numOutputRows_**` 按 `**output_->size()**` 递增。

### 10.4 有 spill：`getOutputWithSpill` 与 `gatherCopy`

```415:458:velox/exec/SortBuffer.cpp
void SortBuffer::getOutputWithSpill() {
  VELOX_CHECK_NOT_NULL(spillMerger_);
  VELOX_DCHECK_EQ(sortedRows_.size(), 0);

  int32_t outputRow = 0;
  int32_t outputSize = 0;
  bool isEndOfBatch = false;
  while (outputRow + outputSize < output_->size()) {
    SpillMergeStream* stream = spillMerger_->next();
    VELOX_CHECK_NOT_NULL(stream);

    spillSources_[outputSize] = &stream->current();
    spillSourceRows_[outputSize] = stream->currentIndex(&isEndOfBatch);
    ++outputSize;
    if (FOLLY_UNLIKELY(isEndOfBatch)) {
      // The stream is at end of input batch. Need to copy out the rows before
      // fetching next batch in 'pop'.
      gatherCopy(
          output_.get(),
          outputRow,
          outputSize,
          spillSources_,
          spillSourceRows_,
          columnMap_);
      outputRow += outputSize;
      outputSize = 0;
    }
    // Advance the stream.
    stream->pop();
  }
  VELOX_CHECK_EQ(outputRow + outputSize, output_->size());

  if (FOLLY_LIKELY(outputSize != 0)) {
    gatherCopy(
        output_.get(),
        outputRow,
        outputSize,
        spillSources_,
        spillSourceRows_,
        columnMap_);
  }

  numOutputRows_ += output_->size();
}
```

`**gatherCopy` + `columnMap**`（`velox/exec/OperatorUtils.cpp`）：

```421:456:velox/exec/OperatorUtils.cpp
void gatherCopy(
    RowVector* target,
    vector_size_t targetIndex,
    vector_size_t count,
    const std::vector<const RowVector*>& sources,
    const std::vector<vector_size_t>& sourceIndices,
    const std::vector<IdentityProjection>& columnMap) {
  ...
  if (!columnMap.empty()) {
    for (const auto& columnProjection : columnMap) {
      gatherCopy(
          target->childAt(columnProjection.outputChannel).get(),
          targetIndex,
          count,
          sources,
          sourceIndices,
          columnProjection.inputChannel);
    }
  } else {
    ...
  }
}
```

- **merge batch 的列序** = `**spillerStoreType_` / 磁盘 RowVector**；`**columnMap_`** 把 `**inputChannel**`（spill 列下标）映到 `**outputChannel**`（计划列下标），与 **无 spill 的 `extractColumn`** 同一套投影。  
- `**isEndOfBatch**`：`**pop()**` 会加载下一 batch，`**current()**` 指向的 `**RowVector**` 可能失效，故 **必须先 `gatherCopy` 落盘到 `output_`**。

`**SpillMergeStream**`（`velox/exec/Spill.h`）：`**compare` / `operator<**` 基于各流 `**sortingKeys()**` 与 `**DecodedVector**`。`**SpillState**` 在 `**SpillerBase**` 构造时绑定 `**sortingKeys**`：`**SortInputSpiller**` 使用 `**SpillState::makeSortingKeys(sortCompareFlags_)**`（第 8 节），`**SortOutputSpiller**` 传入 **空 `std::vector<SpillSortKey>{}`**（`Spiller.cpp` `**SortOutputSpiller` 构造**）。`**TreeOfLosers::next`** 在多流间选出 **当前最小** 行；**输入 spill** 路径下 **各 run 经 `ensureSorted` 与排序键** 对齐 **全局序**；**输出 tail spill** 路径下 **待写盘指针段已全局有序** 且 `**needSort()==false`**，**merge 仍通过同一套 `createOrderedReader` + `SpillMergeStream`** 做 **k 路归并输出**。

---

## 11. 完整调用链速查（ASCII）

### 11.1 纯内存（无 spill）

```text
Driver → OrderBy::addInput → SortBuffer::addInput
  → [可选] ensureInputFits（通常不 spill）
  → newRow × N → store（每列）列→行

Driver → OrderBy::noMoreInput → SortBuffer::noMoreInput
  → ensureSortFits
  → listRows → PrefixSort::sort（行指针数组）

Driver → OrderBy::getOutput → SortBuffer::getOutput
  → ensureOutputFits → prepareOutput
  → getOutputWithoutSpill → extractColumn（行→列，按 columnMap_）
```

### 11.2 输入阶段 spill + merge 输出

```text
OrderBy::addInput → SortBuffer::addInput → ensureInputFits → [失败路径/测试] spill
  → spillInput（首次建 SortInputSpiller）
  → SpillerBase::spill → fillSpillRuns → runSpill → writeSpill
       → ensureSorted（PrefixSort/timsort on run.rows）
       → extractSpillVector → extractSpill（行→RowVector）
       → SpillState::appendToPartition
  → data_->clear()

OrderBy::noMoreInput → SortBuffer::noMoreInput
  → spill（刷剩余）→ finishSpill → spillPartitionSet_（单分区）

OrderBy::getOutput → SortBuffer::getOutput
  → prepareOutputWithSpill（首次）
       → SpillPartition::createOrderedReader[Internal]
       → TreeOfLosers<FileSpillMergeStream>
  → getOutputWithSpill → next/pop + gatherCopy（列→列+列序映射）
```

### 11.3 输出阶段 tail spill（`SortOutputSpiller`）

```text
SortBuffer::getOutput → ensureOutputFits → [测试] spill
  → spillOutput
  → SortOutputSpiller::spill(sortedRows tail)
  → runSpill（needSort=false，不 ensureSorted）
  → extractSpill → finishSpill
  → sortedRows_.clear(); data_->clear()
后续 getOutput 同 10.2 的 merge 路径（hasSpilled() 为真）
```

### 11.4 仲裁 reclaim

```text
Memory reclaimer → OrderBy::reclaim
  → SortBuffer::spill()   // 见 SortBuffer::spill 分支 input vs output
  → pool()->release()
```

---

## 12. 小结：行列与类型锚点（OrderBy 专用）

1. **内存行布局** = `**RowContainer`(key = 排序键列顺序, dependent = 其余列)**，与 `**columnMap_`** 一起锚定 **「物理列下标 ↔ 计划列下标」**。
2. **Spill 文件 schema** = `**spillerStoreType_`** = **与 `extractSpill` 使用的 `rowType_` 一致**，列序为 **排序键前缀 + 非键列**。
3. **写盘**：`**RowContainer` 行** → `**extractColumn`** → `**RowVector**` → **序列化页**。
4. **读盘归并**：**页** → `**RowVector` batch** → `**SpillMergeStream` 按 `SpillSortKey` 比较** → `**TreeOfLosers` 全序弹出** → `**gatherCopy` + `columnMap_`** → **计划列序的 `output_`**。
5. **排序键比较**：内存路径 `**RowContainer::compare` / `PrefixSort`**；spill 路径 `**SpillMergeStream**` 使用 **构造 `SpillState` 时传入的 sorting keys**（由 `**SpillState::makeSortingKeys(sortCompareFlags_)`** 生成），**列索引与 `spillerStoreType_` 中排序键前缀列对齐**。
6. **Partial + `LocalMerge` / `MergeExchange`**：各 `**OrderBy**` 仍按上表 1–5 在 **单 pipeline** 内处理；**跨流有序归并** 在 `**SourceMerger` + `TreeOfLosers<SourceStream>`** 上完成，比较键为 `**Merge::sortingKeys_`（`SpillSortKey`）**，与 `**LocalMergeNode` / `MergeExchangeNode` 计划键** 一致（见 **第 2 节**）。

---

## 13. 参考配置与延伸阅读

- **OrderBy spill 开关**：`QueryConfig::kOrderBySpillEnabled` / `orderBySpillEnabled()`（`velox/core/QueryConfig.h`）。
- **LocalMerge spill / 分组**：`kLocalMergeSpillEnabled` / `localMergeSpillEnabled()`；`**kLocalMergeMaxNumMergeSources`** / `localMergeMaxNumMergeSources()`（与 `**Merge::maxNumMergeSources_**`、`**needSpill()**` 联动）；`**localMergeSourceQueueSize()**`（`MergeSource` 队列深度）。
- **MergeExchange**：`**maxMergeExchangeBufferSize()`** 等（`MergeExchange::addMergeSources` 中计算 `**maxQueuedBytesPerSource**`）；shuffle 侧 `**shuffleCompressionKind()**` 与 `**MergeExchangeNode::serdeKind**`。
- **通用 spill 行为、文件大小、merge 文件数、压缩、buffer** 等：见 `**velox/common/base/SpillConfig.h`** 与 `**velox/docs/develop/spilling.rst**`。
- **与 HashAggregation spill 文档的对照**：`**velox/docs/develop/HashAggregationSpillDetailed.md`** 第 10–12 节（`finishSpill`、`createOrderedReader`、`TreeOfLosers`）与本篇 **第 8–10 节** 描述的是 **同一套 Spill 基础设施** 在不同算子上的接入方式差异。

---

## 14. 已知限制与 TODO（来自源码注释）

- `**OrderBy::reclaim`**：**TODO**：在具备 **RowContainer 压缩/更细粒度** 能力前，**不按 `targetBytes` 精细 spill**，当前为 `**sortBuffer_->spill()`** 粗粒度行为。
- `**SortBuffer::noMoreInput**`：若已发生 spill，**输出阶段不支持继续 spill**，因此在 `**noMoreInput`** 时会 **再次 `spill()`** 刷净内存态。
- `**OrderBy.h**`：实现上有 **两次 memcpy 量级** 的代价提示 —— **输入写入 `RowContainer`** 与 **输出 `extractColumn` / `gatherCopy`**。

---

*文档版本：与 Velox 源码树一致；若后续重构 `SortBuffer` 或 `SpillerBase` API，请以实际源码为准更新本篇交叉引用。*