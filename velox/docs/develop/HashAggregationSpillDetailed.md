# HashAggregation 算子 Spill 流程详解

本文档在源码层面说明 Velox 中 **HashAggregation** 与 **GroupingSet**、**Spiller**、**SpillState/SpillWriter**、**SpillPartition** 协作完成磁盘 spill 的完整路径，并单独强调 **行式（RowContainer）与列式（RowVector）** 及 **类型/列序** 在各个环节的对应关系。

相关源码主要位于：

- `velox/exec/HashAggregation.{h,cpp}`
- `velox/exec/GroupingSet.{h,cpp}`
- `velox/exec/Spiller.{h,cpp}`
- `velox/exec/Spill.{h,cpp}`、`velox/exec/SpillFile.{h,cpp}`
- `velox/serializers/SerializedPageFile.{h,cpp}`

---

## 1. 总体架构与数据形态

### 1.1 三层对象

| 组件 | 作用 |
|------|------|
| **HashAggregation** | 算子生命周期、可选 `SpillConfig`、转发 `addInput` / `noMoreInput` / `getOutput` / `reclaim`。 |
| **GroupingSet** | 哈希表、`RowContainer`、何时 `spill()`、何时 `getOutputWithSpill()`、merge 用 `mergeRows_` 与聚合函数接口。 |
| **SpillerBase**（`AggregationInputSpiller` / `AggregationOutputSpiller`） | 从 `RowContainer` 扫行 → 分区 → 可选排序 → 批式 materialize 为 `RowVector` → `SpillState::appendToPartition` 写文件。 |

### 1.2 行式 vs 列式（全文核心）

1. **运行时聚合状态**：每个 group 对应 `RowContainer` 里一行 **紧凑行缓冲**（key 列 + accumulator 固定区 + 变长/外部内存由 `HashStringAllocator` 等管理）。这是 **行式、按 group 交错存储**。
2. **写盘前**：`SpillerBase::extractSpill` 把一批 `char*` 行转成一批 **列式 `RowVector`**（每列一个 `VectorPtr`），便于走 **Presto VectorSerde** 的 `VectorStreamGroup` 序列化。
3. **读盘后**：`SerializedPageFileReader::nextBatch` 通过 `VectorStreamGroup::read` 反序列化为 **`RowVector`**，列布局与 spill 时的 **RowType** 一致。
4. **Merge 回聚合状态**：`SpillMergeStream` 对当前 batch 做 `DecodedVector`；**分组键** 用 `RowContainer::store` 从 decoded 列写入 **merge 用 `RowContainer` 的一行**；**中间态** 用 `Aggregate::addSingleGroupIntermediateResults` / `addSingleGroupSpillInput` 等，把 **列向量中某一行** 合并进该行 accumulator。

因此 spill 路径本质是：**RowContainer（行）↔ RowVector（列）↔ 磁盘页（Presto 序列化）**，在 merge 阶段再次 **列 → 行（mergeRows_）**。

### 1.3 HashAggregation / GroupingSet 内：输入是列存，聚合状态是行存

**结论**：算子从上游收到的 **`RowVectorPtr input` 是标准列存 batch**（每列 `VectorPtr`，一行跨列索引对齐）。**每个 group 的累加状态** 存在 **`RowContainer` 的一行**里（与哈希表 slot 关联的 `char*`），是 **行式紧凑布局**。Spill 只是把后者批式投影成列存再序列化。

**入口**：`HashAggregation::addInput` → `GroupingSet::addInput` → **`GroupingSet::addInputForActiveRows`**（`velox/exec/GroupingSet.cpp`）。

**处理管线（与 spill 相关的数据形态）**：

1. **`table_->prepareForGroupProbe` / `table_->groupProbe`**：在 **列存 `input`** 上按 active 行做哈希探测，得到 **`lookup_->hits`**（每个输入行对应的 **group 行指针 `char*`**）以及 **`newGroups`**（本批新建 group）。
2. **聚合更新**：对第 `i` 个聚合调用 **`populateTempVectors(i, input)`**，从 **列存 input** 上取出该聚合需要的子列（可能包一层 constant wrap），得到 **`tempVectors_`** —— 仍是 **列向量**，但通常只引用 input 子列、不整表转置。
3. **`function->addRawInput` / `addIntermediateResults`**：签名语义是「对一批 **输入行**（`SelectivityVector rows`），把 **列向量 `tempVectors_`** 上的值累加到 **各 `groups[j]` 指向的 RowContainer 行**」。即：**列存输入行 → 行存 group 状态**。
4. **Distinct / sorted 聚合**：同样在 **`groups` + `input`** 之间桥接，状态仍在 **`RowContainer`**。

因此：

- **没有**在算子内部把整表 input 先转成「行数组结构体」再聚合；Velox 一贯是 **向量化列 + 按行号选中的 group 指针**。
- **Spill 读回 merge** 时则相反：从 **列存 spill batch** 用 **`addSingleGroupIntermediateResults`** 写回 **`mergeRows_` 行存**，再 **`extractGroups`** 列存输出。

---

## 2. Spill 触发时机（与关键调用）

### 2.1 输入阶段：内存预留失败 → 仲裁 reclaim

- **调用链**：`GroupingSet::addInputForActiveRows` → `GroupingSet::ensureInputFits`（`GroupingSet.cpp`）。
- **逻辑要点**：对 **非 partial** 且配置了 `spillConfig_` 的聚合，根据当前 `pool_` 用量、`maybeReserve` 结果决定是否触发测试仲裁或依赖上层 **MemoryArbitrator**。
- **到算子**：`Operator::MemoryReclaimer::reclaim`（`Operator.cpp`）→ `HashAggregation::reclaim`（`HashAggregation.cpp`）。
- **仍在收输入时**：`HashAggregation::reclaim` 调用 **`GroupingSet::spill()`**（无迭代器重载），即 **整表** 走 **InputSpiller** 写盘，然后 `table_->clear`（见 `GroupingSet::spill` 末尾）。

### 2.2 `noMoreInput`：已发生过 spill 时刷净内存态

- **调用链**：`HashAggregation::noMoreInput` → `GroupingSet::noMoreInput`。
- **实现要点**：若 `inputSpiller_ != nullptr`，再调 **`GroupingSet::spill()`**，避免输出阶段仍持有大量 in-memory 状态（注释说明当前不支持输出过程中继续 spill）。

### 2.3 输出阶段 reclaim：从当前输出游标起 tail spill

- **条件**：`HashAggregation::reclaim` 中 `noMoreInput_` 为真、且非 distinct、且尚未进入「已 spill 且正在输出」的受限路径时。
- **调用**：`GroupingSet::spill(resultIterator_)` → 使用 **`AggregationOutputSpiller`**（单分区、`needSort()==false`），只 spill 迭代器之后未输出的行，然后 `table_->clear`。

---

## 3. Spill 类型与列序（`makeSpillType`）

**函数**：`GroupingSet::makeSpillType`（`GroupingSet.cpp`）。

**实现逻辑**：

1. `types` 先放入 **`RowContainer` 的 key 列类型** `rows->keyTypes()`（与哈希表/容器内 key 物理顺序一致）。
2. 再按 `rows->accumulators()` 顺序，对每个 accumulator 追加 **`accumulator.spillType()`**。
3. 合成 `ROW(names: s0,s1,...)`。

**含义**：

- 磁盘上每个 batch 的 **RowVector 子列顺序** = **[所有 grouping key 列] + [各聚合的中间 spill 类型列]**。
- `spillType()` 由各 `Aggregate` 定义，通常为 **intermediate** 形态（便于 final 阶段 `addSingleGroupIntermediateResults` 合并），与算子输出行的「最终结果类型」不一定相同。

**与计划输出列顺序的关系**：

- **Spill 文件列序** 绑定 **`RowContainer` / `makeSpillType()`**，不是直接绑定 `AggregationNode` 的 output 列序。
- 算子最终 `getOutput` 的列序仍由 **`extractGroups`** 等路径按 **`groupingKeyOutputProjections_`** 与 aggregate 顺序生成；**distinct + spill** 时另有 `prepareSpillResultWithoutAggregates` / `projectResult` 把 **容器 key 序** 映回 **plan 输出列序**（见第 8 节）。

**可 spill 时的 key 物理重排**（影响 RowContainer 中 key 列顺序，进而影响 spill 列序与排序键索引）：

- `HashAggregation::setupGroupingKeyChannelProjections`：若 `canSpill() && spillConfig()->prefixSortEnabled()`，会调用 `PrefixSortLayout::optimizeSortKeysOrder` 调整 **grouping key 在 RowContainer 中的顺序**，并建立 **`groupingKeyOutputProjections_`**，使 **输出给下游的列序仍与计划一致**，但 spill 文件内 key 子列顺序可能与「表达式在 input 中的顺序」不同。

---

## 4. 输入侧 Spill 写盘（`AggregationInputSpiller`）

### 4.1 创建 Spiller

**位置**：`GroupingSet::spill()`（首次需要时）。

- `inputSpiller_ = std::make_unique<AggregationInputSpiller>(rows, makeSpillType(), HashBitRange{...}, sortingKeys, spillConfig_, spillStats_)`。
- **`sortingKeys`**：`SpillState::makeSortingKeys` + 每个 key 一列默认 `CompareFlags`，列索引 **0 .. keyTypes.size()-1** 与 **`makeSpillType` 中 key 前缀列** 一一对应。
- **`HashBitRange`**：由 `spillConfig_->startPartitionBit` 与 `numPartitionBits` 决定如何把 64-bit hash 映射到分区号；用于 **跨文件仍可按分区独立恢复**。

### 4.2 `freezeAndExecute` 包裹 `inputSpiller_->spill()`

- **原因**：多分区并行 `writeSpill` 时，若聚合在 spill 过程中仍向 `HashStringAllocator` 申请/释放内存会不安全；**冻结** string allocator 后，spill 期间视为只读。

### 4.3 `SpillerBase::spill` 主循环与关键函数实现

**文件**：`velox/exec/Spiller.cpp`。

#### 4.3.1 总控：`SpillerBase::spill`

```71:86:velox/exec/Spiller.cpp
void SpillerBase::spill(const RowContainerIterator* startRowIter) {
  ...
  bool lastRun{false};
  do {
    lastRun = fillSpillRuns(&rowIter);
    runSpill(lastRun);
  } while (!lastRun);

  checkEmptySpillRuns();
}
```

- **`startRowIter != nullptr`**（`AggregationOutputSpiller`）：从 **`RowContainer`** 的指定游标开始列出待 spill 行，用于 **输出阶段 reclaim** 只写「尚未输出的尾部」。
- **`startRowIter == nullptr`**（`AggregationInputSpiller`）：从默认构造的迭代器开始，等价于 **扫描容器内全部仍占用的 group 行**。
- 外层 **`do/while`**：当 **`maxSpillRunRows_ > 0`** 时，**`fillSpillRuns`** 可能在中途因行数上限 **`break`**，此时返回 **`lastRun == false`**，但本批已收集的分区会通过 **`runSpill`** 写盘；下一轮 **`fillSpillRuns`** 继续推进 **`rowIter`**，直到 **`listRows` 返回 0** 得 **`lastRun == true`**。整段循环结束后 **`checkEmptySpillRuns`** 断言各分区 `SpillRun` 已清空。

#### 4.3.2 `fillSpillRuns`：行指针收集 + 哈希分区

```88:144:velox/exec/Spiller.cpp
bool SpillerBase::fillSpillRuns(RowContainerIterator* iterator) {
  ...
  constexpr int32_t kHashBatchSize = 4096;
  ...
  for (;;) {
    const auto numRows =
        container_->listRows(iterator, rows.size(), rows.data());
    if (numRows == 0) {
      lastRun = true;
      break;
    }
    ...
    for (auto i = 0; i < numRows; ++i) {
      const auto partitionNum =
          isSinglePartition ? 0 : bits_.partition(hashes[i]);
      auto& spillRun = createOrGetSpillRun(SpillPartitionId(partitionNum));
      spillRun.rows.push_back(rows[i]);
      spillRun.numBytes += container_->rowSize(rows[i]);
    }
    totalRows += numRows;
    if (maxSpillRunRows_ > 0 && totalRows >= maxSpillRunRows_) {
      break;
    }
  }
  markSeenPartitionsSpilled();
  ...
}
```

- **输入**：仍是 **`RowContainer`** 里的 **`char*`** 行；**`listRows`** 按分配迭代顺序取指针（见 `RowContainer.h` 中 `listRows` 语义），**不**在此步做列式展开。
- **分区**：多列 key 时循环 **`container_->hash(i, rowSet, ...)`** 更新 64-bit hash，再 **`HashBitRange::partition`** 取低若干位作为分区号；**单分区**（如 OutputSpiller）时 `partitionNum` 恒为 0。
- **`markSeenPartitionsSpilled`**：为本轮出现的每个分区 id 调 **`SpillState::setPartitionSpilled`**，后续 **`appendToPartition`** 才允许写入。

#### 4.3.3 `runSpill`：按分区并行写、结束 sorted run

```146:196:velox/exec/Spiller.cpp
void SpillerBase::runSpill(bool lastRun) {
  ...
  for (const auto& [id, spillRun] : spillRuns_) {
    ...
    writes.push_back(
        memory::createAsyncMemoryReclaimTask<SpillStatus>(
            [partitionId = id, this]() { return writeSpill(partitionId); }));
    if ((writes.size() > 1) && executor_ != nullptr) {
      executor_->add([source = writes.back()]() { source->prepare(); });
    }
  }
  ...
  for (auto& result : results) {
    ...
    run.clear();
    if (needSort()) {
      state_.finishFile(partitionId);
    }
  }
}
```

- 每个有行的分区一个 **`writeSpill`** 任务；若配置了 **`spillExecutor`** 且分区数 > 1，会 **`add`** 到线程池 **`prepare`**（实际写可在异步线程完成），主线程 **`move()`** 收结果并处理异常。
- **`needSort()` 为 true**（聚合 InputSpiller）时，**本轮 `writeSpill` 写完该分区 run 的全部行**后调用 **`finishFile`**：关闭当前页文件句柄，**下一轮 spill 同一分区会新开文件**，从而磁盘上形成 **多个「分区内有序 run」**，供后续 **`TreeOfLosers`** 与 **可能的多文件 `mergeSpillFiles`** 归并。

#### 4.3.4 `writeSpill`：单分区一次写盘的调度

```198:222:velox/exec/Spiller.cpp
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
  } ...
}
```

- **`ensureSorted(run)`**：见下一小节；保证本分区 **`run.rows`** 中指针顺序 **按 grouping key 全局有序**（在同一 run 内）。
- **`written`** 既是 **`extractSpillVector`** 的 **游标**（下一个未抽取的行在 `run.rows` 中的下标），循环结束时等于 **`run.rows.size()`**，**`SpillStatus::rowsWritten`** 与清空前的行数一致，供 **`runSpill`** 校验。

#### 4.3.5 `ensureSorted`：对「行指针数组」按 key 排序

```224:256:velox/exec/Spiller.cpp
void SpillerBase::ensureSorted(SpillRun& run) {
  if (run.sorted || !needSort()) {
    return;
  }
  ...
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
  ...
}
```

- **排序对象**：`SpillRun::rows` 是 **`std::vector<char*>`**，每个元素指向 **`RowContainer`** 内一行；**排序只重排行指针顺序**，**不移动**行缓冲在 arena 内的物理位置。
- **比较器**：**`RowContainer::compareRows(left, right, compareFlags_)`**，`compareFlags_` 由构造 **`SpillState`** 时传入的 **`SpillSortKey`**（聚合 spill 里即 **各 grouping key 列** 的 `CompareFlags`）展开，与 **`makeSpillType` 中 key 列顺序**一致。
- **`PrefixSort::sort`**：当 `SpillConfig` 带 **`prefixSortConfig`** 时走基数/前缀优化路径，仍是对 **同一批 `char*`** 排序。
- **`needSort()==false`**（OutputSpiller、Hash Join 等）直接返回，分区 run 保持 **listRows 顺序**。

#### 4.3.6 `extractSpillVector`：控制单批 `RowVector` 的行数与字节上限

```258:285:velox/exec/Spiller.cpp
int64_t SpillerBase::extractSpillVector(
    SpillRows& rows,
    int32_t maxRows,
    int64_t maxBytes,
    RowVectorPtr& spillVector,
    size_t& nextBatchIndex) {
  ...
  auto limit = std::min<size_t>(rows.size() - nextBatchIndex, maxRows);
  ...
  for (; numRows < limit; ++numRows) {
    bytes += container_->rowSize(rows[nextBatchIndex + numRows]);
    if (bytes > maxBytes) {
      ++numRows;
      break;
    }
  }
  extractSpill(folly::Range(&rows[nextBatchIndex], numRows), spillVector);
  nextBatchIndex += numRows;
  ...
}
```

- **默认 `maxRows=64`、`maxBytes=256KiB`**（与 `writeSpill` 中常量一致）：在 **预估行宽 `rowSize`** 累加超过上限时截断；若首行即超上限，**`++numRows; break`** 保证 **至少一行** 也会进入 **`extractSpill`**，避免死循环。
- **`spillVector`** 在多次调用间 **`prepareForReuse` / resize**（在 **`extractSpill`** 内），减少向 **`memory::spillMemoryPool()`** 反复分配。

#### 4.3.7 `extractSpill`：行存 → 列存（核心投影）

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

- **`rowType_`**：构造 **`AggregationInputSpiller`** 时传入的 **`GroupingSet::makeSpillType()`**，故 **`result->childAt` 列序** = **`columnTypes()`（key + 依赖列）** + **各 accumulator 的 `spillType` 列**。
- **前半循环**：**`extractColumn`** 把 **同一逻辑列** 从 **多行 `char*`** 抽到 **扁平 `VectorPtr`** 的一段连续索引（列存）。
- **后半循环**：**`extractForSpill`** 通常绑定 **`Aggregate::extractAccumulators`**，把 **行上 accumulator 内存布局** 解码为 **该聚合 intermediate 类型的向量**（仍是列存一行对齐）。
- **内存池**：新向量建在 **`spillMemoryPool()`**，与算子主池隔离，便于统计与回收。

#### 4.3.8 `SpillState::appendToPartition` 之后

- **`SpillWriter::write`**（`SpillFile.cpp`）：对 **`RowVector`** 走 **Presto** 序列化写入页文件；细节见 **§4.4**。

### 4.4 物理文件与序列化

**`SpillWriter`**（`SpillFile.cpp`）继承 **`serializer::SerializedPageFileWriter`**：

- 使用 **Presto VectorSerde**（`VectorSerde::Kind::kPresto`）与 `SpillConfig` 中的 **compression** 等选项。
- **`SpillState::appendToPartition`** 里用 `rows->estimateFlatSize()` 做统计与上限校验；实际写入由 **SerializedPage** 管线完成。

**读回对称路径**：`SpillReadFile` / `SerializedPageFileReader::nextBatch` → **`VectorStreamGroup::read(..., type_, ...)`**，其中 **`type_`** 即 spill 文件携带的 **`RowType`**（与 `makeSpillType` 一致），因此 **列类型与列数** 在读写两侧闭合。

### 4.5 Input spill 末尾

- **`sortedAggregations_->clear()`**（若存在）。
- **`table_->clear(true)`**：清空哈希表与主 `RowContainer`，释放已写出行的内存。

---

## 5. `finishSpill` 与分区文件集合

**调用点**：首次进入 **`GroupingSet::getOutputWithSpill`** 时（`outputSpillPartition_ == -1` 分支）。

**链**：`inputSpiller_->finishSpill(spillPartitionSet_)`（或 `outputSpiller_`）→ **`SpillerBase::finishSpill`**：

- **`finalizeSpill()`**：标记 spiller 生命周期结束。
- 遍历 **`SpillState::spilledPartitionIdSet()`**，对每个分区 **`state_.finish(partitionId)`** 得到 **`SpillFiles`**（多文件路径 + 元数据），装入 **`SpillPartitionSet`**；同一逻辑分区若多次出现则 **`addFiles`** 合并文件列表。

此后 **内存中主表已空**（此前 `getOutputWithSpill` 内也会 **`table_->clear`**），结果完全依赖 **磁盘上的有序 run + merge**。

---

## 6. 多文件合并与有序 Reader（`SpillPartition::createOrderedReader`）

**函数**：`SpillPartition::createOrderedReader(const SpillConfig&, ...)`（`Spill.cpp`）。

- 若 **`numMaxMergeFiles == 0` 或 `files_.size() <= numMaxMergeFiles`**：直接 **`createOrderedReaderInternal`**：每个文件一个 **`FileSpillMergeStream`**（内部 **`SpillReadFile::nextBatch`**），再 **`TreeOfLosers<SpillMergeStream>`** 做全局有序流。
- 否则：按文件大小建堆，反复 **`mergeSpillFiles`**：多路 **`TreeOfLosers`** + **`gatherMerge`** 写出 **单个合并文件**，直到文件数降到阈值以下，再 **`createOrderedReaderInternal`**。

**与 HashAggregation 的衔接**：`GroupingSet::prepareNextSpillPartitionOutput` 中 **`merge_ = spillPartition->createOrderedReader(*spillConfig_, pool_, spillStats_)`**。

---

## 7. Merge 输出：列式 spill 批次中的逻辑行 → 行式 `mergeRows_` → 列式 result

**术语**：「列式 spill」= 反序列化后的 **`RowVector` 批次**；其中 **`SpillMergeStream::currentIndex()`** 指向 **一条逻辑行**（各子列同一行号）。「行式 `mergeRows_`」= **`RowContainer`** 里为 **当前输出 batch 内每个 distinct merge key** 分配的一行 **`char* mergeState_`**，上存 key + 累加器。

**入口**：`GroupingSet::getOutput` 在 **`hasSpilled()`** 时进入 **`getOutputWithSpill`** → **`mergeNext`** →（非 distinct）**`mergeNextWithAggregates`**。

下文按 **时间阶段** 说明，并标出 **行存 / 列存** 在何函数发生转换。

---

### 7.0 阶段总览（与行列形态）

| 阶段 | 主要函数 | 数据形态 |
|------|-----------|----------|
| A. 首次进入输出 | `getOutputWithSpill`（`outputSpillPartition_ == -1`） | 建空 **`mergeRows_`（行存容器）**；`finishSpill` 得到 **`SpillPartitionSet`**（元数据，指向磁盘列存文件）。 |
| B. 打开一分区有序流 | `prepareNextSpillPartitionOutput` | **`merge_` = `TreeOfLosers<SpillMergeStream>`**：多个文件流，每次 **`nextBatch`** 拉一块 **列存 `RowVector`**。 |
| C. 按全局 key 顺序消费 | `mergeNextWithAggregates` + **`merge_->nextWithEquals`** | 比较与当前游标在 **列向量** 上完成；合并写入 **`mergeState_`（行）**。 |
| D. 刷出 batch | `extractSpillResult` → **`extractGroups`** | **`mergeRows_` 多行 `char*`** → 输出 **`RowVector`（列存）**。 |
| E. 复位 | `clearMergeRows` | 清空 **`mergeRows_`** 与 sorted agg 辅助内存。 |

---

### 7.1 阶段 A：`getOutputWithSpill` 首次初始化（`GroupingSet.cpp`）

仅当 **`outputSpillPartition_ == -1`** 时执行一次（见 `velox/exec/GroupingSet.cpp` 1086–1127 行）：

```1082:1130:velox/exec/GroupingSet.cpp
bool GroupingSet::getOutputWithSpill(...) {
  if (outputSpillPartition_ == -1) {
    ...
    if (!isDistinct()) {
      mergeArgs_.resize(1);
      std::vector<TypePtr> keyTypes;
      for (auto& hasher : table_->hashers()) {
        keyTypes.push_back(hasher->type());
      }

      mergeRows_ = std::make_unique<RowContainer>(
          keyTypes,
          !ignoreNullKeys_,
          accumulators(false),
          ...
          pool_);

      initializeAggregates(aggregates_, *mergeRows_, false);
    }
    VELOX_CHECK_EQ(table_->rows()->numRows(), 0);
    table_->clear(/*freeTable=*/true);

    ...
    if (inputSpiller_ != nullptr) {
      inputSpiller_->finishSpill(spillPartitionSet_);
    } else {
      outputSpiller_->finishSpill(spillPartitionSet_);
    }
    removeEmptyPartitions(spillPartitionSet_);

    if (!prepareNextSpillPartitionOutput()) {
      ...
      return false;
    }
  }
  VELOX_CHECK_NOT_NULL(merge_);
  return mergeNext(maxOutputRows, maxOutputBytes, result);
}
```

**要点**：

- **`mergeRows_`**：与主表分离的 **`RowContainer`**，schema 为 **最终 key 类型** + **`accumulators(false)`**（final 累加器布局），用于 **仅容纳「当前正在构造输出 batch」的 groups**，不是全量哈希表。
- **`table_->clear(true)`**：主聚合表已空；后续结果只来自 **spill 读 + merge**。
- **`finishSpill`**：把各分区 **`SpillFiles`** 收进 **`spillPartitionSet_`**（见第 5 节）。

**本阶段行列**：尚未读磁盘 batch；**仅分配行存容器 `mergeRows_`**。

---

### 7.2 阶段 B：`prepareNextSpillPartitionOutput` — 建 `TreeOfLosers`

```1133:1145:velox/exec/GroupingSet.cpp
bool GroupingSet::prepareNextSpillPartitionOutput() {
  ...
  auto it = spillPartitionSet_.begin();
  outputSpillPartition_ = it->first.partitionNumber();
  merge_ = it->second->createOrderedReader(*spillConfig_, pool_, spillStats_);
  spillPartitionSet_.erase(it);
  return true;
}
```

- **`createOrderedReader`**（`Spill.cpp`）：可能先 **`mergeSpillFiles`** 降文件数，再为每文件建 **`FileSpillMergeStream`**，最后 **`std::make_unique<TreeOfLosers<SpillMergeStream>>`**。
- **`FileSpillMergeStream::nextBatch`**：`SpillReadFile::nextBatch(rowVector_)` → **`VectorStreamGroup::read`**，得到 **一块列存 `RowVector`**，与 spill 写入时 **`rowType_`（`makeSpillType`）** 一致。

**本阶段行列**：**磁盘页 → 列存 `RowVector`**；merge 树持有多个这样的流。

---

### 7.3 阶段 C：`merge_->nextWithEquals` — 全局有序 +「下一元素是否同 key」

**比较（列存上完成）**：`SpillMergeStream::compare` 直接用 **两个流当前行的 key 子列** 做 `Vector::compare`（同一行号在各自 `rowVector_` 上）：

```101:118:velox/exec/Spill.cpp
int32_t SpillMergeStream::compare(const MergeStream& other) const {
  ...
  const auto& children = rowVector_->children();
  const auto& otherChildren = otherStream.current().children();
  for (const auto& [key, compareFlags] : sortingKeys()) {
    const auto result = children[key]
                            ->compare(
                                otherChildren[key].get(),
                                index_,
                                otherStream.index_,
                                compareFlags)
                            .value();
    if (result != 0) {
      return result;
    }
  }
  return 0;
}
```

**推进（列存 batch 内游标 + 换批）**：`pop` 递增 **`index_`**，越界则 **`setNextBatch()`** 读下一块 **`RowVector`**：

```94:99:velox/exec/Spill.cpp
void SpillMergeStream::pop() {
  VELOX_CHECK(!closed_);
  if (++index_ >= size_) {
    setNextBatch();
  }
}
```

**`setNextBatch`**（`Spill.h`）：`nextBatch()` 填好 **`rowVector_`** 后，若已建 **`decoded_`**，对 **每个已解码列索引** 调 **`DecodedVector::decode(*rowVector_->childAt(i), rows_)`**，**`rows_`** 为覆盖 **整批** 的 **`SelectivityVector`** —— 后续 **`store` / compare** 用 **展平后的列视图**。

```117:125:velox/exec/Spill.h
  void setNextBatch() {
    nextBatch();
    if (!decoded_.empty()) {
      ensureRows();
      for (auto i = 0; i < decoded_.size(); ++i) {
        decoded_[i].decode(*rowVector_->childAt(i), rows_);
      }
    }
  }
```

**`TreeOfLosers::nextWithEquals`**（`velox/common/base/TreeOfLosers.h`）：在 k 路有序流上返回 **当前全局最小流** `Stream*`，以及 **`bool`**：**是否存在另一条流与当前首元素 key 相等**（用于「同一 group 多行 intermediate 连续合并」）。

```126:152:velox/common/base/TreeOfLosers.h
  /// Returns the stream with the lowest first element and a flag that is true
  /// if there is another equal value to come from some other stream. ...
  std::pair<Stream*, bool> nextWithEquals() {
    ...
    return lastIndex_ == kEmpty
        ? std::make_pair(nullptr, false)
        : std::make_pair(streams_[lastIndex_].get(), result.second);
  }
```

内部 `firstWithEquals` / `propagateWithEquals` 使用 **`streams_[left]->compare(*streams_[right])`**（见同文件 225–229 行附近），即 **完全在 spill 列存 batch 语义下比较 key**。

---

### 7.4 阶段 C（续）：`mergeNextWithAggregates` — 把列存逻辑行 fold 进「行存」group

**源码**：`velox/exec/GroupingSet.cpp` 1158–1198 行。

```1158:1198:velox/exec/GroupingSet.cpp
bool GroupingSet::mergeNextWithAggregates(...) {
  bool nextKeyIsEqual{false};
  for (;;) {
    const auto next = merge_->nextWithEquals();
    if (next.first == nullptr) {
      extractSpillResult(result);
      if (result->size() > 0) {
        return true;
      }
      ...
      if (!prepareNextSpillPartitionOutput()) {
        ...
        return false;
      }
      continue;
    }
    if (!nextKeyIsEqual) {
      mergeState_ = mergeRows_->newRow();
      initializeRow(*next.first, mergeState_);
    }
    updateRow(*next.first, mergeState_);
    nextKeyIsEqual = next.second;
    next.first->pop();

    if (!nextKeyIsEqual &&
        ((mergeRows_->numRows() >= maxOutputRows) ||
         (mergeRowBytes() >= maxOutputBytes))) {
      extractSpillResult(result);
      return true;
    }
  }
}
```

**状态变量 `nextKeyIsEqual`**：

- 取值来自 **上一轮** `nextWithEquals()` 返回的 **`next.second`**（在循环末尾执行 **`nextKeyIsEqual = next.second`** 后供 **下一轮** 使用）。
- **`TreeOfLosers::nextWithEquals` 的 `second`**：在 `TreeOfLosers.h` 注释中说明为 —— 在选出当前最小流后，**是否还存在另一条流，其队首元素与当前队首 key 相等**（多路有序归并里「同 key 尚未耗尽」的信号）。
- 因此 **`!nextKeyIsEqual`** 时表示 **新逻辑 key**：分配 **`mergeState_ = mergeRows_->newRow()`** 并 **`initializeRow`**；若为 **true**，则 **同一 `mergeState_`** 上继续 **`updateRow`**，合并来自 **不同文件/不同 batch** 的 **同 key intermediate 行**（仍全是 **列存输入 → 行存累加**）。

**行列转换（本循环核心）**：

1. **输入**：`next.first` 指向 **`SpillMergeStream`**，**`current()`** 为 **列存 `RowVector`**，**`currentIndex()`** 为当前逻辑行号。
2. **新 group**：**`initializeRow`** —— key：**列存 decoded → 行存 `mergeRows_` 一行**；累加器：**`initializeNewGroups`** 在行上初始化。
3. **同一 key 下一行**：**`updateRow`** —— **列存 intermediate 子列** + **单行 `SelectivityVector`** → **`addSingleGroupIntermediateResults`** 写入 **行存 `mergeState_`**。
4. **`pop()`**：在 **列存流** 上前进一行 / 换批；**不**修改 `mergeRows_` 中已写入内容。

**批量上限**：**`mergeRows_->numRows()`** 或 **`mergeRowBytes()`**（含 sorted agg 额外内存，见 `mergeRowBytes` 1201–1218 行）触达时 **`extractSpillResult`** 刷 batch。

---

### 7.5 `initializeRow` — 列存 key → 行存 key + 行上 init agg

```1352:1374:velox/exec/GroupingSet.cpp
void GroupingSet::initializeRow(SpillMergeStream& stream, char* row) {
  for (auto i = 0; i < keyChannels_.size(); ++i) {
    mergeRows_->store(stream.decoded(i), stream.currentIndex(), mergeState_, i);
  }
  vector_size_t zero = 0;
  for (auto i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }
    if (!aggregates_[i].distinct) {
      aggregates_[i].function->initializeNewGroups(
          &row, folly::Range<const vector_size_t*>(&zero, 1));
    } else {
      distinctAggregations_[i]->initializeNewGroups(
          &row, folly::Range<const vector_size_t*>(&zero, 1));
    }
  }

  if (sortedAggregations_ != nullptr) {
    sortedAggregations_->initializeNewGroups(
        &row, folly::Range<const vector_size_t*>(&zero, 1));
  }
}
```

- **`mergeRows_->store(decoded, rowIndex, mergeState_, columnIndex)`**：把 **DecodedVector 在 `rowIndex` 处的一格值** 写入 **`mergeState_` 行**的第 **`i` 个 key 列槽** → **明确的列→行**。
- **`initializeNewGroups(&row, {0})`**：在 **该行 `char*`** 上初始化各 **accumulator 内存布局**（仍为行存）。

---

### 7.6 `updateRow` — 列存 intermediate 列 → 行存 accumulator

```1403:1437:velox/exec/GroupingSet.cpp
void GroupingSet::updateRow(SpillMergeStream& input, char* row) {
  ...
  mergeSelection_.setValid(input.currentIndex(), true);
  mergeSelection_.updateBounds();
  for (auto i = 0; i < aggregates_.size(); ++i) {
    if (!aggregates_[i].sortingKeys.empty()) {
      continue;
    }
    mergeArgs_[0] = input.current().childAt(i + keyChannels_.size());
    aggregates_[i].function->addSingleGroupIntermediateResults(
        row, mergeSelection_, mergeArgs_, false);
  }
  mergeSelection_.setValid(input.currentIndex(), false);

  auto sortOrDistinctAggIndex = aggregates_.size() + keyChannels_.size();
  if (sortedAggregations_ != nullptr) {
    const auto& vector = input.current().childAt(sortOrDistinctAggIndex);
    sortedAggregations_->addSingleGroupSpillInput(
        row, vector, input.currentIndex());
    ++sortOrDistinctAggIndex;
  }

  for (const auto& distinctAgg : distinctAggregations_) {
    if (distinctAgg != nullptr) {
      distinctAgg->addSingleGroupSpillInput(
          row,
          input.current().childAt(sortOrDistinctAggIndex),
          input.currentIndex());
      ++sortOrDistinctAggIndex;
    }
  }
}
```

- **`mergeArgs_[0]`**：整列 **`VectorPtr`**（**列存**），与正常 **`addIntermediateResults(groups, rows, args)`** 类似，但这里 **目标只有一个 group**：**`row`** + **`mergeSelection_` 只选中 `currentIndex()` 一行**。
- **语义**：从 **spill 文件 schema** 中第 **`keyChannels_.size() + i`** 子列读取 **一行 intermediate**，合并进 **`row` 上第 i 个聚合的累加器** → **列→行**。
- **sorted / distinct**：子列下标从 **`aggregates_.size() + keyChannels_.size()`** 起顺序递增，与 **`makeSpillType` / extractSpill** 写出顺序一致。

---

### 7.7 阶段 D：`extractSpillResult` — 行存多 group → 列存输出 `result`

```1376:1386:velox/exec/GroupingSet.cpp
void GroupingSet::extractSpillResult(const RowVectorPtr& result) {
  std::vector<char*> rows(mergeRows_->numRows());
  RowContainerIterator iter;
  if (!rows.empty()) {
    mergeRows_->listRows(
        &iter, rows.size(), RowContainer::kUnlimited, rows.data());
  }
  extractGroups(
      mergeRows_.get(), folly::Range<char**>(rows.data(), rows.size()), result);
  clearMergeRows();
}
```

- **`listRows`**：收集 **`mergeRows_` 中当前所有 `char*`**（**行存句柄数组**）。
- **`extractGroups`**（同文件 776–817 行）：对 key 调 **`rowContainer->extractColumn(..., groupingKeyOutputProjections_[i], keyVector)`**；对聚合调 **`extractValues` / `extractAccumulators`**（视 **`isPartial_`**）→ **多行 `char*` 投影为输出 `RowVector` 各子列** → **行→列**。

---

### 7.8 阶段 E：`clearMergeRows`

```1388:1401:velox/exec/GroupingSet.cpp
void GroupingSet::clearMergeRows() {
  mergeRows_->clear();
  if (sortedAggregations_ != nullptr) {
    sortedAggregations_->clear();
    if (table_ != nullptr) {
      table_->rows()->stringAllocator().clear();
    } else {
      stringAllocator_.clear();
    }
  }
}
```

清空 **本输出 batch** 的 merge 状态，为下一批 **`mergeNextWithAggregates`** 或下一 **`prepareNextSpillPartitionOutput`** 复用内存。

---

### 7.9 小结：Merge 路径上的三次「行 / 列」边界

1. **磁盘 → 内存**：**页 / Presto 序列化 → `RowVector`（列存）**（`FileSpillMergeStream::nextBatch`）。
2. **消费 spill 行 → 累加**：**`RowVector` + `currentIndex` + `DecodedVector`（列）** → **`mergeRows_->store` + `addSingleGroupIntermediateResults`（行 `char*`）**。
3. **输出给下游**：**`mergeRows_` 多 `char*`** → **`extractGroups` → `result` `RowVector`（列存）**。

---

## 8. Distinct + Spill 的列序与 `gatherCopy`

**场景**：`isDistinct()` 且无普通聚合列时，spill 仍以 **key 列** 为主；merge 使用 **`mergeNextWithoutAggregates`**。

**额外逻辑**（`GroupingSet.cpp`）：

- **`prepareSpillResultWithoutAggregates`**：构造/复用 **`spillResultWithoutAggregates_`**，其 **子列名为最终 `result` 类型中的列名**，但 **物理子列顺序仍为 `table_->rows()->keyTypes()` 顺序**；通过 **`groupingKeyOutputProjections_`** 把 **`result` 的子列 move 到** 内部向量的对应槽位。
- **`gatherCopy`**：从各 **`SpillMergeStream::current()`** 按行索引拷贝到 **`spillResultWithoutAggregates_`**。
- **`projectResult(result)`**：把内部缓冲区按 **`groupingKeyOutputProjections_`** 映回 **`result` 子列**，并 **`resize`**。

这里显式处理了 **「RowContainer / spill 文件 key 列序」与「计划输出列序」** 可能不一致（尤其 **prefix sort 重排 key**）时的 **重排与复用 buffer**。

**Distinct stream id**：`numDistinctSpillFilesPerPartition_` 与 **`stream->id()`** 比较，用于区分「此前已输出过的 distinct 流」与「新 distinct」，避免重复输出（见 `mergeNextWithoutAggregates` 注释）。

---

## 9. 输出侧 Spill（`AggregationOutputSpiller`）的差异

| 项目 | InputSpiller | OutputSpiller |
|------|----------------|----------------|
| **分区** | `HashBitRange` 多分区 | `HashBitRange{}`，单分区 |
| **排序** | `needSort()==true`，分区 run 内排序 + `finishFile` 分 run | `needSort()==false` |
| **`runSpill` 末尾** | 仅 `needSort` 时在每轮 `runSpill` 内 `finishFile` | **`lastRun` 时对所有分区 `finishFile`**（`GroupingSet.cpp` `AggregationOutputSpiller::runSpill`） |
| **触发** | `GroupingSet::spill()` / reclaim 输入阶段 | `GroupingSet::spill(RowContainerIterator)` |

**行列转换** 与 Input 相同：**`extractSpill`** 将 **`RowContainer`** 行变为 **`RowVector`** 再写盘；读回仍走 **Presto 反序列化**。

---

## 10. 端到端调用链（展开版）

下列调用链均可在对应 `.cpp` 中按符号跳转；**缩进表示调用深度**。

### 10.1 正常聚合输入（列存 → 行存，与 spill 并列理解）

**文件**：`velox/exec/HashAggregation.cpp` → `velox/exec/GroupingSet.cpp`。

```
HashAggregation::addInput(input: RowVectorPtr)     // 列存 batch
  → GroupingSet::addInput / addInputForActiveRows
       → GroupingSet::ensureInputFits(input)      // 可能间接触发 reclaim → spill（§2）
       → BaseHashTable::prepareForGroupProbe(...)
       → BaseHashTable::groupProbe(...)            // 得到每输入行对应 group 的 char*（行存状态槽）
       → [各聚合] populateTempVectors(i, input)   // 从 input 取子列 → tempVectors_（仍列存）
       → Aggregate::addRawInput / addIntermediateResults(groups, rows, tempVectors_, ...)
            // groups[i] 为 RowContainer 行指针：列存输入 → 行存累加器
```

### 10.2 输入阶段 spill + 清表（reclaim，仍在上游送数据时）

**文件**：`velox/exec/Operator.cpp`（`Operator::MemoryReclaimer::reclaim`）→ `HashAggregation.cpp` → `GroupingSet.cpp` → `Spiller.cpp` → `Spill.cpp` / `SpillFile.cpp`。

```
Operator::MemoryReclaimer::reclaim(pool, targetBytes, ...)
  → HashAggregation::reclaim(targetBytes, stats)    // noMoreInput_ == false 分支
       → GroupingSet::spill()
            → [首次] std::make_unique<AggregationInputSpiller>(
                    table_->rows(), makeSpillType(), HashBitRange{...},
                    SpillState::makeSortingKeys(...), spillConfig_, spillStats_)
            → RowContainer::stringAllocator().freezeAndExecute([&] {
                   inputSpiller_->spill();
               })
                 → AggregationInputSpiller::spill()
                      → SpillerBase::spill(/*start=*/nullptr)
                           ┌─ 循环直到 lastRun ─────────────────────────────────┐
                           │ SpillerBase::fillSpillRuns(&rowIter)                │
                           │   → RowContainer::listRows(...)                   │
                           │   → RowContainer::hash(...)                         │
                           │   → HashBitRange::partition(...)                  │
                           │   → markSeenPartitionsSpilled / setPartitionSpilled│
                           │ SpillerBase::runSpill(lastRun)                     │
                           │   → [每分区异步] SpillerBase::writeSpill(id)       │
                           │        → SpillerBase::ensureSorted(run)            │
                           │        → SpillerBase::extractSpillVector(...)      │
                           │             → SpillerBase::extractSpill(...)       │
                           │                  → RowContainer::extractColumn(...)│
                           │                  → Accumulator::extractForSpill(...)│
                           │        → SpillState::appendToPartition(id, batch)  │
                           │             → SpillWriter::write(...)              │
                           │   → [needSort] SpillState::finishFile(id)           │
                           └────────────────────────────────────────────────────┘
            → [distinct 首 spill] 填充 numDistinctSpillFilesPerPartition_
            → sortedAggregations_->clear()  // 若有
            → BaseHashTable::clear(true)     // 释放哈希表 + RowContainer 行
```

### 10.3 结束输入补 spill

```
HashAggregation::noMoreInput
  → Operator::noMoreInput
  → GroupingSet::noMoreInput
       → [若 inputSpiller_ 已存在] GroupingSet::spill()   // 同 10.2 内层 spiller 路径
       → GroupingSet::ensureOutputFits()
```

### 10.4 首次基于 spill 的输出（finishSpill + 有序读 + merge）

**文件**：`HashAggregation.cpp` → `GroupingSet.cpp` → `Spill.cpp` → `SpillMergeStream` / `TreeOfLosers`。

```
HashAggregation::getOutput
  → GroupingSet::getOutput(maxRows, maxBytes, iterator, result)
       → [hasSpilled()] GroupingSet::getOutputWithSpill(...)
            → [仅首次 outputSpillPartition_ == -1]
            │    → mergeRows_ = std::make_unique<RowContainer>(keyTypes, ..., accumulators(false), ...)
            │    → initializeAggregates(aggregates_, *mergeRows_, false)
            │    → table_->clear(true)   // 主表已空
            │    → inputSpiller_->finishSpill(spillPartitionSet_)
            │         → SpillerBase::finalizeSpill()
            │         → SpillState::finish(partitionId)  // 每分区 → SpillFiles
            │    → removeEmptyPartitions(spillPartitionSet_)
            │    → GroupingSet::prepareNextSpillPartitionOutput()
            │         → SpillPartition::createOrderedReader(spillConfig, pool, stats)
            │              → [文件数 > numMaxMergeFiles] mergeSpillFiles(...)  // Spill.cpp
            │              → SpillPartition::createOrderedReaderInternal(...)
            │                   → FileSpillMergeStream::create(SpillReadFile::create(...))
            │                   → std::make_unique<TreeOfLosers<SpillMergeStream>>
            → GroupingSet::mergeNext / mergeNextWithAggregates
                 → merge_->nextWithEquals()
                 → GroupingSet::initializeRow(stream, mergeState_)   // store + init agg
                 → GroupingSet::updateRow(stream, mergeState_)      // addSingleGroupIntermediateResults
                 → GroupingSet::extractSpillResult(result)
                      → mergeRows_->listRows(...)
                      → GroupingSet::extractGroups(mergeRows_, ..., result)
                 → GroupingSet::clearMergeRows()
```

### 10.5 输出阶段 tail spill（`AggregationOutputSpiller`）

```
HashAggregation::reclaim   // noMoreInput_ == true，非 distinct，且允许 reclaim
  → GroupingSet::spill(resultIterator_)
       → std::make_unique<AggregationOutputSpiller>(rows, makeSpillType(), spillConfig_, spillStats_)
       → RowContainer::stringAllocator().freezeAndExecute([&] {
              outputSpiller_->spill(rowIterator);
          })
             → SpillerBase::spill(&rowIterator)     // 从迭代器起只列出尚未输出的行
             → AggregationOutputSpiller::runSpill(lastRun)
                  → SpillerBase::runSpill(lastRun)
                  → [lastRun] 对所有分区 state_.finishFile(partitionId)
       → BaseHashTable::clear(true)
```

---

## 11. 小结：行列与类型在 spill 中的锚点

1. **Spill 文件 schema** = `makeSpillType()` = **`RowContainer` key 列顺序** + **各 `Accumulator::spillType()`**。
2. **写盘**：`RowContainer` 行 → **`extractColumn` + `extractForSpill`** → **`RowVector`** → **Presto 序列化页文件**。
3. **读盘**：页文件 → **`RowVector`（同 schema）** → **`DecodedVector`** → **`mergeRows_->store`（keys）** + **`addSingleGroupIntermediateResults`（aggs）** → 行式 merge 状态。
4. **算子输出列**：仍由 **`extractGroups`** 的 **`groupingKeyOutputProjections_`** 与聚合 **`extractValues`/`extractAccumulators`** 决定；distinct spill 额外经 **`projectResult`** 对齐计划列序。
5. **Prefix sort 优化** 可能改变 **RowContainer 内 key 物理顺序**，但不改变 **通过 `groupingKeyOutputProjections_` 映到输出** 的语义；**spill 文件中的 key 列顺序** 随 **RowContainer key 顺序**。

---

## 12. 参考配置（与 `DriverCtx::makeSpillConfig` 对齐）

下列配置影响分区位数、单 run 行数、合并文件数、排序与 IO 等，可在 `velox/core/QueryConfig.h` 与 `velox/common/base/SpillConfig.h` 中查阅具体字段名与默认值：

- `spillStartPartitionBit` / `spillNumPartitionBits`
- `maxSpillRunRows`
- `maxSpillFileSize`、`spillWriteBufferSize`、`spillReadBufferSize`
- `spillNumMaxMergeFiles`（触发 `mergeSpillFiles`）
- `spillPrefixSortEnabled`（分组键物理重排 + `PrefixSort` spill 排序）
- `spillCompressionKind`、`spillFileCreateConfig`

文档 **`velox/docs/develop/spilling.rst`** 提供算法级背景图与说明；本文侧重 **HashAggregation + GroupingSet 源码级路径与行列转换**。
