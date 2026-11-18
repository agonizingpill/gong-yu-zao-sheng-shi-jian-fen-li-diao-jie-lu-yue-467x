# 002 分离任务恢复与留档

`separations` 增加中断恢复与留档所需的列。新建库由create_all直接建出；已有库执行：

```sql
ALTER TABLE separations
  ADD COLUMN snapshot JSON NULL COMMENT '入队时冻结的事件快照，新登记事件不混入',
  ADD COLUMN attempts INT NOT NULL DEFAULT 0 COMMENT '认领次数，大于1即中断后恢复执行',
  ADD COLUMN run_token VARCHAR(40) NOT NULL DEFAULT '' COMMENT 'fencing令牌，旧进程不能覆盖已恢复结果',
  ADD COLUMN recovered TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否经历过中断回收',
  ADD COLUMN heartbeat_at DATETIME NULL COMMENT '执行进程心跳，超时即被回收';
```

语义约定：

- `state` 新增取值 `stale`：源数据（频带事件）更新后，旧分离结果立即失效，仅留档备查，不能用于归因确认。
- 回收：worker每轮把 `state='running'` 且心跳超时（默认12秒，`SEPARATION_STALE_SECONDS` 可调）的任务退回 `queued` 并置 `recovered=1`，原任务按原快照接着处理，不新建任务。
- fencing：每次认领签发新 `run_token`，发布结果、心跳、标记失败均按 `id + run_token + state='running'` 条件更新；旧进程晚回来影响0行，不能覆盖已恢复的结果。
- 留档：worker先把结果写入 `separation-{id}.tmp`，库内发布成功后才转正为 `separation-{id}.json`；`GET /complaints/{id}/separations/{sid}/artifact` 与分离页读取同一份库内结果，并在留档文件缺失或不一致时以库内结果重写。
