# MACD Time Signal Bundle v1.3.0

这个目录现在已经整理成一套可运行的 `1.3.0` 推送系统。

当前主要包含这些交付物：

1. `macd_time_signal_scanner.py`
   Python A 股扫描器。基于 AKShare 拉取全市场或单只股票历史数据，输出最近的
   `LEFT_BOTTOM / LEFT_TOP / BUY1 / SELL1 / BUY2 / SELL2 / BUY3 / SELL3` 信号。

2. `macd_time_signal_telegram_push.py`
   完整推送主程序。支持盘中预警、收盘汇总、中文分类推送、去重、归档、状态记录、手动回放。

3. `macd_timeframe_backtest.py`
   多周期回测脚本。支持 `15m / 30m / 60m / 120m / 240m / 日线 / 周线 / 月线`，
   并把价格历史、回测批次、汇总结果、交易明细写入 SQLite。

4. `market_data_store.py` + `scripts/sync_market_data.py`
   本地行情库与同步脚本。负责把股票池快照、K 线、覆盖范围、同步日志正式落到本地 SQLite。

5. `macd_time_signal_indicator.pine`
   TradingView Pine Script v5 指标，可在图表上绘制同一套信号体系。

## 安装

```bash
pip install -r requirements.txt
```

## Python 扫描器

查看版本：

```bash
python macd_time_signal_scanner.py --version
```

扫描全市场：

```bash
python macd_time_signal_scanner.py \
  --start-date 20220101 \
  --end-date 20260310 \
  --adjust qfq \
  --period daily \
  --workers 4 \
  --min-amount 100000000 \
  --min-price 2 \
  --recent-bars 5
```

默认输出到当前项目下的：

```bash
outputs/a_share_signals_v1_3_0.csv
```

只测试单只股票：

```bash
python macd_time_signal_scanner.py \
  --symbol 000001 \
  --name 平安银行 \
  --start-date 20220101 \
  --end-date 20260310
```

常用参数：

- `--all-signals` 保留最近窗口内全部信号，而不是每只股票只取最新一条
- `--limit 200` 先在较小样本上试跑
- `--include-st` 包含 ST
- `--include-delisting` 包含退市相关名称
- `--output /absolute/or/relative/path.csv` 自定义输出文件

输出字段除了 `date/symbol/name/signal/close/score` 外，还包含不同信号对应的特征列，例如：
`votes`、`T_conf1_buy`、`R_2`、`T_pull3`、`B0`、`T_near0` 等。

## TradingView 指标

把 `macd_time_signal_indicator.pine` 全量复制到 TradingView 的 Pine Editor，保存后添加到图表即可。

图表标记：

- `LB` LEFT_BOTTOM
- `LT` LEFT_TOP
- `B1` BUY1
- `S1` SELL1
- `B2` BUY2
- `S2` SELL2
- `B3` BUY3
- `S3` SELL3

## Telegram 推送

先准备环境变量：

```bash
export TELEGRAM_BOT_TOKEN="your-bot-token"
export TELEGRAM_CHAT_ID="your-chat-id"
```

然后运行：

```bash
python macd_time_signal_telegram_push.py \
  --start-date 20220101 \
  --end-date 20260310 \
  --workers 4 \
  --recent-bars 5
```

默认系统能力：

- 全中文推送
- 分组为 `左侧 / 一买 / 二买 / 三买 / 一卖 / 二卖 / 三卖`
- 同一条信号按 `date + symbol + signal` 去重，避免重复推送
- 每次运行都会归档 CSV 到 `outputs/archive/`
- 每次运行都会生成 JSON 报告到 `reports/`
- 状态文件写入 `state/telegram_push_state.json`
- 默认只推送新信号；加 `--notify-empty` 才会在无新信号时发提示
- 内置锁文件，避免定时任务重叠运行
- `收盘汇总` 可以切到 `--data-source market-db`，直接从 `data/market_data.sqlite` 扫描，不再临时联网拉整市场日线

推送节奏：

- `盘中预警`：交易时段内多次扫描，只推送当天新增信号
- `收盘汇总`：收盘后先补本地行情库，再基于本地库汇总当天全部信号，即使盘中已经发过，也会单独汇总

注意：

- 当前信号引擎本身仍是 `日线级别`，不是 4 小时 K 线级别
- 现在新增的是推送频率与推送场景，不是改动 K 线周期

如果要使用仓库内包装脚本：

```bash
./scripts/run_telegram_push.sh
```

常用运维命令：

```bash
./scripts/run_intraday_alert.sh
./scripts/run_close_summary.sh
./scripts/replay_by_date.sh 2026-03-10
python3 scripts/push_status.py
./scripts/install_systemd.sh
```

`replay_by_date.sh` 用来补发某天结果，默认基于 `outputs/latest_scan.csv`，也可以传第二个参数指定归档 CSV。

## 多周期回测与数据库

回测脚本默认按 `A 股纯多头` 口径运行：

- `LEFT_BOTTOM / BUY1 / BUY2 / BUY3` 作为开仓信号
- `LEFT_TOP / SELL1 / SELL2 / SELL3` 作为平仓信号
- 默认使用 `反向卖点平仓`

示例：

```bash
python macd_timeframe_backtest.py \
  --exit-mode reverse \
  --minute-source baostock \
  --symbols 000001,600036,600519,300750,601318,000858,600900,002594,300059,601688,600276,688981 \
  --levels 15m,60m,120m,240m,daily,weekly,monthly \
  --output outputs/timeframe_backtest_reverse_symbols12_bao.csv \
  --summary-output outputs/timeframe_backtest_reverse_symbols12_bao_summary.csv \
  --db-path outputs/backtest_store.sqlite
```

如果要跑更大的样本，优先用并行入口：

```bash
python scripts/run_parallel_backtest.py \
  --limit 100 \
  --workers 6 \
  --levels 15m,60m,120m,240m \
  --output outputs/timeframe_backtest_reverse_limit100_focus_bao.csv \
  --summary-output outputs/timeframe_backtest_reverse_limit100_focus_bao_summary.csv
```

默认数据库：

```bash
outputs/backtest_store.sqlite
```

SQLite 里会保存这些表：

- `price_history`：各 symbol / level 的历史行情缓存
- `backtest_runs`：每次回测的参数和输出路径
- `backtest_summary`：各周期汇总指标
- `backtest_trades`：逐笔交易明细

反向卖点平仓汇总当前会附带：

- `avg_hold_bars`：平均持仓 K 线数
- `avg_hold_days`：平均持仓自然日
- `avg_flat_days`：两笔交易之间的平均空仓自然日
- `max_drawdown`：按逐笔复利序列估算的最大回撤
- `annual_signal_freq`：该周期在整组样本上的年化信号次数
- `annual_signal_freq_per_symbol`：平均每只股票每年的信号次数

当前分钟级长历史默认走 `baostock`，`120m / 240m` 由更细分钟线聚合生成。首轮建库会慢一些，后续同参数重跑会直接优先命中 SQLite 和本地缓存。

## 本地行情库

正式行情底库默认在：

```bash
data/market_data.sqlite
```

和回测库不同，这个库是给后续所有能力共用的基础数据层，当前会保存：

- `symbols`：股票主数据
- `universe_snapshots` / `universe_snapshot_items`：股票池快照
- `kline`：正式 K 线表
- `data_coverage`：每个 `symbol + level` 的覆盖范围
- `sync_runs` / `sync_run_items`：每次同步和逐标的执行记录

同步命令：

```bash
python scripts/sync_market_data.py \
  --start-date 20220101 \
  --end-date 20300101 \
  --levels 15m,60m,120m,240m,daily,weekly,monthly \
  --db-path data/market_data.sqlite
```

全市场分批同步时，建议先跑 `15m + daily`：

```bash
python scripts/sync_market_data.py \
  --levels 15m,daily \
  --batch-size 30 \
  --batch-index 1 \
  --db-path data/market_data.sqlite \
  --skip-bootstrap
```

然后把 `--batch-index` 依次改成 `2, 3, 4 ...` 往后推。

只同步指定股票：

```bash
python scripts/sync_market_data.py \
  --symbols 000001,600036,600519 \
  --levels 15m,60m,120m,240m,daily,weekly,monthly \
  --db-path data/market_data.sqlite
```

默认行为：

- 如果存在 `outputs/backtest_store.sqlite`，会先把历史缓存导入正式行情库
- 股票池会优先实时拉取，并缓存到 `data/universe_latest.csv`
- `15m` 是分钟级原始底线

日更脚本：

```bash
./scripts/run_daily_market_sync.sh
```

默认会补最近 `14` 天的 `15m,60m,120m,240m,daily`，用于收盘后的增量更新。
- `30m / 60m / 120m / 240m` 由 `15m` 聚合生成
- `daily / weekly / monthly` 通过 AKShare 同步
- 分钟线默认使用 `baostock`

所以现在建议的分工是：

- `data/market_data.sqlite`：正式行情底库
- `outputs/backtest_store.sqlite`：回测运行结果和临时价格缓存

## 发布

执行下面脚本会生成当前 `VERSION` 对应的发布目录和压缩包：

```bash
./scripts/release_v1_1.sh
```

产物会放到：

```bash
dist/macd_time_signal_v1.3.0/
dist/macd_time_signal_v1.3.0.tar.gz
```

## 说明

- Python 端默认使用 AKShare：
  - `stock_zh_a_spot_em()` 获取股票池
  - `stock_zh_a_hist()` 获取历史行情
- 默认配置是 `daily + qfq`
- Pine 脚本尽量保持与 Python 状态机语义一致
- 输出目录会自动创建，不再依赖原版里的 `/mnt/data/...`
