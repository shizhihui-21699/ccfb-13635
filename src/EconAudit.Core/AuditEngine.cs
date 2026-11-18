namespace EconAudit.Core;

/// <summary>物品流转轨迹中的一跳(用于污染物品的流向追踪)。</summary>
public sealed record Hop(DateTimeOffset Ts, string? From, string? To, string OpId, FlowChannel Channel);

/// <summary>一个滚动对账窗口的汇总结果。</summary>
public sealed record WindowReport(
    DateTimeOffset Start,
    DateTimeOffset End,
    int EventCount,
    IReadOnlyDictionary<string, long> Minted,        // 系统产出(按物品类型)
    IReadOnlyDictionary<string, long> Consumed,      // 系统消耗
    IReadOnlyDictionary<string, long> Unexplained,   // 无法解释的新增(守恒被打破的量)
    IReadOnlyList<Anomaly> Anomalies);

/// <summary>
/// 经济守恒审计引擎。三层对账:
///  1. 实时逐笔:每条事件到达即校验所有权与余额,亚秒级发现复制;
///  2. 滚动窗口:按固定窗口汇总"净变动 == 系统产出 − 系统消耗",给出窗口级守恒证明;
///  3. 周期快照:服务器定期上报全量库存,核对台账,兜底"漏报事件"导致的盲区。
/// 引擎本身无状态存储依赖,生产环境可接 Kafka/事件总线,快照可接定时任务。
/// </summary>
public sealed class AuditEngine
{
    public const string GoldUid = "GOLD";
    public const string GoldType = "gold";

    private readonly TimeSpan _windowSize;
    private readonly Dictionary<string, FlowEvent> _seen = new();              // 幂等去重
    private readonly Dictionary<string, (string Holder, string Type)> _ownerOf = new(); // 物品UID -> 当前持有者
    private readonly Dictionary<string, long> _gold = new();                   // 持有者 -> 金币台账余额
    private readonly Dictionary<string, List<Hop>> _trails = new();            // 物品UID -> 流转轨迹
    private readonly HashSet<string> _taintedUids = new();                     // 被污染(涉复制)的物品
    private readonly Dictionary<string, long> _taintedGold = new();            // 持有者 -> 黑金额度
    private readonly List<Anomaly> _anomalies = new();
    private readonly List<WindowReport> _windows = new();
    private WindowBucket? _cur;

    public AuditEngine(TimeSpan? windowSize = null) => _windowSize = windowSize ?? TimeSpan.FromMinutes(1);

    public IReadOnlyList<Anomaly> Anomalies => _anomalies;
    public IReadOnlyList<WindowReport> Windows => _windows;
    public IReadOnlySet<string> TaintedUids => _taintedUids;
    public IReadOnlyDictionary<string, long> TaintedGold => _taintedGold;
    public IReadOnlyDictionary<string, long> GoldLedger => _gold;

    public IReadOnlyList<Hop> TrailOf(string uid) =>
        _trails.TryGetValue(uid, out var t) ? t : Array.Empty<Hop>();

    /// <summary>所有被污染物品及其完整流转轨迹。</summary>
    public IEnumerable<(string Uid, List<Hop> Trail)> TaintedTrails()
    {
        foreach (var (uid, trail) in _trails)
            if (_taintedUids.Contains(uid))
                yield return (uid, trail);
    }

    // ---------------------------------------------------------------- 实时逐笔

    public void Ingest(FlowEvent e)
    {
        // 幂等:同一 EventId 只入账一次;ID 相同内容不同本身就是异常
        if (_seen.TryGetValue(e.EventId, out var prev))
        {
            if (prev != e)
                Raise(AnomalyKind.DuplicateEvent, e.Ts, e.Channel, e.TxnId, e.OpId, e.ItemUid, e.ItemType,
                      e.Amount, Parties(e), $"事件ID {e.EventId} 重复提交且内容不一致(首次 seq={prev.Seq})");
            return;
        }
        _seen[e.EventId] = e;
        var bucket = Bucket(e.Ts);
        bucket.EventCount++;

        if (e.ItemUid == GoldUid) { IngestGold(e, bucket); return; }

        // 记录流转轨迹(无论是否正常,追踪要用)
        if (!_trails.TryGetValue(e.ItemUid, out var trail))
            _trails[e.ItemUid] = trail = new List<Hop>();
        trail.Add(new Hop(e.Ts, e.From, e.To, e.OpId, e.Channel));

        var hasOwner = _ownerOf.TryGetValue(e.ItemUid, out var cur);

        switch (e.From, e.To)
        {
            case (null, null):
                break;

            case (null, not null): // 入账
                if (e.Channel == FlowChannel.System && !hasOwner)
                {
                    _ownerOf[e.ItemUid] = (e.To!, e.ItemType);
                    bucket.Mint(e.ItemType, e.Amount);
                }
                else
                {
                    var detail = hasOwner
                        ? $"该UID当前由 [{cur.Holder}] 持有,却又凭空入账给 [{e.To}] —— 同一物品出现第二份"
                        : $"非系统渠道凭空入账给 [{e.To}],台账中无任何出处";
                    Raise(AnomalyKind.CreatedFromNothing, e.Ts, e.Channel, e.TxnId, e.OpId, e.ItemUid, e.ItemType,
                          e.Amount, hasOwner ? new[] { cur.Holder, e.To! } : new[] { e.To! }, detail);
                    bucket.UnexplainedDelta(e.ItemType, e.Amount);
                    _taintedUids.Add(e.ItemUid);
                    _ownerOf[e.ItemUid] = (e.To!, e.ItemType); // 台账跟随最新状态,继续追踪
                }
                break;

            case (not null, null): // 出账到虚空
                if (!hasOwner || cur.Holder != e.From)
                {
                    Raise(AnomalyKind.OrphanDebit, e.Ts, e.Channel, e.TxnId, e.OpId, e.ItemUid, e.ItemType,
                          e.Amount, Parties(e), $"[{e.From}] 并不持有该物品,却将其销毁 —— 存在未入账的复制体");
                    _taintedUids.Add(e.ItemUid);
                }
                else
                {
                    _ownerOf.Remove(e.ItemUid);
                    if (e.Channel == FlowChannel.System) bucket.Consume(e.ItemType, e.Amount);
                    else
                    {
                        Raise(AnomalyKind.UnaccountedSink, e.Ts, e.Channel, e.TxnId, e.OpId, e.ItemUid, e.ItemType,
                              e.Amount, Parties(e), $"[{e.From}] 经非系统渠道把物品销毁到虚空");
                        bucket.UnexplainedDelta(e.ItemType, -e.Amount);
                    }
                }
                break;

            default: // 持有者间转移
                if (!hasOwner)
                {
                    Raise(AnomalyKind.OrphanDebit, e.Ts, e.Channel, e.TxnId, e.OpId, e.ItemUid, e.ItemType,
                          e.Amount, Parties(e), $"台账中不存在该物品,[{e.From}] 却将其转给 [{e.To}] —— 存在未入账的复制体");
                    bucket.UnexplainedDelta(e.ItemType, e.Amount);
                    _taintedUids.Add(e.ItemUid);
                }
                else if (cur.Holder != e.From)
                {
                    Raise(AnomalyKind.OrphanDebit, e.Ts, e.Channel, e.TxnId, e.OpId, e.ItemUid, e.ItemType,
                          e.Amount, new[] { cur.Holder, e.From!, e.To! },
                          $"该UID实际由 [{cur.Holder}] 持有,[{e.From}] 又交出一份给 [{e.To}] —— 复制体在流动");
                    bucket.UnexplainedDelta(e.ItemType, e.Amount);
                    _taintedUids.Add(e.ItemUid);
                }
                _ownerOf[e.ItemUid] = (e.To!, e.ItemType);
                break;
        }
    }

    private void IngestGold(FlowEvent e, WindowBucket bucket)
    {
        if (e.From != null)
        {
            var bal = _gold.GetValueOrDefault(e.From) - e.Amount;
            _gold[e.From] = bal;
            if (bal < 0)
                Raise(AnomalyKind.NegativeBalance, e.Ts, e.Channel, e.TxnId, e.OpId, e.ItemUid, e.ItemType,
                      e.Amount, new[] { e.From }, $"[{e.From}] 付出 {e.Amount} 金币后台账余额为 {bal} —— 支出了不存在的金币");
            // 黑金随转账传播
            if (e.To != null && _taintedGold.TryGetValue(e.From, out var t) && t > 0)
            {
                var move = Math.Min(t, e.Amount);
                _taintedGold[e.From] = t - move;
                _taintedGold[e.To] = _taintedGold.GetValueOrDefault(e.To) + move;
            }
            if (e.To == null)
            {
                if (e.Channel == FlowChannel.System) bucket.Consume(GoldType, e.Amount);
                else
                {
                    Raise(AnomalyKind.UnaccountedSink, e.Ts, e.Channel, e.TxnId, e.OpId, e.ItemUid, e.ItemType,
                          e.Amount, Parties(e), $"[{e.From}] 经非系统渠道把 {e.Amount} 金币销毁到虚空");
                    bucket.UnexplainedDelta(GoldType, -e.Amount);
                }
            }
        }
        if (e.To != null)
        {
            _gold[e.To] = _gold.GetValueOrDefault(e.To) + e.Amount;
            if (e.From == null)
            {
                if (e.Channel == FlowChannel.System) bucket.Mint(GoldType, e.Amount);
                else
                {
                    Raise(AnomalyKind.CreatedFromNothing, e.Ts, e.Channel, e.TxnId, e.OpId, e.ItemUid, e.ItemType,
                          e.Amount, new[] { e.To }, $"非系统渠道凭空入账 {e.Amount} 金币给 [{e.To}]");
                    bucket.UnexplainedDelta(GoldType, e.Amount);
                    _taintedGold[e.To] = _taintedGold.GetValueOrDefault(e.To) + e.Amount;
                }
            }
        }
    }

    // ---------------------------------------------------------------- 周期快照对账

    /// <summary>
    /// 用服务器上报的某个持有者的全量快照核对台账。
    /// 兜底检测"服务器漏报事件"造成的盲区(例如静默的状态回滚)。
    /// </summary>
    public void ReconcileSnapshot(string holder, long gold, IReadOnlyList<(string Uid, string Type)> items,
                                  DateTimeOffset ts, string opId)
    {
        var bucket = Bucket(ts);

        var expected = _gold.GetValueOrDefault(holder);
        if (expected != gold)
        {
            var diff = gold - expected;
            Raise(AnomalyKind.SnapshotBalanceDrift, ts, FlowChannel.Snapshot, "snapshot", opId, GoldUid, GoldType,
                  Math.Abs(diff), new[] { holder },
                  $"[{holder}] 台账余额 {expected},快照实报 {gold},差额 {diff:+#;-#;0} —— 存在未上报的金币变更");
            bucket.UnexplainedDelta(GoldType, diff);
            if (diff > 0) _taintedGold[holder] = _taintedGold.GetValueOrDefault(holder) + diff;
            _gold[holder] = gold; // 台账向快照对齐,以便继续追踪
        }

        var snapUids = new HashSet<string>();
        foreach (var (uid, type) in items)
        {
            snapUids.Add(uid);
            if (!_ownerOf.TryGetValue(uid, out var cur))
            {
                Raise(AnomalyKind.ShadowItem, ts, FlowChannel.Snapshot, "snapshot", opId, uid, type, 1,
                      new[] { holder }, $"[{holder}] 快照中持有 {uid}({type}),但台账从未见过该物品 —— 漏报事件或非法生成");
                bucket.UnexplainedDelta(type, 1);
                _taintedUids.Add(uid);
                _ownerOf[uid] = (holder, type);
            }
            else if (cur.Holder != holder)
            {
                Raise(AnomalyKind.DoubleOwned, ts, FlowChannel.Snapshot, "snapshot", opId, uid, type, 1,
                      new[] { cur.Holder, holder },
                      $"[{holder}] 快照中持有 {uid}({type}),但台账显示持有者为 [{cur.Holder}] —— 同一物品两人各持一份");
                bucket.UnexplainedDelta(type, 1);
                _taintedUids.Add(uid);
            }
        }

        // 反向:台账认为该账号持有,快照里却没有 —— 物品凭空消失
        foreach (var (uid, cur) in _ownerOf)
            if (cur.Holder == holder && !snapUids.Contains(uid))
            {
                Raise(AnomalyKind.VanishedItem, ts, FlowChannel.Snapshot, "snapshot", opId, uid, cur.Type, 1,
                      new[] { holder }, $"台账显示 [{holder}] 持有 {uid}({cur.Type}),快照中却不存在 —— 物品凭空消失");
                bucket.UnexplainedDelta(cur.Type, -1);
            }
    }

    // ---------------------------------------------------------------- 滚动窗口

    /// <summary>关闭当前窗口,返回全部已关闭窗口的对账报告。</summary>
    public IReadOnlyList<WindowReport> Finalize()
    {
        CloseCurrent();
        return _windows;
    }

    private WindowBucket Bucket(DateTimeOffset ts)
    {
        _cur ??= new WindowBucket(Align(ts), Align(ts) + _windowSize);
        while (ts >= _cur.End)
        {
            var end = _cur.End;
            CloseCurrent();
            _cur = new WindowBucket(end, end + _windowSize);
        }
        return _cur;
    }

    private DateTimeOffset Align(DateTimeOffset ts) => new(ts.Ticks - ts.Ticks % _windowSize.Ticks, ts.Offset);

    private void CloseCurrent()
    {
        if (_cur is null) return;
        foreach (var (type, delta) in _cur.Unexplained)
        {
            if (delta == 0) continue;
            var a = new Anomaly(AnomalyKind.SupplyDrift, _cur.End, FlowChannel.System, "-", "-", "-", type,
                                Math.Abs(delta), Array.Empty<string>(),
                                $"窗口 [{_cur.Start:HH:mm:ss}~{_cur.End:HH:mm:ss}] 内 [{type}] 净变动 {delta:+#;-#;0}," +
                                $"与系统产出/消耗对不上 —— 该窗口内守恒被打破");
            _anomalies.Add(a);
            _cur.Anomalies.Add(a);
        }
        _windows.Add(_cur.ToReport());
        _cur = null;
    }

    // ---------------------------------------------------------------- 工具

    private void Raise(AnomalyKind kind, DateTimeOffset ts, FlowChannel channel, string txn, string op,
                       string uid, string type, long amount, IReadOnlyList<string> accounts, string detail)
    {
        var a = new Anomaly(kind, ts, channel, txn, op, uid, type, amount, accounts, detail);
        _anomalies.Add(a);
        Bucket(ts).Anomalies.Add(a);
    }

    private static string[] Parties(FlowEvent e) => (e.From, e.To) switch
    {
        (null, null) => Array.Empty<string>(),
        (null, var t) => new[] { t! },
        (var f, null) => new[] { f! },
        var (f, t) => new[] { f!, t! },
    };

    private sealed class WindowBucket
    {
        public readonly DateTimeOffset Start, End;
        public int EventCount;
        public readonly Dictionary<string, long> Minted = new();
        public readonly Dictionary<string, long> Consumed = new();
        public readonly Dictionary<string, long> Unexplained = new();
        public readonly List<Anomaly> Anomalies = new();

        public WindowBucket(DateTimeOffset start, DateTimeOffset end) { Start = start; End = end; }
        public void Mint(string t, long a) => Minted[t] = Minted.GetValueOrDefault(t) + a;
        public void Consume(string t, long a) => Consumed[t] = Consumed.GetValueOrDefault(t) + a;
        public void UnexplainedDelta(string t, long d) => Unexplained[t] = Unexplained.GetValueOrDefault(t) + d;
        public WindowReport ToReport() => new(Start, End, EventCount, Minted, Consumed, Unexplained, Anomalies);
    }
}
