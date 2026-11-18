namespace EconAudit.Core;

/// <summary>物品流转发生的渠道(环节)。</summary>
public enum FlowChannel
{
    /// <summary>系统产出/消耗:任务奖励、NPC 商店、回收。这是唯一允许打破守恒的通道。</summary>
    System,
    Trade,
    Mail,
    Auction,
    Storage,
    /// <summary>掉线重连引发的状态恢复。</summary>
    Reconnect,
    /// <summary>周期快照对账(非流转,仅核对)。</summary>
    Snapshot,
}

/// <summary>
/// 一笔物品/货币流转事件。游戏服务器每改变一次物品归属就必须Emit一条。
/// From == null 表示来自虚空(产出),To == null 表示流向虚空(消耗)。
/// </summary>
public sealed record FlowEvent(
    string EventId,      // 全局唯一,用于幂等去重
    string TxnId,        // 所属事务(一次操作可能产生多条事件)
    long Seq,            // 服务器单调递增序号
    DateTimeOffset Ts,
    FlowChannel Channel,
    string OpId,         // 操作标识,如 trade#12 / mail-claim#7,用于定位问题环节
    string ItemUid,      // 物品唯一实例ID;货币固定为 AuditEngine.GoldUid
    string ItemType,     // 物品种类,如 iron-sword / gold
    long Amount,
    string? From,        // 原持有者(账号/容器,如 mail#3、storage:guild);null = 系统/虚空
    string? To);         // 新持有者;null = 系统/虚空

public enum AnomalyKind
{
    /// <summary>同一 EventId 重复提交且内容不一致。</summary>
    DuplicateEvent,
    /// <summary>非系统渠道凭空入账(无对应出账)。</summary>
    CreatedFromNothing,
    /// <summary>从一个并不持有该物品的对象手中交出物品 —— 复制体在流动。</summary>
    OrphanDebit,
    /// <summary>快照核对发现同一 UID 被两个持有者同时占有。</summary>
    DoubleOwned,
    /// <summary>非系统渠道把物品/金币销毁到虚空。</summary>
    UnaccountedSink,
    /// <summary>账号金币台账余额被扣成负数。</summary>
    NegativeBalance,
    /// <summary>快照中存在但台账从未见过的物品(漏报事件或非法生成)。</summary>
    ShadowItem,
    /// <summary>台账认为持有但快照中不存在 —— 物品凭空消失。</summary>
    VanishedItem,
    /// <summary>快照金币余额与台账不一致。</summary>
    SnapshotBalanceDrift,
    /// <summary>窗口内某物品净变动 ≠ 系统产出 − 系统消耗。</summary>
    SupplyDrift,
}

public sealed record Anomaly(
    AnomalyKind Kind,
    DateTimeOffset Ts,
    FlowChannel Channel,
    string TxnId,
    string OpId,
    string ItemUid,
    string ItemType,
    long Amount,
    IReadOnlyList<string> Accounts,
    string Detail);
