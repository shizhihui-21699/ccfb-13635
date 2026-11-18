using System.Text;
using EconAudit.Core;
using GameSim;

Console.OutputEncoding = Encoding.UTF8;

// 对账窗口:演示用 30 秒滚动窗口;生产建议 1 分钟(见 README 的窗口设计说明)
var audit = new AuditEngine(TimeSpan.FromSeconds(30));
var gs = new GameServer(audit);

void Banner(string t) { Console.WriteLine(); Console.WriteLine($"═══ {t} ═══"); gs.Tick(10); }

// ---------------------------------------------------------------- 阶段 0:正常经济活动(基线,应零告警)
Banner("阶段 0:正常经济活动(基线,应零告警)");
gs.CreateAccount("alice", 1000);
gs.CreateAccount("bob", 500);
gs.CreateAccount("carol", 300);
gs.CreateAccount("dave", 800);
gs.CreateAccount("eve", 600);
var sword  = gs.MintItem("alice", "iron-sword");   // 任务奖励(系统产出)
var shield = gs.MintItem("bob", "oak-shield");
var gem    = gs.MintItem("carol", "star-gem");
var armor  = gs.MintItem("carol", "plate-armor");
var potion = gs.NpcBuy("alice", "hp-potion", 50)!; // NPC 商店:金币消耗 + 物品产出
gs.TradeSafe("alice", sword, "bob", shield);       // 正常玩家交易
var cleanAnomalies = audit.Anomalies.Count;
Console.WriteLine($"  正常阶段结束,审计告警 {cleanAnomalies} 条(应为 0)");

// ---------------------------------------------------------------- 阶段 1:漏洞① 交易 × 断线回滚
Banner("阶段 1:交易提交与断线回滚竞争(漏洞①)");
gs.TradeWithDisconnectRace("carol", gem, "dave");  // carol 的 gem 给了 dave,回滚后 carol 手里又有一份
gs.SellForGold("dave", gem, "eve", 300);           // dave 把复制体卖给 eve 洗白

// ---------------------------------------------------------------- 阶段 2:漏洞② 邮件重复领取
Banner("阶段 2:邮件重复领取(漏洞②)");
var m1 = gs.MailSend("alice", "bob", potion);
gs.MailClaim("bob", m1);
gs.MailClaim("bob", m1);                           // 第二次领取 = 复制
var m2 = gs.MailSend("bob", "carol", potion);      // 复制体转寄给 carol
gs.MailClaim("carol", m2);

// ---------------------------------------------------------------- 阶段 3:漏洞③ 拍卖已售出仍可取消
Banner("阶段 3:拍卖已售出仍可取消(漏洞③)");
var auc = gs.AuctionList("carol", armor, 600);
gs.AuctionBuy("dave", auc);                        // dave 付款 600 买到 armor
gs.AuctionCancelBuggy(auc);                        // carol 取消拍卖,armor 又被退回
gs.SellForGold("dave", armor, "eve", 250);         // dave 把复制体卖给 eve

// ---------------------------------------------------------------- 阶段 4:漏洞④ 重连静默恢复(漏报事件)
Banner("阶段 4:重连加载过期快照且漏报事件(漏洞④)");
gs.Login("bob");                                   // 登录拍快照:gold=500, bag=[sword, potion复制体]
gs.NpcBuy("bob", "mp-potion", 80);                 // 花 80 金币买药
gs.GuildDeposit("bob", sword);                     // 把 sword 存入公会仓库
gs.Disconnect("bob");
gs.ReconnectRestoreSilent("bob");                  // 静默恢复:gold 回到 500,sword/potion 回到背包,mp-potion 消失
gs.GoldGift("bob", "carol", 100);                  // 黑金流出

// ---------------------------------------------------------------- 阶段 5:周期快照对账
Banner("阶段 5:周期快照对账(全量核对)");
foreach (var id in gs.Accounts.Keys) gs.ReportSnapshot(id);
audit.Finalize();

// ================================================================ 审计报告
var kindName = new Dictionary<AnomalyKind, string>
{
    [AnomalyKind.DuplicateEvent]        = "重复事件",
    [AnomalyKind.CreatedFromNothing]    = "凭空产生",
    [AnomalyKind.OrphanDebit]           = "无源出账",
    [AnomalyKind.DoubleOwned]           = "一物两主",
    [AnomalyKind.UnaccountedSink]       = "无账销毁",
    [AnomalyKind.NegativeBalance]       = "余额透支",
    [AnomalyKind.ShadowItem]            = "幽灵物品",
    [AnomalyKind.VanishedItem]          = "凭空消失",
    [AnomalyKind.SnapshotBalanceDrift]  = "余额漂移",
    [AnomalyKind.SupplyDrift]           = "总量漂移",
};

Console.WriteLine();
Console.WriteLine("╔══════════════════════════════════════════════════════════╗");
Console.WriteLine("║                     经 济 守 恒 审 计 报 告              ║");
Console.WriteLine("╚══════════════════════════════════════════════════════════╝");

// 1) 按操作环节定位
Console.WriteLine();
Console.WriteLine("【1】异常清单 —— 按操作环节定位");
foreach (var g in audit.Anomalies.GroupBy(a => (a.Channel, a.OpId)))
{
    Console.WriteLine($"  ◆ 环节 {g.Key.Channel} / 操作 {g.Key.OpId}");
    foreach (var a in g)
    {
        var who = a.Accounts.Count > 0 ? $" | 涉及: {string.Join(", ", a.Accounts)}" : "";
        Console.WriteLine($"      [{a.Ts:HH:mm:ss}] {kindName[a.Kind]}({a.Kind}) {a.ItemUid} x{a.Amount}{who}");
        Console.WriteLine($"               {a.Detail}");
    }
}

// 2) 污染物品流向追踪
Console.WriteLine();
Console.WriteLine("【2】污染物品流向追踪");
foreach (var (uid, trail) in audit.TaintedTrails())
{
    var holders = trail.SelectMany(h => new[] { h.From, h.To })
                       .Where(h => h != null && !h.Contains('#') && !h.Contains(':'))
                       .Distinct().ToList();
    Console.WriteLine($"  物品 {uid} —— 流经账号: {string.Join(" → ", holders)}");
    foreach (var h in trail)
        Console.WriteLine($"      {h.Ts:HH:mm:ss}  [{h.Channel,-9}] {h.From ?? "∅(系统/虚空)"} → {h.To ?? "∅(系统/虚空)"}  ({h.OpId})");
}

// 3) 黑金追踪
Console.WriteLine();
Console.WriteLine("【3】黑金流向(凭空产生的金币)");
var tainted = audit.TaintedGold.Where(kv => kv.Value > 0).ToList();
if (tainted.Count == 0) Console.WriteLine("  (无)");
foreach (var (holder, amt) in tainted)
    Console.WriteLine($"  {holder}: 持有黑金 {amt}");

// 4) 滚动窗口对账
Console.WriteLine();
Console.WriteLine("【4】滚动窗口对账(净变动 应== 系统产出 − 系统消耗)");
foreach (var w in audit.Windows)
{
    Console.WriteLine($"  窗口 {w.Start:HH:mm:ss} ~ {w.End:HH:mm:ss}  事件 {w.EventCount} 笔,异常 {w.Anomalies.Count} 条");
    foreach (var (t, n) in w.Minted)      Console.WriteLine($"      系统产出 {t}: +{n}");
    foreach (var (t, n) in w.Consumed)    Console.WriteLine($"      系统消耗 {t}: -{n}");
    foreach (var (t, n) in w.Unexplained) Console.WriteLine($"      ⚠ 无法解释的净变动 {t}: {n:+#;-#;0}");
}

// 5) 总结
var involved = audit.Anomalies.SelectMany(a => a.Accounts)
                              .Where(a => !a.Contains('#') && !a.Contains(':'))
                              .Distinct().OrderBy(x => x).ToList();
Console.WriteLine();
Console.WriteLine("【5】总结");
Console.WriteLine($"  基线阶段误报: {cleanAnomalies} 条");
Console.WriteLine($"  异常总数: {audit.Anomalies.Count} 条");
Console.WriteLine($"  被污染物品: {audit.TaintedUids.Count} 件 ({string.Join(", ", audit.TaintedUids.OrderBy(x => x))})");
Console.WriteLine($"  涉及账号: {string.Join(", ", involved)}");
