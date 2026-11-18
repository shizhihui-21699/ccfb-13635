using EconAudit.Core;

namespace GameSim;

/// <summary>
/// 简化的游戏服务器,用于演示。所有物品归属变化都会 Emit 流转事件给审计引擎
/// (漏洞④除外:它故意漏报,用来演示快照对账的兜底作用)。
/// 持有者可以是账号,也可以是容器:mail#N、auction#N、storage:guild。
/// </summary>
public sealed class GameServer
{
    private readonly AuditEngine _audit;
    private long _seq;
    private int _evt, _txn, _op, _itemSeq, _mailSeq, _aucSeq;

    public DateTimeOffset Clock { get; private set; } = new(2026, 11, 19, 20, 0, 0, TimeSpan.Zero);

    public sealed class Account
    {
        public required string Id { get; init; }
        public long Gold;
        public bool Online = true;
        public readonly List<(string Uid, string Type)> Bag = new();
        /// <summary>登录时服务器为"断线保护"拍的状态快照。</summary>
        public (long Gold, List<(string Uid, string Type)> Bag)? LoginSnapshot;
    }

    private sealed class Mail { public required string Id; public required string To; public (string Uid, string Type) Item; public bool Claimed; }
    private sealed class Auction { public required string Id; public required string Seller; public (string Uid, string Type) Item; public long Price; public bool Sold; }

    public Dictionary<string, Account> Accounts { get; } = new();
    private readonly List<Mail> _mails = new();
    private readonly List<Auction> _auctions = new();
    private readonly List<(string Uid, string Type)> _guildStorage = new();

    public GameServer(AuditEngine audit) => _audit = audit;

    public void Tick(double seconds) => Clock = Clock.AddSeconds(seconds);

    private void Emit(FlowChannel ch, string op, string txn, string uid, string type, long amt, string? from, string? to)
        => _audit.Ingest(new FlowEvent($"evt-{++_evt}", txn, ++_seq, Clock, ch, op, uid, type, amt, from, to));

    private string Op(string kind) => $"{kind}#{++_op}";
    private string Txn() => $"txn-{++_txn}";

    private static bool Take(Account acc, string uid, out (string Uid, string Type) item)
    {
        var i = acc.Bag.FindIndex(x => x.Uid == uid);
        item = i >= 0 ? acc.Bag[i] : default;
        if (i >= 0) acc.Bag.RemoveAt(i);
        return i >= 0;
    }

    // ---------------------------------------------------------- 正常系统功能

    public Account CreateAccount(string id, long gold)
    {
        var acc = new Account { Id = id, Gold = gold };
        Accounts[id] = acc;
        if (gold > 0)
            Emit(FlowChannel.System, Op("create-account"), Txn(), AuditEngine.GoldUid, AuditEngine.GoldType, gold, null, id);
        return acc;
    }

    /// <summary>任务奖励:系统产出物品。</summary>
    public string MintItem(string to, string type)
    {
        var uid = $"itm-{++_itemSeq:D4}";
        Accounts[to].Bag.Add((uid, type));
        Emit(FlowChannel.System, Op("quest-reward"), Txn(), uid, type, 1, null, to);
        return uid;
    }

    /// <summary>NPC 商店:金币消耗 + 物品产出,同一事务两条事件。</summary>
    public string? NpcBuy(string id, string type, long price)
    {
        var acc = Accounts[id];
        if (acc.Gold < price) return null;
        acc.Gold -= price;
        var uid = $"itm-{++_itemSeq:D4}";
        acc.Bag.Add((uid, type));
        var txn = Txn(); var op = Op("npc-buy");
        Emit(FlowChannel.System, op, txn, AuditEngine.GoldUid, AuditEngine.GoldType, price, id, null);
        Emit(FlowChannel.System, op, txn, uid, type, 1, null, id);
        return uid;
    }

    /// <summary>正常玩家交易:原子交换,先记账后交货,无漏洞。</summary>
    public bool TradeSafe(string a, string uidA, string b, string uidB)
    {
        var aa = Accounts[a]; var bb = Accounts[b];
        if (!Take(aa, uidA, out var ia) || !Take(bb, uidB, out var ib)) return false;
        aa.Bag.Add(ib); bb.Bag.Add(ia);
        var txn = Txn(); var op = Op("trade");
        Emit(FlowChannel.Trade, op, txn, uidA, ia.Type, 1, a, b);
        Emit(FlowChannel.Trade, op, txn, uidB, ib.Type, 1, b, a);
        return true;
    }

    /// <summary>玩家间金币+物品买卖(正常路径)。</summary>
    public bool SellForGold(string seller, string uid, string buyer, long price)
    {
        var s = Accounts[seller]; var b = Accounts[buyer];
        if (b.Gold < price || !Take(s, uid, out var item)) return false;
        b.Gold -= price; s.Gold += price;
        b.Bag.Add(item);
        var txn = Txn(); var op = Op("player-sell");
        Emit(FlowChannel.Trade, op, txn, AuditEngine.GoldUid, AuditEngine.GoldType, price, buyer, seller);
        Emit(FlowChannel.Trade, op, txn, uid, item.Type, 1, seller, buyer);
        return true;
    }

    public void GoldGift(string from, string to, long amount)
    {
        Accounts[from].Gold -= amount;
        Accounts[to].Gold += amount;
        Emit(FlowChannel.Trade, Op("gold-gift"), Txn(), AuditEngine.GoldUid, AuditEngine.GoldType, amount, from, to);
    }

    public void GuildDeposit(string id, string uid)
    {
        if (!Take(Accounts[id], uid, out var item)) return;
        _guildStorage.Add(item);
        Emit(FlowChannel.Storage, Op("guild-deposit"), Txn(), uid, item.Type, 1, id, "storage:guild");
    }

    // ---------------------------------------------------------- 漏洞①:交易提交与断线回滚竞争

    /// <summary>
    /// 交易先把物品交付给买家并落账,此时卖家"断线",断线保护线程用交易前的
    /// 背包快照回滚卖家状态 —— 卖家物品失而复得,买家手里也有一份。
    /// </summary>
    public void TradeWithDisconnectRace(string seller, string uid, string buyer)
    {
        var s = Accounts[seller]; var b = Accounts[buyer];
        var backup = s.Bag.ToList();                 // 交易前的"安全快照"
        if (!Take(s, uid, out var item)) return;
        b.Bag.Add(item);                             // 已交付买家
        var txn = Txn(); var op = Op("trade");
        Emit(FlowChannel.Trade, op, txn, uid, item.Type, 1, seller, buyer);
        // --- 卖家断线,断线处理线程未检查交易已提交,直接用旧快照回滚 ---
        s.Bag.Clear();
        s.Bag.AddRange(backup);                    // 复制完成:双方各持一份
        Emit(FlowChannel.Reconnect, op, txn, uid, item.Type, 1, null, seller); // 回滚补账(有日志的版本)
    }

    // ---------------------------------------------------------- 漏洞②:邮件可重复领取

    public string MailSend(string from, string to, string uid)
    {
        if (!Take(Accounts[from], uid, out var item)) return "";
        var mail = new Mail { Id = $"mail#{++_mailSeq}", To = to, Item = item, Claimed = false };
        _mails.Add(mail);
        Emit(FlowChannel.Mail, Op("mail-send"), Txn(), uid, item.Type, 1, from, mail.Id);
        return mail.Id;
    }

    public bool MailClaim(string accountId, string mailId)
    {
        var mail = _mails.First(m => m.Id == mailId);
        if (mail.To != accountId) return false;
        // BUG: 发货与"标记已领取"不在同一事务,这里干脆漏掉了标记,可重复领取
        Accounts[accountId].Bag.Add(mail.Item);
        Emit(FlowChannel.Mail, Op("mail-claim"), Txn(), mail.Item.Uid, mail.Item.Type, 1, mail.Id, accountId);
        // 正确实现应有: mail.Claimed = true; 且与发货原子提交
        return true;
    }

    // ---------------------------------------------------------- 漏洞③:拍卖已售出仍可取消

    public string AuctionList(string seller, string uid, long price)
    {
        if (!Take(Accounts[seller], uid, out var item)) return "";
        var auc = new Auction { Id = $"auction#{++_aucSeq}", Seller = seller, Item = item, Price = price, Sold = false };
        _auctions.Add(auc);
        Emit(FlowChannel.Auction, Op("auction-list"), Txn(), uid, item.Type, 1, seller, auc.Id);
        return auc.Id;
    }

    public bool AuctionBuy(string buyer, string auctionId)
    {
        var auc = _auctions.First(a => a.Id == auctionId);
        var b = Accounts[buyer]; var s = Accounts[auc.Seller];
        if (auc.Sold || b.Gold < auc.Price) return false;
        b.Gold -= auc.Price; s.Gold += auc.Price;
        b.Bag.Add(auc.Item);
        auc.Sold = true;
        var txn = Txn(); var op = Op("auction-buy");
        Emit(FlowChannel.Auction, op, txn, AuditEngine.GoldUid, AuditEngine.GoldType, auc.Price, buyer, auc.Seller);
        Emit(FlowChannel.Auction, op, txn, auc.Item.Uid, auc.Item.Type, 1, auc.Id, buyer);
        return true;
    }

    public void AuctionCancelBuggy(string auctionId)
    {
        var auc = _auctions.First(a => a.Id == auctionId);
        // BUG: 取消时未校验 Sold,已成交的物品又被退回卖家
        Accounts[auc.Seller].Bag.Add(auc.Item);
        Emit(FlowChannel.Auction, Op("auction-cancel"), Txn(), auc.Item.Uid, auc.Item.Type, 1, auc.Id, auc.Seller);
    }

    // ---------------------------------------------------------- 漏洞④:重连加载过期快照且漏报事件

    public void Login(string id)
    {
        var acc = Accounts[id];
        acc.Online = true;
        acc.LoginSnapshot = (acc.Gold, acc.Bag.ToList());
    }

    public void Disconnect(string id) => Accounts[id].Online = false;

    /// <summary>
    /// 重连时用登录时的旧快照整体覆盖当前状态。物品/金币全部"回到过去",
    /// 且该恢复路径没有接入事件总线 —— 一条事件都不发,实时对账完全看不到。
    /// </summary>
    public void ReconnectRestoreSilent(string id)
    {
        var acc = Accounts[id];
        if (acc.LoginSnapshot is { } snap)
        {
            acc.Gold = snap.Gold;
            acc.Bag.Clear();
            acc.Bag.AddRange(snap.Bag);
        }
        acc.Online = true;
        // 注意:故意不 Emit —— 这就是"漏事件"场景,只能靠周期快照对账发现
    }

    // ---------------------------------------------------------- 对账钩子

    /// <summary>周期快照上报:把账号当前全量状态发给审计引擎核对。</summary>
    public void ReportSnapshot(string id)
    {
        var acc = Accounts[id];
        _audit.ReconcileSnapshot(id, acc.Gold, acc.Bag, Clock, $"snapshot/{id}");
    }
}
