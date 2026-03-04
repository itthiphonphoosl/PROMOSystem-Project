// controllers/op_scan.controller.js
// ──────────────────────────────────────────
// มีแค่ 2 function: startOpScan, finishOpScan
// GET endpoints อยู่ใน op_scan_query.controller.js
// ──────────────────────────────────────────
const sql = require("mssql");
const { getPool } = require("../config/db");

const OP_SCAN_TABLE  = process.env.OP_SCAN_TABLE || process.env.OPSCAN_TABLE || "dbo.op_scan";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";
const MACHINE_TABLE  = process.env.MACHINE_TABLE  || "dbo.machine";
const PART_TABLE     = process.env.PART_TABLE     || "dbo.part";
const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";
const SAFE_RUNLOG    = safeTableName(process.env.TKRUNLOG_TABLE || "dbo.TKRunLog");

function safeTableName(fullName) {
  const s = String(fullName || "");
  if (!/^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return s;
}

const SAFE_OPSCAN   = safeTableName(OP_SCAN_TABLE);
const SAFE_TKDETAIL = safeTableName(TKDETAIL_TABLE);
const SAFE_MACHINE  = safeTableName(MACHINE_TABLE);
const SAFE_PART     = safeTableName(PART_TABLE);
const SAFE_TRANSFER = safeTableName(TRANSFER_TABLE);

// ── helpers ──────────────────────────────────────────────
function pad(n, len) { return String(n).padStart(len, "0"); }
function yymmdd(d) {
  const yy = String(d.getFullYear()).slice(-2);
  const mm  = pad(d.getMonth() + 1, 2);
  const dd  = pad(d.getDate(), 2);
  return `${yy}${mm}${dd}`;
}
function normalizeClientType(raw) {
  const v = String(raw || "").trim().toUpperCase();
  if (v === "HH" || v === "FLUTTER") return "HH";
  if (v === "PC" || v === "REACT")   return "PC";
  return "UNKNOWN";
}
function actorOf(req) {
  return {
    u_id:        req.user?.u_id        ?? null,
    u_name:      req.user?.u_name      ?? "unknown",
    role:        req.user?.role        ?? "unknown",
    u_type:      req.user?.u_type      ?? "unknown",
    op_sta_id:   req.user?.op_sta_id   ?? null,
    op_sta_name: req.user?.op_sta_name ?? null,
    clientType:  normalizeClientType(req.headers["x-client-type"]),
  };
}
function forbid(res, msg, actor) { return res.status(403).json({ message: msg, actor }); }

async function genOpScId(tx, now) {
  const prefix = `SC${yymmdd(now)}`;
  const r = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(20), `${prefix}%`)
    .query(`
      SELECT TOP 1 op_sc_id FROM ${SAFE_OPSCAN} WITH (UPDLOCK, HOLDLOCK)
      WHERE op_sc_id LIKE @likePrefix ORDER BY op_sc_id DESC
    `);
  let running = 1;
  if (r.recordset?.length) {
    const n = parseInt(String(r.recordset[0].op_sc_id).slice(prefix.length), 10);
    if (!Number.isNaN(n)) running = n + 1;
  }
  return `${prefix}${pad(running, 4)}`;
}

async function getPartByNo(tx, part_no) {
  const r = await new sql.Request(tx)
    .input("part_no", sql.VarChar(100), String(part_no).trim())
    .query(`SELECT TOP 1 part_id, part_no, part_name FROM ${SAFE_PART} WITH (NOLOCK) WHERE part_no = @part_no`);
  return r.recordset?.[0] ?? null;
}

// ดึง part_id จาก lot_no ใน TKRunLog (ใช้ตอน Gen Lot พักสำหรับ Split/CO)
async function getPartIdByLotNo(tx, tk_id, lot_no) {
  const r = await new sql.Request(tx)
    .input("tk_id",  sql.VarChar(20),   tk_id)
    .input("lot_no", sql.NVarChar(300), lot_no)
    .query(`
      SELECT TOP 1 part_id FROM ${SAFE_RUNLOG} WITH (NOLOCK)
      WHERE tk_id = @tk_id AND lot_no = @lot_no
    `);
  return r.recordset?.[0]?.part_id ?? null;
}

async function genNewLot(tx, tk_id, part_id, actor_u_id) {
  const sp = await new sql.Request(tx)
    .input("tk_id",           sql.VarChar(20), tk_id)
    .input("part_id",         sql.Int,         Number(part_id))
    .input("created_by_u_id", sql.Int,         Number(actor_u_id))
    .output("run_no",         sql.Char(14))
    .output("lot_no",         sql.NVarChar(300))
    .execute("dbo.usp_TKRunLog_Create");

  const run_no = sp.output.run_no;
  const lot_no = sp.output.lot_no;
  if (!run_no || !lot_no) throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");
  return { run_no: String(run_no).trim(), lot_no: String(lot_no) };
}

async function lotExistsInRunLog(tx, tk_id, lot_no) {
  const r = await new sql.Request(tx)
    .input("tk_id",  sql.VarChar(20),   tk_id)
    .input("lot_no", sql.NVarChar(300), lot_no)
    .query(`SELECT TOP 1 lot_no FROM ${SAFE_RUNLOG} WITH (NOLOCK) WHERE tk_id = @tk_id AND lot_no = @lot_no`);
  return !!r.recordset?.[0];
}

// ── Cross-TK Parked Lot Helpers ────────────────────────────────
// หา TK owner ของ lot จาก TKRunLog (ไม่จำกัด tk_id) — รองรับ cross-TK
async function getLotOwnerTk(tx, lot_no) {
  const r = await new sql.Request(tx)
    .input("lot_no", sql.NVarChar(300), String(lot_no || "").trim())
    .query(`SELECT TOP 1 tk_id FROM ${SAFE_RUNLOG} WITH (NOLOCK) WHERE lot_no = @lot_no`);
  return r.recordset?.[0]?.tk_id ? String(r.recordset[0].tk_id).trim() : null;
}

// Un-park lot จาก TK อื่น (set lot_parked_status=0 บน transfer row เดิม)
async function unParkCrossTkLot(tx, owner_tk_id, lot_no) {
  await new sql.Request(tx)
    .input("owner_tk_id", sql.VarChar(20),   String(owner_tk_id).trim())
    .input("lot_no",      sql.NVarChar(300), String(lot_no).trim())
    .query(`
      UPDATE ${SAFE_TRANSFER}
      SET lot_parked_status = 0
      WHERE from_tk_id       = @owner_tk_id
        AND to_lot_no         = @lot_no
        AND lot_parked_status = 1
    `);
}

// INSERT 1 row ใน t_transfer พร้อม field ใหม่ op_sta_id + lot_parked_status
async function insertTransfer(tx, { from_tk_id, to_tk_id, from_lot_no, to_lot_no,
  tf_rs_code, transfer_qty, op_sc_id, MC_id, op_sta_id,
  lot_parked_status, created_by_u_id, transfer_ts }) {
  await new sql.Request(tx)
    .input("from_tk_id",        sql.VarChar(20),   from_tk_id)
    .input("to_tk_id",          sql.VarChar(20),   to_tk_id)
    .input("from_lot_no",       sql.NVarChar(300), from_lot_no || "")
    .input("to_lot_no",         sql.NVarChar(300), to_lot_no)
    .input("tf_rs_code",        sql.Int,           tf_rs_code)
    .input("transfer_qty",      sql.Int,           transfer_qty)
    .input("op_sc_id",          sql.Char(12),      op_sc_id)
    .input("MC_id",             sql.VarChar(10),   MC_id ?? null)
    .input("op_sta_id",         sql.VarChar(20),   op_sta_id ?? null)
    .input("lot_parked_status", sql.Bit,           lot_parked_status ?? 0)
    .input("created_by_u_id",   sql.Int,           Number(created_by_u_id))
    .input("transfer_ts",       sql.DateTime2(3),  transfer_ts)
    .query(`
      INSERT INTO ${SAFE_TRANSFER}
        (from_tk_id, to_tk_id, from_lot_no, to_lot_no,
         tf_rs_code, transfer_qty, op_sc_id, MC_id,
         op_sta_id, lot_parked_status,
         created_by_u_id, transfer_ts)
      VALUES
        (@from_tk_id, @to_tk_id, @from_lot_no, @to_lot_no,
         @tf_rs_code, @transfer_qty, @op_sc_id, @MC_id,
         @op_sta_id, @lot_parked_status,
         @created_by_u_id, @transfer_ts)
    `);
}

// ══════════════════════════════════════════════════════════
// START
// POST /api/op-scans/start
// ══════════════════════════════════════════════════════════
exports.startOpScan = async (req, res) => {
  const actor     = actorOf(req);
  const op_sta_id = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";

  if (actor.u_type   !== "op") return forbid(res, "Forbidden: u_type must be op", actor);
  if (actor.clientType !== "HH") return forbid(res, "Forbidden: clientType must be HH", actor);
  if (!actor.u_id)   return res.status(401).json({ message: "Unauthorized", actor });
  if (!op_sta_id)    return res.status(400).json({ message: "Missing op_sta_id in token", actor });

  const tk_id = String(req.body.tk_id || "").trim();
  const MC_id = String(req.body.MC_id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "tk_id is required", actor });
  if (!MC_id) return res.status(400).json({ message: "MC_id is required", actor });

  try {
    const pool = await getPool();
    const tx   = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      const now = new Date();

      // ① TKHead ต้องไม่ FINISHED
      const headR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`SELECT TOP 1 tk_status FROM dbo.TKHead WITH (NOLOCK) WHERE tk_id = @tk_id`);
      const headStatus = Number(headR.recordset?.[0]?.tk_status ?? -1);
      if (headStatus === -1) {
        await tx.rollback();
        return res.status(404).json({ message: "tk_id not found", actor, tk_id });
      }
      if (headStatus === 1) {
        await tx.rollback();
        return res.status(403).json({ message: "This tk_id is FINISHED. Cannot start.", actor, tk_id });
      }

      // ② TKDetail
      const tkDetailR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          SELECT TOP 1 d.tk_id, d.part_id, p.part_no, p.part_name, d.lot_no,
                       d.tk_status, d.tk_created_at_ts
          FROM ${SAFE_TKDETAIL} d WITH (UPDLOCK, HOLDLOCK)
          LEFT JOIN dbo.part p ON p.part_id = d.part_id
          WHERE d.tk_id = @tk_id ORDER BY d.tk_created_at_ts DESC
        `);
      const tkDoc = tkDetailR.recordset?.[0];
      if (!tkDoc) { await tx.rollback(); return res.status(404).json({ message: "tk_id not found in TKDetail", actor, tk_id }); }

      // ③ base lot (จาก TKRunLog)
      const allLotsR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          SELECT r.run_no, r.lot_no, p.part_no, p.part_name, r.created_at_ts
          FROM dbo.TKRunLog r WITH (NOLOCK)
          LEFT JOIN dbo.part p ON p.part_id = r.part_id
          WHERE r.tk_id = @tk_id ORDER BY r.created_at_ts DESC
        `);
      const allLots    = allLotsR.recordset || [];
      const baseLotRow = allLots.length ? allLots[allLots.length - 1] : null;
      const base_lot_no = baseLotRow?.lot_no ? String(baseLotRow.lot_no).trim() : null;
      const base_run_no = baseLotRow?.run_no ? String(baseLotRow.run_no).trim() : null;

      // ③.b leaf lots — active lots ที่ยังไม่ถูก split ต่อ และไม่ใช่ lot พัก
      //   = to_lot_no ที่ lot_parked_status=0 AND ไม่อยู่ใน fromSet
      const transfersR2 = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          SELECT from_lot_no, to_lot_no, lot_parked_status
          FROM ${SAFE_TRANSFER} WITH (NOLOCK)
          WHERE from_tk_id = @tk_id OR to_tk_id = @tk_id
        `);
      const tRows    = transfersR2.recordset || [];
      const fromSet  = new Set(tRows.map(r => (r.from_lot_no || '').trim()).filter(Boolean));
      const leafLots = tRows
        .filter(r => r.lot_parked_status === 0 || r.lot_parked_status === false)
        .map(r => (r.to_lot_no || '').trim())
        .filter(lot => lot && !fromSet.has(lot));
      // deduplicate
      const leafLotSet = [...new Set(leafLots)];

      // ④ กัน STA007 finished
      const finishedR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`SELECT TOP 1 op_sc_id FROM ${SAFE_OPSCAN} WITH (NOLOCK)
                WHERE tk_id = @tk_id AND op_sta_id = 'STA007' AND op_sc_finish_ts IS NOT NULL`);
      if (finishedR.recordset?.[0]) {
        await tx.rollback();
        return res.status(403).json({ message: "Already FINISHED at STA007.", actor, tk_id });
      }

      // ⑤ กัน station ย้อน
      const lastFinR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          SELECT TOP 1 s.op_sta_id, st.op_sta_name
          FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
          LEFT JOIN dbo.op_station st ON st.op_sta_id = s.op_sta_id
          WHERE s.tk_id = @tk_id AND s.op_sc_finish_ts IS NOT NULL AND s.op_sta_id IS NOT NULL
          ORDER BY s.op_sc_finish_ts DESC
        `);
      const lastFinSta = lastFinR.recordset?.[0];
      if (lastFinSta) {
        const staNum = (sta) => parseInt(String(sta).replace("STA", ""), 10);
        const lastNum    = staNum(lastFinSta.op_sta_id);
        const currentNum = staNum(op_sta_id);
        if (Number.isFinite(lastNum) && Number.isFinite(currentNum) && currentNum <= lastNum) {
          const nextStaR = await new sql.Request(tx)
            .input("lastNum", sql.Int, lastNum)
            .query(`SELECT TOP 1 op_sta_id, op_sta_name FROM dbo.op_station
                    WHERE CAST(REPLACE(op_sta_id,'STA','') AS INT) > @lastNum AND op_sta_active=1
                    ORDER BY CAST(REPLACE(op_sta_id,'STA','') AS INT) ASC`);
          const nextSta = nextStaR.recordset?.[0];
          await tx.rollback();
          return res.status(403).json({
            message: nextSta
              ? `ทำถึง ${lastFinSta.op_sta_id} แล้ว ❌ ห้ามเริ่มที่ ${op_sta_id} ✅ ให้ไปที่ ${nextSta.op_sta_id} (${nextSta.op_sta_name})`
              : `ทำถึง ${lastFinSta.op_sta_id} แล้ว ไม่มี Station ถัดไป`,
            actor, tk_id,
            last_finished_sta:      lastFinSta.op_sta_id,
            suggested_next_sta:     nextSta?.op_sta_id    ?? null,
            suggested_next_sta_name: nextSta?.op_sta_name ?? null,
          });
        }
      }

      // ⑥ validate machine
      const mcR = await new sql.Request(tx)
        .input("MC_id", sql.VarChar(10), MC_id)
        .query(`SELECT TOP 1 MC_id, MC_name, op_sta_id FROM ${SAFE_MACHINE} WITH (NOLOCK)
                WHERE MC_id = @MC_id AND MC_active = 1`);
      const mcRow = mcR.recordset?.[0];
      if (!mcRow) { await tx.rollback(); return res.status(400).json({ message: "MC_id not found or inactive", actor, MC_id }); }
      if (String(mcRow.op_sta_id || "").trim() !== op_sta_id) {
        await tx.rollback();
        return res.status(403).json({
          message: `Machine ${MC_id} ไม่ได้อยู่ใน station ของคุณ (${op_sta_id}). Machine อยู่ที่ ${mcRow.op_sta_id ?? "ไม่มี"}`,
          actor, MC_id,
        });
      }

      // ⑦ กัน active scan ซ้ำ
      const activeR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`SELECT TOP 1 op_sc_id, op_sta_id FROM ${SAFE_OPSCAN} WITH (UPDLOCK, HOLDLOCK)
                WHERE tk_id = @tk_id AND op_sc_finish_ts IS NULL ORDER BY op_sc_ts DESC`);
      if (activeR.recordset?.[0]) {
        await tx.rollback();
        return res.status(409).json({ message: "tk_id นี้มี active scan อยู่แล้ว", actor, tk_id, op_sc_id: activeR.recordset[0].op_sc_id });
      }

      // ⑧ gen op_sc_id + INSERT
      const op_sc_id = await genOpScId(tx, now);
      await new sql.Request(tx)
        .input("op_sc_id",  sql.Char(12),      op_sc_id)
        .input("tk_id",     sql.VarChar(20),   tk_id)
        .input("op_sta_id", sql.VarChar(20),   op_sta_id)
        .input("MC_id",     sql.VarChar(10),   MC_id)
        .input("u_id",      sql.Int,           Number(actor.u_id))
        .input("lot_no",    sql.NVarChar(300), tkDoc.lot_no || null)
        .input("op_sc_ts",  sql.DateTime2(3),  now)
        .query(`
          INSERT INTO ${SAFE_OPSCAN}
            (op_sc_id, tk_id, op_sta_id, MC_id, u_id,
             op_sc_total_qty, op_sc_scrap_qty, op_sc_good_qty,
             tf_rs_code, lot_no, op_sc_ts, op_sc_finish_ts)
          VALUES
            (@op_sc_id, @tk_id, @op_sta_id, @MC_id, @u_id,
             0, 0, 0, NULL, @lot_no, @op_sc_ts, NULL)
        `);

      // ⑨ update TKDetail + TKHead status = 3 (IN_PROGRESS)
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("MC_id", sql.VarChar(10), MC_id)
        .input("op_sta_id", sql.VarChar(20), op_sta_id)
        .input("op_sc_id",  sql.Char(12),    op_sc_id)
        .query(`UPDATE ${SAFE_TKDETAIL} SET MC_id=@MC_id, op_sta_id=@op_sta_id, op_sc_id=@op_sc_id WHERE tk_id=@tk_id`);

      for (const tbl of ["dbo.TKHead", SAFE_TKDETAIL]) {
        await new sql.Request(tx)
          .input("tk_id", sql.VarChar(20), tk_id)
          .query(`UPDATE ${tbl} SET tk_status=3 WHERE tk_id=@tk_id AND tk_status<>1`);
      }

      await tx.commit();
      console.log(`[OPSCAN_START][OK] op_sc_id=${op_sc_id} tk_id=${tk_id} op_sta_id=${op_sta_id} MC_id=${MC_id}`);

      return res.status(201).json({
        message: "Started",
        actor:   { u_id: actor.u_id, u_name: actor.u_name, role: actor.role },
        op_sc_id, op_sta_id, op_sta_name: actor.op_sta_name ?? null, MC_id,
        op_sc_total_qty: 0,
        base_lot_no, base_run_no,
        tk_doc: {
          // ✅ ไม่ใส่ lot_no เพราะมีใน current_lots แล้ว
          tk_id:      tkDoc.tk_id,
          part_id:    tkDoc.part_id,
          part_no:    tkDoc.part_no,
          part_name:  tkDoc.part_name,
          op_sta_id,  op_sta_name: actor.op_sta_name ?? null,
          tk_status:  3,
        },
        // ✅ current_lots = leaf lots เท่านั้น
        //    (active + ยังไม่ถูก split ต่อ + ไม่ใช่ lot พัก)
        //    ถ้ายังไม่มี transfer เลย → ใช้ base_lot_no
        current_lots: leafLotSet.length > 0
          ? leafLotSet.map(lot => {
              const runRow = allLots.find(r => (r.lot_no || '').trim() === lot);
              return {
                run_no:    runRow?.run_no ? String(runRow.run_no).trim() : null,
                lot_no:    lot,
                part_no:   runRow?.part_no   ?? null,
                part_name: runRow?.part_name ?? null,
              };
            })
          : base_lot_no
            ? [{ run_no: base_run_no, lot_no: base_lot_no,
                 part_no: tkDoc.part_no ?? null, part_name: tkDoc.part_name ?? null }]
            : [],
      });
    } catch (e) { await tx.rollback(); throw e; }
  } catch (err) {
    console.error("[OPSCAN_START][ERROR]", err);
    return res.status(500).json({ message: "Start failed", actor, error: err.message });
  }
};

// ══════════════════════════════════════════════════════════
// FINISH
// POST /api/op-scans/finish
// ══════════════════════════════════════════════════════════
exports.finishOpScan = async (req, res) => {
  const actor = actorOf(req);
  if (actor.u_type   !== "op") return forbid(res, "Forbidden: u_type must be op", actor);
  if (actor.clientType !== "HH") return forbid(res, "Forbidden: clientType must be HH", actor);
  if (!actor.u_id)   return res.status(401).json({ message: "Unauthorized", actor });

  const op_sc_id      = String(req.body.op_sc_id || "").trim();
  const good_qty      = Math.abs(Number(req.body.good_qty));
  const scrap_qty     = Math.abs(Number(req.body.scrap_qty));
  const total_qty     = good_qty + scrap_qty;
  const groups        = Array.isArray(req.body.groups) ? req.body.groups : [];

  if (!op_sc_id)  return res.status(400).json({ message: "op_sc_id is required", actor });
  if (!Number.isFinite(good_qty) || !Number.isFinite(scrap_qty))
    return res.status(400).json({ message: "good_qty/scrap_qty must be numbers", actor });
  if (good_qty === 0 && scrap_qty === 0)
    return res.status(400).json({ message: "good_qty and scrap_qty cannot both be 0", actor });
  if (groups.length === 0)
    return res.status(400).json({ message: "groups[] is required", actor });

  // ── validate + auto-calc group qty ────────────────────
  // Design:
  //   tf=1 → qty ต้องส่งมา (ไม่มีแหล่งอื่นบอก)
  //   tf=2 → qty ไม่ต้องส่ง → backend ดึงจาก from_lot ใน DB (ทำตอน process)
  //          validate แค่ splits.qty ≤ from_lot qty
  //   tf=3 → qty ไม่ต้องส่ง → backend sum(merge_lots.qty) อัตโนมัติ
  for (let i = 0; i < groups.length; i++) {
    const g   = groups[i];
    const tf  = Number(g.tf_rs_code);
    const num = i + 1;

    if (![1, 2, 3].includes(tf))
      return res.status(400).json({ message: `groups[${num}]: tf_rs_code must be 1, 2, or 3`, actor });

    // tf=1: qty required
    if (tf === 1) {
      if (!Number.isFinite(Number(g.qty)) || Number(g.qty) <= 0)
        return res.status(400).json({ message: `groups[${num}]: qty is required for tf=1`, actor });
      if (!String(g.out_part_no || "").trim())
        return res.status(400).json({ message: `groups[${num}]: out_part_no is required`, actor });
    }

    // tf=2: ต้องส่ง qty มาด้วย (บอกว่า group นี้ใช้กี่ชิ้นจาก good_qty)
    //        ไม่เช็คกับ DB lot qty — สนแค่ sum(groups) == good_qty
    if (tf === 2) {
      if (!Number.isFinite(Number(g.qty)) || Number(g.qty) <= 0)
        return res.status(400).json({ message: `groups[${num}]: qty is required for tf=2`, actor });
      if (!String(g.out_part_no || "").trim())
        return res.status(400).json({ message: `groups[${num}]: out_part_no is required`, actor });
      if (!String(g.from_lot_no || "").trim())
        return res.status(400).json({ message: `groups[${num}]: from_lot_no is required for tf=2`, actor });
      if (Array.isArray(g.splits) && g.splits.length > 0) {
        let sumSplit = 0;
        for (const s of g.splits) {
          if (!String(s.out_part_no || "").trim())
            return res.status(400).json({ message: `groups[${num}] split: out_part_no is required`, actor });
          if (!Number.isFinite(Number(s.qty)) || Number(s.qty) <= 0)
            return res.status(400).json({ message: `groups[${num}] split: qty must be > 0`, actor });
          sumSplit += Math.trunc(Number(s.qty));
        }
        // splits ต้องไม่เกิน group.qty (ส่วนที่เหลือ = พักอัตโนมัติ)
        if (sumSplit > Math.trunc(Number(g.qty)))
          return res.status(400).json({
            message: `groups[${num}]: sum splits (${sumSplit}) เกิน group qty (${Math.trunc(Number(g.qty))})`, actor
          });
      }
    }

    // tf=3: qty ไม่ต้องส่ง — auto sum(merge_lots.qty)
    if (tf === 3) {
      if (!String(g.out_part_no || "").trim())
        return res.status(400).json({ message: `groups[${num}]: out_part_no is required`, actor });
      if (!Array.isArray(g.merge_lots) || g.merge_lots.length < 2)
        return res.status(400).json({ message: `groups[${num}]: merge_lots[] must have >= 2 lots`, actor });
      for (const m of g.merge_lots) {
        if (!String(m.from_lot_no || "").trim())
          return res.status(400).json({ message: `groups[${num}]: every merge_lot must have from_lot_no`, actor });
        if (!Number.isFinite(Number(m.qty)) || Number(m.qty) <= 0)
          return res.status(400).json({ message: `groups[${num}]: merge_lot qty must be > 0`, actor });
      }
      // ✅ auto-inject qty = sum(merge_lots.qty)
      g.qty = g.merge_lots.reduce((a, m) => a + Math.trunc(Number(m.qty)), 0);
    }
  }

  // ── กัน from_lot_no ซ้ำข้ามกลุ่ม (ทุก tf) ────────────────
  const allFromLots = new Set();
  for (const g of groups) {
    const tf = Number(g.tf_rs_code);
    const lots = tf === 3
      ? g.merge_lots.map(m => String(m.from_lot_no).trim())
      : (g.from_lot_no ? [String(g.from_lot_no).trim()] : []);
    for (const lot of lots) {
      if (!lot) continue;
      if (allFromLots.has(lot))
        return res.status(400).json({ message: `from_lot_no "${lot}" ใช้ซ้ำในหลาย group`, actor });
      allFromLots.add(lot);
    }
  }

  // ── Validate: sum(all groups qty) === good_qty ───────────────
  // ไม่เช็คกับ DB lot qty — สนแค่จำนวนที่ operation นี้ประกาศ
  // tf=1: qty user ส่งมา
  // tf=2: qty user ส่งมา (splits ≤ group.qty)
  // tf=3: auto sum(merge_lots.qty)
  const totalGroupQty = groups.reduce((a, g) => a + Math.trunc(Number(g.qty)), 0);
  if (totalGroupQty !== Math.trunc(good_qty))
    return res.status(400).json({
      message: `Sum ของทุก group (${totalGroupQty}) ต้องเท่ากับ good_qty (${Math.trunc(good_qty)}) พอดี`,
      actor,
    });

  try {
    const pool = await getPool();
    const tx   = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      const now = new Date();

      // ① lock op_scan row
      const rowR = await new sql.Request(tx)
        .input("op_sc_id", sql.Char(12), op_sc_id)
        .query(`SELECT TOP 1 * FROM ${SAFE_OPSCAN} WITH (UPDLOCK, HOLDLOCK) WHERE op_sc_id = @op_sc_id`);
      const row = rowR.recordset?.[0];
      if (!row) { await tx.rollback(); return res.status(404).json({ message: "op_sc_id not found", actor, op_sc_id }); }
      if (row.op_sc_finish_ts) { await tx.rollback(); return res.status(409).json({ message: "Already finished", actor, op_sc_id }); }

      // ② station lock: ต้อง finish ที่ station เดิมที่ start
      const actorSta = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";
      if (!actorSta) { await tx.rollback(); return res.status(400).json({ message: "Missing op_sta_id in token", actor }); }
      const scanSta = row.op_sta_id ? String(row.op_sta_id).trim() : "";
      if (scanSta && scanSta !== actorSta) {
        await tx.rollback();
        return res.status(403).json({ message: `Scan started at ${scanSta} but you login as ${actorSta}`, actor, op_sc_id });
      }

      const master_tk_id = String(row.tk_id || "").trim();

      // ③ TKHead ต้องไม่ FINISHED
      const headR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), master_tk_id)
        .query(`SELECT TOP 1 tk_status FROM dbo.TKHead WITH (NOLOCK) WHERE tk_id = @tk_id`);
      if (Number(headR.recordset?.[0]?.tk_status) === 1) {
        await tx.rollback();
        return res.status(403).json({ message: "This tk_id is FINISHED. Cannot finish again.", actor, tk_id: master_tk_id });
      }

      // ④ base lot (part_id เดิมของ TK ใช้เป็น fallback)
      const baseDetailR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), master_tk_id)
        .query(`SELECT TOP 1 lot_no, part_id FROM ${SAFE_TKDETAIL} WITH (NOLOCK)
                WHERE tk_id=@tk_id ORDER BY tk_created_at_ts DESC`);
      const baseDetail  = baseDetailR.recordset?.[0];
      const base_lot_no = baseDetail?.lot_no ? String(baseDetail.lot_no).trim() : null;
      const base_part_id = baseDetail?.part_id ?? null;

      // ⑤ validate from_lot_no + parked lot station check
      // Business Rule:
      //   - from_lot_no ทุกตัวต้องมีใน TKRunLog
      //   - ถ้า lot นั้นเป็น parked lot (lot_parked_status=1)
      //     → ต้องพักอยู่ที่ station เดียวกับ operator ที่ทำตอนนี้เท่านั้น
      //   - ป้องกัน operator STA005 ดึง lot ที่พักอยู่ที่ STA003 มาใช้ผิด station
      {
        // รวม from_lot_no ทุกตัวจากทุก group/merge_lots
        const fromLotChecks = [];
        for (const g of groups) {
          const tf = Number(g.tf_rs_code);
          if (tf === 1 || tf === 2) {
            const lot = g.from_lot_no ? String(g.from_lot_no).trim() : null;
            if (lot) fromLotChecks.push({ lot, gLabel: `tf=${tf}` });
          }
          if (tf === 3) {
            for (const m of g.merge_lots) {
              fromLotChecks.push({ lot: String(m.from_lot_no).trim(), gLabel: "tf=3 merge_lot" });
            }
          }
        }

        for (const { lot, gLabel } of fromLotChecks) {
          // ① ต้องมีใน TKRunLog (TK ใดก็ได้ — รองรับ cross-TK parked lot)
          const lotOwnerTk = await getLotOwnerTk(tx, lot);
          if (!lotOwnerTk) {
            await tx.rollback();
            return res.status(400).json({ message: `${gLabel}: from_lot_no "${lot}" ไม่พบใน TKRunLog`, actor });
          }

          // ② ถ้าเป็น parked lot → ต้องพักที่ station เดียวกับ operator
          const parkedR = await new sql.Request(tx)
            .input("to_lot_no", sql.NVarChar(300), lot)
            .query(`
              SELECT TOP 1 op_sta_id, lot_parked_status
              FROM ${SAFE_TRANSFER} WITH (NOLOCK)
              WHERE to_lot_no = @to_lot_no
              ORDER BY transfer_ts DESC
            `);
          const pRow = parkedR.recordset?.[0];
          const isParked = pRow?.lot_parked_status === true || pRow?.lot_parked_status === 1;
          if (isParked) {
            const parkedSta = pRow?.op_sta_id ? String(pRow.op_sta_id).trim() : "";
            if (parkedSta && parkedSta !== actorSta) {
              await tx.rollback();
              return res.status(403).json({
                message: `${gLabel}: lot "${lot}" พักอยู่ที่ ${parkedSta} — ไม่สามารถใช้ที่ ${actorSta} ได้`,
                detail:  "Parked lot ต้องใช้ที่ station เดียวกับที่พักไว้เท่านั้น",
                parked_at_sta: parkedSta,
                your_sta:      actorSta,
                actor,
              });
            }
          }
        }
      }

      // ══════════════════════════════════════════════════
      // ⑥ process groups
      // ══════════════════════════════════════════════════
      const created_children = [];
      let first_lot_no = null;
      const MC_id_val  = row.MC_id ? String(row.MC_id).trim() : null;

      for (let i = 0; i < groups.length; i++) {
        const g         = groups[i];
        const tf        = Number(g.tf_rs_code);
        const group_qty = Math.trunc(Number(g.qty));
        const gNum      = i + 1;

        // ── tf=1 Master-ID ────────────────────────────
        // Business Rule:
        //   - lot_no ไม่เปลี่ยน (from_lot = to_lot) บันทึกว่าผ่าน station นี้แล้ว
        //   - out_part_no user เลือกได้ (part เดิมหรือ part ใหม่)
        //   - ถ้า part เปลี่ยน → UPDATE TKDetail.part_id (ไม่ gen lot ใหม่)
        if (tf === 1) {
          const outPart = await getPartByNo(tx, String(g.out_part_no).trim());
          if (!outPart) {
            await tx.rollback();
            return res.status(400).json({ message: `groups[${gNum}]: out_part_no "${g.out_part_no}" not found`, actor });
          }

          const from_lot = g.from_lot_no ? String(g.from_lot_no).trim() : base_lot_no;
          if (!from_lot) {
            await tx.rollback();
            return res.status(400).json({ message: `groups[${gNum}]: ไม่พบ from_lot_no สำหรับ Master-ID`, actor });
          }

          if (!first_lot_no) first_lot_no = from_lot;

          // ✅ UPDATE part_id ใน TKDetail เฉพาะเมื่อ part เปลี่ยน (lot ไม่เปลี่ยน)
          await new sql.Request(tx)
            .input("tk_id",   sql.VarChar(20), master_tk_id)
            .input("part_id", sql.Int,          Number(outPart.part_id))
            .query(`UPDATE ${SAFE_TKDETAIL} SET part_id=@part_id WHERE tk_id=@tk_id`);

          // ✅ Cross-TK: หา owner TK ของ from_lot — ถ้าเป็น TK อื่น ให้ un-park ก่อน
          const fromOwnerTk_1 = (await getLotOwnerTk(tx, from_lot)) || master_tk_id;
          if (fromOwnerTk_1 !== master_tk_id) {
            await unParkCrossTkLot(tx, fromOwnerTk_1, from_lot);
          }

          await insertTransfer(tx, {
            from_tk_id: fromOwnerTk_1, to_tk_id: master_tk_id,  // ✅ cross-TK aware
            from_lot_no: from_lot,
            to_lot_no:   from_lot,   // ✅ lot ไม่เปลี่ยน
            tf_rs_code: 1, transfer_qty: group_qty,
            op_sc_id, MC_id: MC_id_val,
            op_sta_id: actorSta,
            lot_parked_status: 0,
            created_by_u_id: actor.u_id, transfer_ts: now,
          });

          created_children.push({
            group: gNum, tf_rs_code: 1,
            from_lot_no: from_lot,
            to_lot_no:   from_lot,
            from_tk_id:  fromOwnerTk_1,   // ✅ บอก client ว่า lot มาจาก TK ไหน
            out_part_no: outPart.part_no,
            lots: [{ lot_no: from_lot, out_part_no: outPart.part_no, qty: group_qty }],
            parked_lots: [],
          });
        }

        // ── tf=2 Split-ID ──────────────────────────────
        // tf=2: group.qty มาจาก user โดยตรง — ไม่เช็คกับ DB lot qty
        if (tf === 2) {
          const from_lot = String(g.from_lot_no).trim();
          const splits   = Array.isArray(g.splits) ? g.splits : [];

          // ✅ Cross-TK: หา owner TK ของ from_lot — ถ้าเป็น TK อื่น ให้ un-park ก่อน
          const fromOwnerTk_2 = (await getLotOwnerTk(tx, from_lot)) || master_tk_id;
          if (fromOwnerTk_2 !== master_tk_id) {
            await unParkCrossTkLot(tx, fromOwnerTk_2, from_lot);
          }

          // ✅ ใช้ group.qty จาก request โดยตรง — ไม่เช็คกับ DB
          const group_qty_tf2 = Math.trunc(Number(g.qty));
          const sumUsed   = splits.reduce((a, s) => a + Math.trunc(Number(s.qty)), 0);
          const parkedQty = group_qty_tf2 - sumUsed;   // qty เหลือ = พักอัตโนมัติ

          const splitLots  = [];
          const parkedLots = [];

          // Gen lot สำหรับตัวที่ใช้จริง
          for (const s of splits) {
            const outPart = await getPartByNo(tx, String(s.out_part_no).trim());
            if (!outPart) { await tx.rollback(); return res.status(400).json({ message: `groups[${gNum}] split: out_part_no "${s.out_part_no}" not found`, actor }); }

            const s_qty = Math.trunc(Number(s.qty));
            const { run_no, lot_no } = await genNewLot(tx, master_tk_id, outPart.part_id, actor.u_id);
            if (!first_lot_no) first_lot_no = lot_no;

            await insertTransfer(tx, {
              from_tk_id: fromOwnerTk_2, to_tk_id: master_tk_id,  // ✅ cross-TK aware
              from_lot_no: from_lot, to_lot_no: lot_no,
              tf_rs_code: 2, transfer_qty: s_qty,
              op_sc_id, MC_id: MC_id_val,
              op_sta_id: actorSta,
              lot_parked_status: 0,   // ตัวที่ใช้ = Active
              created_by_u_id: actor.u_id, transfer_ts: now,
            });
            splitLots.push({ run_no, lot_no, out_part_no: outPart.part_no, qty: s_qty });
          }

          // Gen lot พักอัตโนมัติ ถ้ายัง qty เหลือ
          if (parkedQty > 0) {
            // ✅ ดึง part_id จาก owner TK ก่อน — fallback ไป master_tk_id
            const parkedPartId = from_lot
              ? (await getPartIdByLotNo(tx, fromOwnerTk_2, from_lot) || await getPartIdByLotNo(tx, master_tk_id, from_lot))
              : base_part_id;

            if (!parkedPartId) { await tx.rollback(); return res.status(400).json({ message: `groups[${gNum}]: cannot resolve part_id for parked lot`, actor }); }

            const { run_no: p_run, lot_no: p_lot } = await genNewLot(tx, master_tk_id, parkedPartId, actor.u_id);

            await insertTransfer(tx, {
              from_tk_id: fromOwnerTk_2, to_tk_id: master_tk_id,  // ✅ cross-TK aware
              from_lot_no: from_lot, to_lot_no: p_lot,
              tf_rs_code: 2, transfer_qty: parkedQty,
              op_sc_id, MC_id: MC_id_val,
              op_sta_id: actorSta,
              lot_parked_status: 1,   // ← พักไว้
              created_by_u_id: actor.u_id, transfer_ts: now,
            });
            parkedLots.push({ run_no: p_run, lot_no: p_lot, qty: parkedQty, parked_at_sta: actorSta });
          }

          // update TKDetail ให้ชี้ lot ที่ใช้ล่าสุด (ถ้ามี) หรือ parked lot
          const latestLot = splitLots.length ? splitLots[splitLots.length - 1].lot_no
                          : parkedLots.length ? parkedLots[0].lot_no : null;
          if (latestLot) {
            await new sql.Request(tx)
              .input("tk_id",  sql.VarChar(20),  master_tk_id)
              .input("lot_no", sql.NVarChar(300), latestLot)
              .query(`UPDATE ${SAFE_TKDETAIL} SET lot_no=@lot_no WHERE tk_id=@tk_id`);
          }

          created_children.push({
            group: gNum, tf_rs_code: 2,
            from_lot_no: from_lot,
            from_tk_id:  fromOwnerTk_2,   // ✅ บอก client ว่า lot มาจาก TK ไหน
            lots:        splitLots,
            parked_lots: parkedLots,
          });
        }

        // ── tf=3 Co-ID ─────────────────────────────────
        if (tf === 3) {
          const outPart = await getPartByNo(tx, String(g.out_part_no).trim());
          if (!outPart) { await tx.rollback(); return res.status(400).json({ message: `groups[${gNum}]: out_part_no "${g.out_part_no}" not found`, actor }); }

          // merge_lots ที่ส่งมา = ตัวที่รวมกัน (ต้องมี >= 2 ตรวจแล้วข้างบน)
          const { run_no, lot_no } = await genNewLot(tx, master_tk_id, outPart.part_id, actor.u_id);
          if (!first_lot_no) first_lot_no = lot_no;

          for (const m of g.merge_lots) {
            const mLotNo = String(m.from_lot_no).trim();
            // ✅ Cross-TK: หา owner TK ของแต่ละ merge lot
            const mergeOwnerTk = (await getLotOwnerTk(tx, mLotNo)) || master_tk_id;
            if (mergeOwnerTk !== master_tk_id) {
              await unParkCrossTkLot(tx, mergeOwnerTk, mLotNo);
            }
            await insertTransfer(tx, {
              from_tk_id: mergeOwnerTk, to_tk_id: master_tk_id,  // ✅ cross-TK aware
              from_lot_no: mLotNo, to_lot_no: lot_no,
              tf_rs_code: 3, transfer_qty: Math.trunc(Number(m.qty)),
              op_sc_id, MC_id: MC_id_val,
              op_sta_id: actorSta,
              lot_parked_status: 0,
              created_by_u_id: actor.u_id, transfer_ts: now,
            });
          }

          // Lot ที่ไม่ถูก CO (parked_lots ที่ส่งมาใน g.parked_from_lots)
          const parkedLots = [];
          if (Array.isArray(g.parked_from_lots) && g.parked_from_lots.length > 0) {
            for (const pl of g.parked_from_lots) {
              const pLotNo = String(pl.from_lot_no || "").trim();
              const pQty   = Math.trunc(Number(pl.qty));
              if (!pLotNo || pQty <= 0) continue;

              // ✅ Cross-TK: หา owner TK ของ parked lot
              const plOwnerTk = (await getLotOwnerTk(tx, pLotNo)) || master_tk_id;
              if (plOwnerTk !== master_tk_id) {
                await unParkCrossTkLot(tx, plOwnerTk, pLotNo);
              }

              const pPartId = await getPartIdByLotNo(tx, plOwnerTk, pLotNo)
                           || await getPartIdByLotNo(tx, master_tk_id, pLotNo);
              if (!pPartId) { await tx.rollback(); return res.status(400).json({ message: `parked lot "${pLotNo}" part_id not found in TKRunLog`, actor }); }

              const { run_no: p_run, lot_no: p_lot } = await genNewLot(tx, master_tk_id, pPartId, actor.u_id);

              await insertTransfer(tx, {
                from_tk_id: plOwnerTk, to_tk_id: master_tk_id,  // ✅ cross-TK aware
                from_lot_no: pLotNo, to_lot_no: p_lot,
                tf_rs_code: 3, transfer_qty: pQty,
                op_sc_id, MC_id: MC_id_val,
                op_sta_id: actorSta,
                lot_parked_status: 1,
                created_by_u_id: actor.u_id, transfer_ts: now,
              });
              parkedLots.push({ run_no: p_run, lot_no: p_lot, qty: pQty, parked_at_sta: actorSta });
            }
          }

          await new sql.Request(tx)
            .input("tk_id",   sql.VarChar(20),  master_tk_id)
            .input("part_id", sql.Int,           Number(outPart.part_id))
            .input("lot_no",  sql.NVarChar(300), lot_no)
            .query(`UPDATE ${SAFE_TKDETAIL} SET part_id=@part_id, lot_no=@lot_no WHERE tk_id=@tk_id`);

          created_children.push({
            group: gNum, tf_rs_code: 3,
            out_part_no: outPart.part_no, out_part_name: outPart.part_name ?? null,
            to_lot_no: lot_no,
            merge_qty: g.qty,
            merged_from: g.merge_lots.map(m => ({ from_lot_no: m.from_lot_no, qty: Math.trunc(Number(m.qty)) })),
            lots:        [{ run_no, lot_no, qty: g.qty }],
            parked_lots: parkedLots,
          });
        }
      }

      // ⑦.pre — UPDATE lot_parked_status=1 สำหรับ leaf lots ที่ไม่ถูกใช้ใน finish นี้
      // Business Rule: หลัง finish ถ้ามี leaf lot ที่ไม่ได้เป็น from_lot ใน groups นี้
      //   → UPDATE lot_parked_status = 1 ในแถวเดิม (ไม่สร้าง row ใหม่)
      //   → lot นั้นจะโผล่ในหน้า "Lot ที่พักไว้" ให้ TK อื่นมาหยิบใช้ได้
      {
        const usedFromLots = new Set();
        for (const g of groups) {
          const tf = Number(g.tf_rs_code);
          if (tf === 1 || tf === 2) {
            const lot = g.from_lot_no ? String(g.from_lot_no).trim() : null;
            if (lot) usedFromLots.add(lot);
          }
          if (tf === 3) {
            for (const m of g.merge_lots) {
              usedFromLots.add(String(m.from_lot_no).trim());
            }
          }
        }

        // ดึง leaf lots ของ TK นี้ที่ยัง active (lot_parked_status=0)
        // ยกเว้น lot ที่เพิ่งสร้างใน finish นี้ (op_sc_id เดียวกัน)
        const leafR = await new sql.Request(tx)
          .input("tk_id",    sql.VarChar(20), master_tk_id)
          .input("op_sc_id", sql.Char(12),    op_sc_id)
          .query(`
            SELECT DISTINCT t.to_lot_no
            FROM ${SAFE_TRANSFER} t WITH (NOLOCK)
            WHERE t.from_tk_id        = @tk_id
              AND t.lot_parked_status = 0
              AND t.op_sc_id         != @op_sc_id
              AND t.to_lot_no NOT IN (
                SELECT DISTINCT from_lot_no
                FROM ${SAFE_TRANSFER} WITH (NOLOCK)
                WHERE from_tk_id   = @tk_id
                  AND from_lot_no IS NOT NULL
              )
          `);

        for (const lr of leafR.recordset || []) {
          const lotNo = (lr.to_lot_no || '').trim();
          if (!lotNo || usedFromLots.has(lotNo)) continue;

          // ✅ UPDATE แถวเดิม — ไม่สร้าง row ใหม่ / ไม่มี FK ปัญหา
        await new sql.Request(tx)
  .input("tk_id",    sql.VarChar(20),  master_tk_id)
  .input("lot_no",   sql.NVarChar(300), lotNo)
  .input("op_sta_id", sql.VarChar(20), actorSta)
  .query(`
    UPDATE ${SAFE_TRANSFER}
    SET lot_parked_status = 1,
        op_sta_id         = @op_sta_id   -- ✅ อัปเดต station ที่พักจริง
    WHERE from_tk_id        = @tk_id
      AND to_lot_no         = @lot_no
      AND lot_parked_status = 0
  `);

          console.log(`[MARK_PARKED] lot=${lotNo} tk=${master_tk_id} sta=${actorSta}`);
        }
      }

      // ⑦ update op_scan finish
      await new sql.Request(tx)
        .input("op_sc_id",   sql.Char(12),     op_sc_id)
        .input("total_qty",  sql.Int,           Math.trunc(total_qty))
        .input("good_qty",   sql.Int,           Math.trunc(good_qty))
        .input("scrap_qty",  sql.Int,           Math.trunc(scrap_qty))
        .input("tf_rs_code", sql.Int,           Number(groups[groups.length - 1].tf_rs_code))
        .input("lot_no",     sql.NVarChar(300), first_lot_no || "")
        .input("op_sta_id",  sql.VarChar(20),   actorSta)
        .input("finish_ts",  sql.DateTime2(3),  now)
        .query(`
          UPDATE ${SAFE_OPSCAN}
          SET op_sc_total_qty=@total_qty, op_sc_good_qty=@good_qty,
              op_sc_scrap_qty=@scrap_qty, tf_rs_code=@tf_rs_code,
              lot_no=@lot_no,
              op_sta_id=COALESCE(op_sta_id, @op_sta_id),
              op_sc_finish_ts=@finish_ts
          WHERE op_sc_id=@op_sc_id
        `);

      // ⑧ update TKHead/TKDetail status
      const isFinishAtSTA007 = actorSta === "STA007";
      const newTkStatus      = isFinishAtSTA007 ? 1 : 2;
      for (const tbl of ["dbo.TKHead", SAFE_TKDETAIL]) {
        await new sql.Request(tx)
          .input("tk_id",     sql.VarChar(20), master_tk_id)
          .input("tk_status", sql.Int,         newTkStatus)
          .query(`UPDATE ${tbl} SET tk_status=@tk_status WHERE tk_id=@tk_id`);
      }

      await tx.commit();
      console.log(`[OPSCAN_FINISH][OK] op_sc_id=${op_sc_id} tk_id=${master_tk_id} sta=${actorSta} good=${Math.trunc(good_qty)} scrap=${Math.trunc(scrap_qty)}`);

      return res.json({
        message: "Finished",
        actor:   { u_id: actor.u_id, u_name: actor.u_name, role: actor.role },
        op_sc_id, tk_id: master_tk_id,
        op_sta_id: actorSta, op_sta_name: actor.op_sta_name ?? null,
        MC_id: row.MC_id ?? null,
        op_sc_total_qty: Math.trunc(total_qty),
        op_sc_good_qty:  Math.trunc(good_qty),
        op_sc_scrap_qty: Math.trunc(scrap_qty),
        tk_status:   newTkStatus,
        is_finished: isFinishAtSTA007,
        created_groups_count: created_children.length,
        created_groups:       created_children,
        op_sc_ts:        row.op_sc_ts ? new Date(row.op_sc_ts).toISOString() : null,
        op_sc_finish_ts: now.toISOString(),
      });
    } catch (e) { await tx.rollback(); throw e; }
  } catch (err) {
    console.error("[OPSCAN_FINISH][ERROR]", err);
    return res.status(500).json({ message: "Finish failed", actor, error: err.message });
  }
};