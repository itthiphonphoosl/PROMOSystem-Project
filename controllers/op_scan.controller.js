// controllers/op_scan.controller.js
// ──────────────────────────────────────────
// มีแค่ 2 function: startOpScan, finishOpScan
// GET endpoints อยู่ใน op_scan_query.controller.js
// ──────────────────────────────────────────
const { getPool } = require("../config/db");

const OP_SCAN_TABLE  = process.env.OP_SCAN_TABLE  || process.env.OPSCAN_TABLE || "dbo.op_scan";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";
const MACHINE_TABLE  = process.env.MACHINE_TABLE  || "dbo.machine";
const PART_TABLE     = process.env.PART_TABLE     || "dbo.part";
const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";
const TKRUNLOG_TABLE = process.env.TKRUNLOG_TABLE || "dbo.TKRunLog";

// MySQL: strip schema prefix (dbo.) and wrap in backticks
function safeTableName(fullName) {
  const s = String(fullName || "").replace(/^[A-Za-z0-9_]+\./, "");
  if (!/^[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return `\`${s}\``;
}

const SAFE_OPSCAN   = safeTableName(OP_SCAN_TABLE);
const SAFE_TKDETAIL = safeTableName(TKDETAIL_TABLE);
const SAFE_MACHINE  = safeTableName(MACHINE_TABLE);
const SAFE_PART     = safeTableName(PART_TABLE);
const SAFE_TRANSFER = safeTableName(TRANSFER_TABLE);
const SAFE_RUNLOG   = safeTableName(TKRUNLOG_TABLE);

// ── helpers ──────────────────────────────────────────────
function pad(n, len) { return String(n).padStart(len, "0"); }
function yymmdd(d) {
  const yy = String(d.getFullYear()).slice(-2);
  const mm = pad(d.getMonth() + 1, 2);
  const dd = pad(d.getDate(), 2);
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
    u_firstname: req.user?.u_firstname ?? "",
    u_lastname:  req.user?.u_lastname  ?? "",
    role:        req.user?.role        ?? "unknown",
    u_type:      req.user?.u_type      ?? "unknown",
    op_sta_id:   req.user?.op_sta_id   ?? null,
    op_sta_name: req.user?.op_sta_name ?? null,
    clientType:  normalizeClientType(req.headers["x-client-type"]),
  };
}
function forbid(res, msg, actor) { return res.status(403).json({ message: msg, actor }); }

async function genOpScId(conn, now) {
  const prefix = `SC${yymmdd(now)}`;
  const [rows] = await conn.query(
    `SELECT op_sc_id FROM ${SAFE_OPSCAN}
     WHERE op_sc_id LIKE ?
     ORDER BY op_sc_id DESC
     LIMIT 1
     FOR UPDATE`,
    [`${prefix}%`]
  );
  let running = 1;
  if (rows.length) {
    const n = parseInt(String(rows[0].op_sc_id).slice(prefix.length), 10);
    if (!Number.isNaN(n)) running = n + 1;
  }
  return `${prefix}${pad(running, 4)}`;
}

async function getPartByNo(conn, part_no) {
  const [rows] = await conn.query(
    `SELECT part_id, part_no, part_name FROM ${SAFE_PART} WHERE part_no = ? LIMIT 1`,
    [String(part_no).trim()]
  );
  return rows[0] ?? null;
}

// ดึง part_id จาก lot_no ใน TKRunLog
async function getPartIdByLotNo(conn, tk_id, lot_no) {
  const [rows] = await conn.query(
    `SELECT part_id FROM ${SAFE_RUNLOG}
     WHERE tk_id = ? AND lot_no = ?
     LIMIT 1`,
    [tk_id, lot_no]
  );
  return rows[0]?.part_id ?? null;
}

async function genNewLot(conn, tk_id, part_id, actor_u_id) {
  await conn.query(
    `CALL usp_TKRunLog_Create(?, ?, ?, @run_no, @lot_no)`,
    [tk_id, Number(part_id), Number(actor_u_id)]
  );
  const [[outRow]] = await conn.query(`SELECT @run_no AS run_no, @lot_no AS lot_no`);
  const run_no = outRow?.run_no;
  const lot_no = outRow?.lot_no;
  if (!run_no || !lot_no) throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");
  return { run_no: String(run_no).trim(), lot_no: String(lot_no) };
}

// หา TK owner ของ lot จาก TKRunLog — รองรับ cross-TK
async function getLotOwnerTk(conn, lot_no) {
  const [rows] = await conn.query(
    `SELECT tk_id FROM ${SAFE_RUNLOG} WHERE lot_no = ? LIMIT 1`,
    [String(lot_no || "").trim()]
  );
  return rows[0]?.tk_id ? String(rows[0].tk_id).trim() : null;
}

// ✅ [FIX BUG2] unPark lot — ใช้ได้ทั้ง same-TK และ cross-TK
// เดิมชื่อ unParkCrossTkLot และมีเงื่อนไข if (ownerTk !== master_tk_id)
// แก้: เรียกได้เสมอ ไม่ต้องเช็ค TK ownership
async function unParkLot(conn, lot_no) {
  await conn.query(
    `UPDATE ${SAFE_TRANSFER}
     SET lot_parked_status = 0
     WHERE to_lot_no         = ?
       AND lot_parked_status = 1`,
    [String(lot_no).trim()]
  );
}

// INSERT 1 row ใน t_transfer
async function insertTransfer(conn, {
  from_tk_id, to_tk_id, from_lot_no, to_lot_no,
  tf_rs_code, transfer_qty, op_sc_id, MC_id, op_sta_id,
  lot_parked_status, created_by_u_id, transfer_ts, color_id
}) {
  await conn.query(
    `INSERT INTO ${SAFE_TRANSFER}
       (from_tk_id, to_tk_id, from_lot_no, to_lot_no,
        tf_rs_code, transfer_qty, op_sc_id, MC_id,
        op_sta_id, lot_parked_status,
        created_by_u_id, transfer_ts, color_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    [
      from_tk_id, to_tk_id, from_lot_no || "", to_lot_no,
      tf_rs_code, transfer_qty, op_sc_id, MC_id ?? null,
      op_sta_id ?? null, lot_parked_status ?? 0,
      Number(created_by_u_id), transfer_ts, color_id ?? null,
    ]
  );
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
  if (!tk_id) return res.status(400).json({ message: "กรุณากรอก Tracking No.", actor });
  if (!MC_id) return res.status(400).json({ message: "กรุณาเลือก Machine", actor });

  const pool = getPool();
  const conn = await pool.getConnection();

  try {
    await conn.query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    await conn.beginTransaction();

    try {
      const now = new Date();

      // ① TKHead status check
      const [headRows] = await conn.query(
        `SELECT tk_status, tk_active FROM \`TKHead\` WHERE tk_id = ? LIMIT 1`,
        [tk_id]
      );
      const headRow    = headRows[0];
      const headStatus = Number(headRow?.tk_status ?? -1);
      if (headStatus === -1) {
        await conn.rollback(); conn.release();
        return res.status(404).json({ message: "ไม่พบ Tracking No. นี้ในระบบ", actor, tk_id });
      }
      if (Number(headRow?.tk_active) !== 1) {
        await conn.rollback(); conn.release();
        return res.status(403).json({ message: "เอกสารนี้ถูกปิดใช้งานอยู่ กรุณาติดต่อ Admin", actor, tk_id, tk_active: Number(headRow?.tk_active) });
      }
      if (headStatus === 4) {
        await conn.rollback(); conn.release();
        return res.status(403).json({ message: "เอกสาร Tracking No. นี้ถูก Cancel ไปแล้ว", actor, tk_id });
      }
      if (headStatus === 1) {
        await conn.rollback(); conn.release();
        return res.status(403).json({ message: "Tracking No. นี้ดำเนินการเสร็จสิ้นแล้ว ไม่สามารถเริ่มงานได้", actor, tk_id });
      }

      // ② TKDetail
      const [tkDetailRows] = await conn.query(
        `SELECT d.tk_id, d.part_id, p.part_no, p.part_name, d.lot_no,
                d.tk_status, d.tk_created_at_ts
         FROM ${SAFE_TKDETAIL} d
         LEFT JOIN \`part\` p ON p.part_id = d.part_id
         WHERE d.tk_id = ?
         ORDER BY d.tk_created_at_ts DESC
         LIMIT 1
         FOR UPDATE`,
        [tk_id]
      );
      const tkDoc = tkDetailRows[0];
      if (!tkDoc) {
        await conn.rollback(); conn.release();
        return res.status(404).json({ message: "ไม่พบข้อมูล TKDetail ของ Tracking No. นี้", actor, tk_id });
      }

      // ③ base lot (จาก TKRunLog)
      const [allLotRows] = await conn.query(
        `SELECT r.run_no, r.lot_no, p.part_no, p.part_name, r.created_at_ts
         FROM \`TKRunLog\` r
         LEFT JOIN \`part\` p ON p.part_id = r.part_id
         WHERE r.tk_id = ?
         ORDER BY r.created_at_ts DESC`,
        [tk_id]
      );
      const allLots    = allLotRows || [];
      const baseLotRow = allLots.length ? allLots[allLots.length - 1] : null;
      const base_lot_no = baseLotRow?.lot_no ? String(baseLotRow.lot_no).trim() : null;
      const base_run_no = baseLotRow?.run_no ? String(baseLotRow.run_no).trim() : null;

      // ③.b leaf lots
      // ✅ [FIX BUG3] ใช้ MAX(transfer_id) dedup — ดึงเฉพาะ latest row ต่อ to_lot_no
      // เดิม: SELECT ทุก row → lot ที่มีทั้ง parked=0 เก่า + parked=1 ใหม่ เข้า current_lots ผิด
      const [tRows2] = await conn.query(
        `SELECT t.from_lot_no, t.to_lot_no, t.lot_parked_status
         FROM ${SAFE_TRANSFER} t
         INNER JOIN (
           SELECT to_lot_no, MAX(transfer_id) AS max_id
           FROM ${SAFE_TRANSFER}
           WHERE from_tk_id = ? OR to_tk_id = ?
           GROUP BY to_lot_no
         ) latest
           ON latest.to_lot_no = t.to_lot_no
          AND latest.max_id    = t.transfer_id`,
        [tk_id, tk_id]
      );
      const tRows   = tRows2 || [];
      // fromSet: lots ที่ถูก "ใช้ไป" (เป็น from ในการสร้าง lot ใหม่ที่ต่างออกไป)
      const fromSet = new Set(
        tRows
          .filter(r => (r.from_lot_no || "").trim() !== (r.to_lot_no || "").trim())
          .map(r => (r.from_lot_no || "").trim())
          .filter(Boolean)
      );
      // selfRefLots: lots ที่ from=to (Master-ID) → ยังมีชีวิตอยู่ ถือเป็น leaf เสมอ
      const selfRefLots = new Set(
        tRows
          .filter(r => (r.lot_parked_status === 0 || r.lot_parked_status === false)
                    && (r.from_lot_no || "").trim() === (r.to_lot_no || "").trim()
                    && (r.to_lot_no || "").trim())
          .map(r => (r.to_lot_no || "").trim())
      );
      const leafLots = tRows
        .filter(r => r.lot_parked_status === 0 || r.lot_parked_status === false)
        .map(r => (r.to_lot_no || "").trim())
        .filter(lot => lot && (!fromSet.has(lot) || selfRefLots.has(lot)));
      const leafLotSet = [...new Set(leafLots)];

      // ③.c ตรวจว่า lot ปัจจุบันของ tk นี้เป็น Parked Lot หรือไม่
      //     lot พัก = lot_parked_status=1 → ห้ามเริ่มงาน จนกว่าจะถูก Co-ID นำไปใช้
      if (leafLotSet.length > 0) {
        const [parkedCheckRows] = await conn.query(
          `SELECT to_lot_no, op_sta_id
           FROM ${SAFE_TRANSFER}
           WHERE to_tk_id = ?
             AND to_lot_no IN (${leafLotSet.map(() => '?').join(',')})
             AND lot_parked_status = 1
           LIMIT 1`,
          [tk_id, ...leafLotSet]
        );
        if (parkedCheckRows[0]) {
          const parkedLotNo = parkedCheckRows[0].to_lot_no;
          const parkedSta   = parkedCheckRows[0].op_sta_id ?? '-';
          await conn.rollback(); conn.release();
          return res.status(403).json({
            message: `Lot "${parkedLotNo}" ถูกพักไว้ที่ ${parkedSta} ไม่สามารถเริ่มงานได้ จนกว่าจะถูกนำไปใช้ใน Co-ID`,
            actor, tk_id,
            parked_lot_no: parkedLotNo,
            parked_at_sta: parkedSta,
          });
        }
      }

      // ④ กัน STA007 finished
      const [finishedRows] = await conn.query(
        `SELECT op_sc_id FROM ${SAFE_OPSCAN}
         WHERE tk_id = ? AND op_sta_id = 'STA007' AND op_sc_finish_ts IS NOT NULL
         LIMIT 1`,
        [tk_id]
      );
      if (finishedRows[0]) {
        await conn.rollback(); conn.release();
        return res.status(403).json({ message: "Already FINISHED at STA007.", actor, tk_id });
      }

      // ⑤ กัน station ย้อน
      const [lastFinRows] = await conn.query(
        `SELECT s.op_sta_id, st.op_sta_name
         FROM ${SAFE_OPSCAN} s
         LEFT JOIN \`op_station\` st ON st.op_sta_id = s.op_sta_id
         WHERE s.tk_id = ? AND s.op_sc_finish_ts IS NOT NULL AND s.op_sta_id IS NOT NULL
         ORDER BY s.op_sc_finish_ts DESC
         LIMIT 1`,
        [tk_id]
      );
      const lastFinSta = lastFinRows[0];
      if (lastFinSta) {
        const staNum = (sta) => parseInt(String(sta).replace("STA", ""), 10);
        const lastNum    = staNum(lastFinSta.op_sta_id);
        const currentNum = staNum(op_sta_id);
        if (Number.isFinite(lastNum) && Number.isFinite(currentNum) && currentNum <= lastNum) {
          const [nextStaRows] = await conn.query(
            `SELECT op_sta_id, op_sta_name FROM \`op_station\`
             WHERE CAST(REPLACE(op_sta_id,'STA','') AS UNSIGNED) > ? AND op_sta_active = 1
             ORDER BY CAST(REPLACE(op_sta_id,'STA','') AS UNSIGNED) ASC
             LIMIT 1`,
            [lastNum]
          );
          const nextSta = nextStaRows[0];
          await conn.rollback(); conn.release();
          return res.status(403).json({
            message: nextSta
              ? `ทำถึง ${lastFinSta.op_sta_id} แล้ว ❌ ห้ามเริ่มที่ ${op_sta_id} ✅ ให้ไปที่ ${nextSta.op_sta_id} (${nextSta.op_sta_name})`
              : `ทำถึง ${lastFinSta.op_sta_id} แล้ว ไม่มี Station ถัดไป`,
            actor, tk_id,
            last_finished_sta:       lastFinSta.op_sta_id,
            suggested_next_sta:      nextSta?.op_sta_id    ?? null,
            suggested_next_sta_name: nextSta?.op_sta_name  ?? null,
          });
        }
      }

      // ⑥ validate machine
      const [mcRows] = await conn.query(
        `SELECT MC_id, MC_name, op_sta_id FROM ${SAFE_MACHINE}
         WHERE MC_id = ? AND MC_active = 1
         LIMIT 1`,
        [MC_id]
      );
      const mcRow = mcRows[0];
      if (!mcRow) {
        await conn.rollback(); conn.release();
        return res.status(400).json({ message: "ไม่พบ Machine หรือ Machine ถูกปิดใช้งาน", actor, MC_id });
      }
      if (String(mcRow.op_sta_id || "").trim() !== op_sta_id) {
        await conn.rollback(); conn.release();
        return res.status(403).json({
          message: `Machine ${MC_id} ไม่ได้อยู่ใน station ของคุณ (${op_sta_id}). Machine อยู่ที่ ${mcRow.op_sta_id ?? "ไม่มี"}`,
          actor, MC_id,
        });
      }

      // ⑦ กัน active scan ซ้ำ — เริ่มที่ใครต้องเสร็จที่คนนั้น
      const [activeRows] = await conn.query(
        `SELECT s.op_sc_id, s.op_sta_id, st.op_sta_name, s.u_id,
                u.u_firstname, u.u_lastname
         FROM ${SAFE_OPSCAN} s
         LEFT JOIN \`user\`       u  ON u.u_id       = s.u_id
         LEFT JOIN \`op_station\` st ON st.op_sta_id = s.op_sta_id
         WHERE s.tk_id = ? AND s.op_sc_finish_ts IS NULL
         ORDER BY s.op_sc_ts DESC
         LIMIT 1
         FOR UPDATE`,
        [tk_id]
      );
      if (activeRows[0]) {
        const activeScan = activeRows[0];
        const activeUId  = Number(activeScan.u_id);
        const actorUId   = Number(actor.u_id);

        if (activeUId === actorUId) {
          // คนเดียวกัน → return scan เดิมได้เลย (reload หน้า finish)
          await conn.rollback(); conn.release();
          return res.status(200).json({
            message: "Lot No. นี้กำลังทำอยู่ในหน้า Active Scan", actor, tk_id,
            op_sc_id: activeScan.op_sc_id,
            tk_id,
            op_sta_id,
            MC_id,
            actor,
            resuming: true,
          });
        }

        // คนอื่น → block พร้อมบอกชื่อ + station ที่กำลังทำอยู่
        await conn.rollback(); conn.release();
        const ownerName    = [activeScan.u_firstname, activeScan.u_lastname]
          .filter(Boolean).join(' ') || `u_id ${activeUId}`;
        const activeSta    = activeScan.op_sta_id   ? String(activeScan.op_sta_id).trim()   : null;
        const activeStaName = activeScan.op_sta_name ? String(activeScan.op_sta_name).trim() : null;
        return res.status(409).json({
          message: `งานนี้กำลังทำอยู่โดย ${ownerName} ที่ ${activeSta ?? "-"} กรุณารอให้เสร็จก่อน`,
          actor, tk_id,
          locked_by: {
            u_id:       activeUId,
            name:       ownerName,
            op_sta_id:   activeSta,
            op_sta_name: activeStaName,
          },
          op_sc_id: activeScan.op_sc_id,
        });
      }

      // ⑧ gen op_sc_id + INSERT
      const op_sc_id = await genOpScId(conn, now);
      await conn.query(
        `INSERT INTO ${SAFE_OPSCAN}
           (op_sc_id, tk_id, op_sta_id, MC_id, u_id,
            op_sc_total_qty, op_sc_scrap_qty, op_sc_good_qty,
            tf_rs_code, lot_no, op_sc_ts, op_sc_finish_ts)
         VALUES (?, ?, ?, ?, ?, 0, 0, 0, NULL, ?, ?, NULL)`,
        [op_sc_id, tk_id, op_sta_id, MC_id, Number(actor.u_id), tkDoc.lot_no || null, now]
      );

      // ⑨ update TKDetail + TKHead status = 3 (IN_PROGRESS)
      await conn.query(
        `UPDATE ${SAFE_TKDETAIL} SET MC_id=?, op_sta_id=?, op_sc_id=? WHERE tk_id=?`,
        [MC_id, op_sta_id, op_sc_id, tk_id]
      );
      for (const tbl of ["`TKHead`", SAFE_TKDETAIL]) {
        await conn.query(
          `UPDATE ${tbl} SET tk_status=3 WHERE tk_id=? AND tk_status<>1`,
          [tk_id]
        );
      }

      await conn.commit();
      conn.release();
      console.log(`[OPSCAN_START][OK] op_sc_id=${op_sc_id} tk_id=${tk_id} op_sta_id=${op_sta_id} MC_id=${MC_id}`);

      return res.status(201).json({
        message: "Started",
        actor:   { u_id: actor.u_id, u_firstname: actor.u_firstname, u_lastname: actor.u_lastname, role: actor.role },
        op_sc_id, op_sta_id, op_sta_name: actor.op_sta_name ?? null, MC_id,
        op_sc_total_qty: 0,
        base_lot_no, base_run_no,
        tk_doc: {
          tk_id:      tkDoc.tk_id,
          part_id:    tkDoc.part_id,
          part_no:    tkDoc.part_no,
          part_name:  tkDoc.part_name,
          op_sta_id,  op_sta_name: actor.op_sta_name ?? null,
          MC_id,      MC_name: mcRow?.MC_name ?? null,
          tk_status:  3,
        },
        current_lots: leafLotSet.length > 0
          ? leafLotSet.map(lot => {
              const runRow = allLots.find(r => (r.lot_no || "").trim() === lot);
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
    } catch (e) {
      await conn.rollback(); conn.release(); throw e;
    }
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

  const op_sc_id  = String(req.body.op_sc_id || "").trim();
  const good_qty  = Math.abs(Number(req.body.good_qty));
  const scrap_qty = Math.abs(Number(req.body.scrap_qty));
  const total_qty = good_qty + scrap_qty;
  const groups    = Array.isArray(req.body.groups) ? req.body.groups : [];
  const _safeColorId = (v) => (v != null && Number.isFinite(Number(v))) ? Number(v) : null;

  if (!op_sc_id) return res.status(400).json({ message: "op_sc_id is required", actor });
  if (!Number.isFinite(good_qty) || !Number.isFinite(scrap_qty))
    return res.status(400).json({ message: "good_qty/scrap_qty must be numbers", actor });
  if (good_qty === 0 && scrap_qty === 0)
    return res.status(400).json({ message: "good_qty and scrap_qty cannot both be 0", actor });
  if (groups.length === 0)
    return res.status(400).json({ message: "groups[] is required", actor });

  // ── validate groups ────────────────────
  for (let i = 0; i < groups.length; i++) {
    const g   = groups[i];
    const tf  = Number(g.tf_rs_code);
    const num = i + 1;

    if (![1, 2, 3].includes(tf))
      return res.status(400).json({ message: `groups[${num}]: tf_rs_code must be 1, 2, or 3`, actor });

    if (tf === 1) {
      if (!Number.isFinite(Number(g.qty)) || Number(g.qty) <= 0)
        return res.status(400).json({ message: `groups[${num}]: qty is required for tf=1`, actor });
      if (!String(g.out_part_no || "").trim())
        return res.status(400).json({ message: `groups[${num}]: out_part_no is required`, actor });
    }

    if (tf === 2) {
      if (!Number.isFinite(Number(g.qty)) || Number(g.qty) <= 0)
        return res.status(400).json({ message: `groups[${num}]: qty is required for tf=2`, actor });
      if (!String(g.out_part_no || "").trim())
        return res.status(400).json({ message: `groups[${num}]: out_part_no is required`, actor });
      if (!String(g.from_lot_no || "").trim())
        return res.status(400).json({ message: `Group ${num}: กรุณาเลือก From Lot ก่อนบันทึก (Split-ID ต้องระบุ Lot ต้นทาง)`, actor });
      if (Array.isArray(g.splits) && g.splits.length > 0) {
        let sumSplit = 0;
        for (const s of g.splits) {
          if (!String(s.out_part_no || "").trim())
            return res.status(400).json({ message: `groups[${num}] split: out_part_no is required`, actor });
          if (!Number.isFinite(Number(s.qty)) || Number(s.qty) <= 0)
            return res.status(400).json({ message: `groups[${num}] split: qty must be > 0`, actor });
          sumSplit += Math.trunc(Number(s.qty));
        }
        if (sumSplit > Math.trunc(Number(g.qty)))
          return res.status(400).json({
            message: `groups[${num}]: sum splits (${sumSplit}) เกิน group qty (${Math.trunc(Number(g.qty))})`, actor
          });
      }
    }

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
      g.qty = g.merge_lots.reduce((a, m) => a + Math.trunc(Number(m.qty)), 0);
    }
  }

  // ── กัน from_lot_no ซ้ำข้ามกลุ่ม ────────────────
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

  const totalGroupQty = groups.reduce((a, g) => a + Math.trunc(Number(g.qty)), 0);
  if (totalGroupQty !== Math.trunc(good_qty))
    return res.status(400).json({
      message: `⚠️ เช็คจำนวนรวมทุก Group ตอนนี้ได้ [${totalGroupQty}] ชิ้น\nแต่จำนวน OK มี [${Math.trunc(good_qty)}] ชิ้น — กรุณาตรวจสอบ`,
      actor,
    });

  const pool = getPool();
  const conn = await pool.getConnection();

  try {
    await conn.query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
    await conn.beginTransaction();

    try {
      const now = new Date();

      // ① lock op_scan row
      const [scanRows] = await conn.query(
        `SELECT * FROM ${SAFE_OPSCAN} WHERE op_sc_id = ? LIMIT 1 FOR UPDATE`,
        [op_sc_id]
      );
      const row = scanRows[0];
      if (!row) {
        await conn.rollback(); conn.release();
        return res.status(404).json({ message: "op_sc_id not found", actor, op_sc_id });
      }
      if (row.op_sc_finish_ts) {
        await conn.rollback(); conn.release();
        return res.status(409).json({ message: "Already finished", actor, op_sc_id });
      }

      // ② station lock
      const actorSta = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";
      if (!actorSta) {
        await conn.rollback(); conn.release();
        return res.status(400).json({ message: "Missing op_sta_id in token", actor });
      }
      const scanSta = row.op_sta_id ? String(row.op_sta_id).trim() : "";
      if (scanSta && scanSta !== actorSta) {
        await conn.rollback(); conn.release();
        return res.status(403).json({ message: `Scan started at ${scanSta} but you login as ${actorSta}`, actor, op_sc_id });
      }

      // เริ่มที่ใครต้องเสร็จที่คนนั้น
      if (Number(row.u_id) !== Number(actor.u_id)) {
        const [ownerRows] = await conn.query(
          `SELECT u_firstname, u_lastname FROM \`user\` WHERE u_id = ? LIMIT 1`,
          [row.u_id]
        );
        const ownerName = ownerRows[0]
          ? [ownerRows[0].u_firstname, ownerRows[0].u_lastname].filter(Boolean).join(' ')
          : `u_id ${row.u_id}`;
        await conn.rollback(); conn.release();
        return res.status(403).json({
          message: `งานนี้เริ่มโดย ${ownerName} ต้องให้ ${ownerName} กด Finish เท่านั้น`,
          actor, op_sc_id,
          locked_by: { u_id: row.u_id, name: ownerName },
        });
      }

      const master_tk_id = String(row.tk_id || "").trim();

      // ③ TKHead ต้องไม่ FINISHED
      const [headRows] = await conn.query(
        `SELECT tk_status, tk_active FROM \`TKHead\` WHERE tk_id = ? LIMIT 1`,
        [master_tk_id]
      );
      if (Number(headRows[0]?.tk_active) !== 1) {
        await conn.rollback(); conn.release();
        return res.status(403).json({ message: "เอกสารนี้ถูกปิดใช้งานอยู่ กรุณาติดต่อ Admin", actor, tk_id: master_tk_id, tk_active: Number(headRows[0]?.tk_active) });
      }
      if (Number(headRows[0]?.tk_status) === 1) {
        await conn.rollback(); conn.release();
        return res.status(403).json({ message: "This tk_id is FINISHED. Cannot finish again.", actor, tk_id: master_tk_id });
      }

      // ④ base lot
      const [baseDetailRows] = await conn.query(
        `SELECT lot_no, part_id FROM ${SAFE_TKDETAIL}
         WHERE tk_id = ?
         ORDER BY tk_created_at_ts DESC
         LIMIT 1`,
        [master_tk_id]
      );
      const baseDetail   = baseDetailRows[0];
      const base_lot_no  = baseDetail?.lot_no ? String(baseDetail.lot_no).trim() : null;
      const base_part_id = baseDetail?.part_id ?? null;

      // ⑤ validate from_lot_no + parked lot station check
      {
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
          const lotOwnerTk = await getLotOwnerTk(conn, lot);
          if (!lotOwnerTk) {
            await conn.rollback(); conn.release();
            return res.status(400).json({ message: `${gLabel}: from_lot_no "${lot}" ไม่พบใน TKRunLog`, actor });
          }

          const [parkedRows] = await conn.query(
            `SELECT op_sta_id, lot_parked_status
             FROM ${SAFE_TRANSFER}
             WHERE to_lot_no = ?
             ORDER BY transfer_ts DESC
             LIMIT 1`,
            [lot]
          );
          const pRow     = parkedRows[0];
          const isParked = pRow?.lot_parked_status === true || pRow?.lot_parked_status === 1;

          if (isParked) {
            if (gLabel.startsWith("tf=1") || gLabel.startsWith("tf=2")) {
              await conn.rollback(); conn.release();
              return res.status(400).json({
                message: `${gLabel}: lot "${lot}" เป็น Parked Lot — สามารถใช้ได้เฉพาะ Co-ID (tf=3) เท่านั้น`,
                detail:  "Master-ID และ Split-ID ต้องใช้ Active Lot เท่านั้น",
                lot_no:  lot, actor,
              });
            }
            const parkedSta = pRow?.op_sta_id ? String(pRow.op_sta_id).trim() : "";
            if (parkedSta && parkedSta !== actorSta) {
              await conn.rollback(); conn.release();
              return res.status(403).json({
                message: `${gLabel}: lot "${lot}" พักอยู่ที่ ${parkedSta} — ไม่สามารถใช้ที่ ${actorSta} ได้`,
                detail:  "Parked lot ต้องใช้ที่ station เดียวกับที่พักไว้เท่านั้น",
                parked_at_sta: parkedSta, your_sta: actorSta, actor,
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
        if (tf === 1) {
          const outPart = await getPartByNo(conn, String(g.out_part_no).trim());
          if (!outPart) {
            await conn.rollback(); conn.release();
            return res.status(400).json({ message: `groups[${gNum}]: out_part_no "${g.out_part_no}" not found`, actor });
          }

          const from_lot = g.from_lot_no ? String(g.from_lot_no).trim() : base_lot_no;
          if (!from_lot) {
            await conn.rollback(); conn.release();
            return res.status(400).json({ message: `groups[${gNum}]: ไม่พบ from_lot_no สำหรับ Master-ID`, actor });
          }

          if (!first_lot_no) first_lot_no = from_lot;

          await conn.query(
            `UPDATE ${SAFE_TKDETAIL} SET part_id=? WHERE tk_id=?`,
            [Number(outPart.part_id), master_tk_id]
          );

          const fromOwnerTk_1 = (await getLotOwnerTk(conn, from_lot)) || master_tk_id;
          // ✅ [FIX BUG2] เรียก unParkLot เสมอ ไม่เช็ค TK ownership
          // เดิม: if (fromOwnerTk_1 !== master_tk_id) { await unParkCrossTkLot(...) }
          // แก้: same-TK ที่ถูก auto-park ก็ต้องถูก unpark เมื่อถูกนำมาใช้
          await unParkLot(conn, from_lot);

          await insertTransfer(conn, {
            from_tk_id: fromOwnerTk_1, to_tk_id: master_tk_id,
            from_lot_no: from_lot, to_lot_no: from_lot,
            tf_rs_code: 1, transfer_qty: group_qty,
            op_sc_id, MC_id: MC_id_val, op_sta_id: actorSta,
            lot_parked_status: 0,
            created_by_u_id: actor.u_id, transfer_ts: now,
            color_id: actorSta === "STA006" ? _safeColorId(g.color_id) : null,
          });

          created_children.push({
            group: gNum, tf_rs_code: 1,
            from_lot_no: from_lot, to_lot_no: from_lot,
            from_tk_id:  fromOwnerTk_1,
            out_part_no: outPart.part_no,
            lots: [{ lot_no: from_lot, out_part_no: outPart.part_no, qty: group_qty }],
            parked_lots: [],
          });
        }

        // ── tf=2 Split-ID ──────────────────────────────
        if (tf === 2) {
          const from_lot = String(g.from_lot_no).trim();
          const splits   = Array.isArray(g.splits) ? g.splits : [];

          const fromOwnerTk_2 = (await getLotOwnerTk(conn, from_lot)) || master_tk_id;
          // ✅ [FIX BUG2] เรียก unParkLot เสมอ ไม่เช็ค TK ownership
          await unParkLot(conn, from_lot);

          const group_qty_tf2 = Math.trunc(Number(g.qty));
          const sumUsed   = splits.reduce((a, s) => a + Math.trunc(Number(s.qty)), 0);
          const parkedQty = group_qty_tf2 - sumUsed;

          const splitLots  = [];
          const parkedLots = [];

          for (let si = 0; si < splits.length; si++) {
            const s = splits[si];
            const outPart = await getPartByNo(conn, String(s.out_part_no).trim());
            if (!outPart) {
              await conn.rollback(); conn.release();
              return res.status(400).json({ message: `groups[${gNum}] split: out_part_no "${s.out_part_no}" not found`, actor });
            }

            const s_qty = Math.trunc(Number(s.qty));
            let run_no, lot_no;

            if (si === 0) {
              lot_no = from_lot;
              const [rnRows] = await conn.query(
                `SELECT run_no FROM \`TKRunLog\`
                 WHERE tk_id = ? AND lot_no = ?
                 LIMIT 1`,
                [master_tk_id, from_lot]
              );
              run_no = rnRows[0]?.run_no ?? null;
            } else {
              const gen = await genNewLot(conn, master_tk_id, outPart.part_id, actor.u_id);
              run_no = gen.run_no;
              lot_no = gen.lot_no;
            }

            if (!first_lot_no) first_lot_no = lot_no;

            await insertTransfer(conn, {
              from_tk_id: fromOwnerTk_2, to_tk_id: master_tk_id,
              from_lot_no: from_lot, to_lot_no: lot_no,
              tf_rs_code: 2, transfer_qty: s_qty,
              op_sc_id, MC_id: MC_id_val, op_sta_id: actorSta,
              lot_parked_status: 0,
              created_by_u_id: actor.u_id, transfer_ts: now,
              color_id: actorSta === "STA006" ? _safeColorId(s.color_id) : null,
            });
            splitLots.push({ run_no, lot_no, out_part_no: outPart.part_no, qty: s_qty });
          }

          if (parkedQty > 0) {
            const parkedPartId = from_lot
              ? (await getPartIdByLotNo(conn, fromOwnerTk_2, from_lot) || await getPartIdByLotNo(conn, master_tk_id, from_lot))
              : base_part_id;

            if (!parkedPartId) {
              await conn.rollback(); conn.release();
              return res.status(400).json({ message: `groups[${gNum}]: cannot resolve part_id for parked lot`, actor });
            }

            const { run_no: p_run, lot_no: p_lot } = await genNewLot(conn, master_tk_id, parkedPartId, actor.u_id);

            await insertTransfer(conn, {
              from_tk_id: fromOwnerTk_2, to_tk_id: master_tk_id,
              from_lot_no: from_lot, to_lot_no: p_lot,
              tf_rs_code: 2, transfer_qty: parkedQty,
              op_sc_id, MC_id: MC_id_val, op_sta_id: actorSta,
              lot_parked_status: 1,
              created_by_u_id: actor.u_id, transfer_ts: now,
              color_id: null,
            });
            parkedLots.push({ run_no: p_run, lot_no: p_lot, qty: parkedQty, parked_at_sta: actorSta });
          }

          const latestLot = splitLots.length ? splitLots[splitLots.length - 1].lot_no
                          : parkedLots.length ? parkedLots[0].lot_no : null;
          if (latestLot) {
            await conn.query(
              `UPDATE ${SAFE_TKDETAIL} SET lot_no=? WHERE tk_id=?`,
              [latestLot, master_tk_id]
            );
          }

          created_children.push({
            group: gNum, tf_rs_code: 2,
            from_lot_no: from_lot, from_tk_id: fromOwnerTk_2,
            lots: splitLots, parked_lots: parkedLots,
          });
        }

        // ── tf=3 Co-ID ─────────────────────────────────
        if (tf === 3) {
          const outPart = await getPartByNo(conn, String(g.out_part_no).trim());
          if (!outPart) {
            await conn.rollback(); conn.release();
            return res.status(400).json({ message: `groups[${gNum}]: out_part_no "${g.out_part_no}" not found`, actor });
          }

          const { run_no, lot_no } = await genNewLot(conn, master_tk_id, outPart.part_id, actor.u_id);
          if (!first_lot_no) first_lot_no = lot_no;

          for (const m of g.merge_lots) {
            const mLotNo = String(m.from_lot_no).trim();
            const mergeOwnerTk = (await getLotOwnerTk(conn, mLotNo)) || master_tk_id;
            // ✅ [FIX BUG2] เรียก unParkLot เสมอ ไม่เช็ค TK ownership
            // เดิม: if (mergeOwnerTk !== master_tk_id) { await unParkCrossTkLot(...) }
            // แก้: same-TK lot ที่ถูก auto-park ก็ต้องถูก unpark เมื่อถูกนำมาใช้ใน Co-ID
            await unParkLot(conn, mLotNo);
            await insertTransfer(conn, {
              from_tk_id: mergeOwnerTk, to_tk_id: master_tk_id,
              from_lot_no: mLotNo, to_lot_no: lot_no,
              tf_rs_code: 3, transfer_qty: Math.trunc(Number(m.qty)),
              op_sc_id, MC_id: MC_id_val, op_sta_id: actorSta,
              lot_parked_status: 0,
              created_by_u_id: actor.u_id, transfer_ts: now,
              color_id: actorSta === "STA006" ? _safeColorId(g.color_id) : null,
            });
          }

          const parkedLots = [];
          if (Array.isArray(g.parked_from_lots) && g.parked_from_lots.length > 0) {
            for (const pl of g.parked_from_lots) {
              const pLotNo = String(pl.from_lot_no || "").trim();
              const pQty   = Math.trunc(Number(pl.qty));
              if (!pLotNo || pQty <= 0) continue;

              const plOwnerTk = (await getLotOwnerTk(conn, pLotNo)) || master_tk_id;
              // ✅ [FIX BUG2] เรียก unParkLot เสมอ
              await unParkLot(conn, pLotNo);

              const pPartId = await getPartIdByLotNo(conn, plOwnerTk, pLotNo)
                           || await getPartIdByLotNo(conn, master_tk_id, pLotNo);
              if (!pPartId) {
                await conn.rollback(); conn.release();
                return res.status(400).json({ message: `parked lot "${pLotNo}" part_id not found in TKRunLog`, actor });
              }

              const { run_no: p_run, lot_no: p_lot } = await genNewLot(conn, master_tk_id, pPartId, actor.u_id);

              await insertTransfer(conn, {
                from_tk_id: plOwnerTk, to_tk_id: master_tk_id,
                from_lot_no: pLotNo, to_lot_no: p_lot,
                tf_rs_code: 3, transfer_qty: pQty,
                op_sc_id, MC_id: MC_id_val, op_sta_id: actorSta,
                lot_parked_status: 1,
                created_by_u_id: actor.u_id, transfer_ts: now,
                color_id: actorSta === "STA006" ? _safeColorId(g.color_id) : null,
              });
              parkedLots.push({ run_no: p_run, lot_no: p_lot, qty: pQty, parked_at_sta: actorSta });
            }
          }

          await conn.query(
            `UPDATE ${SAFE_TKDETAIL} SET part_id=?, lot_no=? WHERE tk_id=?`,
            [Number(outPart.part_id), lot_no, master_tk_id]
          );

          created_children.push({
            group: gNum, tf_rs_code: 3,
            out_part_no: outPart.part_no, out_part_name: outPart.part_name ?? null,
            to_lot_no: lot_no, merge_qty: g.qty,
            merged_from: g.merge_lots.map(m => ({ from_lot_no: m.from_lot_no, qty: Math.trunc(Number(m.qty)) })),
            lots: [{ run_no, lot_no, qty: g.qty }],
            parked_lots: parkedLots,
          });
        }
      }

      // ⑦.pre — mark unused leaf lots as parked (INSERT-only, ไม่แตะ row เดิม)
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

        // ✅ [FIX BUG4] กัน lot ที่ถูก Co-ID ไปแล้ว (consumed) ไม่ให้ถูก auto-park ซ้ำ
        //
        //    เดิม (version แรก): NOT EXISTS กว้างเกินไป → เจอ Split row เดิม (from=lot1,to=lot2)
        //          → คิดว่า lot1 consumed แล้ว → ไม่ park lot1 ทั้งที่ควร (bug นี้)
        //
        //    แก้: เช็คเฉพาะ Co-ID (tf=3, parked=0) เท่านั้น
        //         เหตุผล: มีแค่ Co-ID ที่ "กิน" lot จริงๆ (merge เข้า lot ใหม่)
        //         Split: lot1 → lot2,lot3 แต่ lot1 ยังมีชีวิตผ่าน from=lot1,to=lot1 row
        //         Master-ID: from=lot1,to=lot1 ไม่ได้เปลี่ยน lot
        //         Co-ID: from=lot1,to=lot6 (different) → lot1 หายไปจริงๆ
        const [leafRows] = await conn.query(
          `SELECT t.to_lot_no, t.from_lot_no, t.tf_rs_code, t.transfer_qty, t.color_id
           FROM ${SAFE_TRANSFER} t
           INNER JOIN (
             SELECT to_lot_no, MAX(transfer_id) AS max_id
             FROM ${SAFE_TRANSFER}
             WHERE (from_tk_id = ? OR to_tk_id = ?)
             GROUP BY to_lot_no
           ) latest ON latest.to_lot_no = t.to_lot_no AND latest.max_id = t.transfer_id
           WHERE (t.from_tk_id = ? OR t.to_tk_id = ?)
             AND t.lot_parked_status = 0
             AND t.op_sc_id         != ?
             AND NOT EXISTS (
               SELECT 1 FROM ${SAFE_TRANSFER} t2
               WHERE t2.from_lot_no      = t.to_lot_no
                 AND t2.to_lot_no       != t.to_lot_no
                 AND t2.tf_rs_code       = 3
                 AND t2.lot_parked_status = 0
                 AND (t2.from_tk_id = ? OR t2.to_tk_id = ?)
             )`,
          [master_tk_id, master_tk_id, master_tk_id, master_tk_id, op_sc_id, master_tk_id, master_tk_id]
        );

        // ── own-TK: park unused active lots ────────────────
        // ✅ [FIX BUG1] ลบ UPDATE block ออก — ไม่แก้ row เดิมที่เป็น history ของ station ก่อนหน้า
        //    ใช้ INSERT-only เพื่อบันทึก park event ที่ station ปัจจุบัน (actorSta)
        //    เดิมมี UPDATE lot_parked_status=1 → ทำให้ K26-000001's STA002 row ถูกแก้เป็น parked
        //    ซึ่งทำให้ lot โผล่ที่ STA002 แทนที่จะโผล่ที่ STA003 ที่พักจริง
        for (const lr of leafRows || []) {
          const lotNo = (lr.to_lot_no || "").trim();
          if (!lotNo || usedFromLots.has(lotNo)) continue;

          // เช็คก่อนว่ายังไม่มี park row ของ op_sc_id นี้ + to_lot_no นี้ (ป้องกัน duplicate)
          const [dupCheck] = await conn.query(
            `SELECT transfer_id FROM ${SAFE_TRANSFER}
             WHERE op_sc_id = ? AND to_lot_no = ? AND lot_parked_status = 1 LIMIT 1`,
            [op_sc_id, lotNo]
          );
          if (dupCheck.length === 0) {
            await insertTransfer(conn, {
              from_tk_id:       master_tk_id,
              to_tk_id:         master_tk_id,
              from_lot_no:      lr.from_lot_no || lotNo,
              to_lot_no:        lotNo,
              tf_rs_code:       lr.tf_rs_code ?? 2,
              transfer_qty:     lr.transfer_qty ?? 0,
              op_sc_id,
              MC_id:            MC_id_val,
              op_sta_id:        actorSta,        // ← station ที่พักจริง
              lot_parked_status: 1,
              created_by_u_id:  actor.u_id,
              transfer_ts:      now,
              color_id:         lr.color_id ?? null,
            });
          }

          console.log(`[MARK_PARKED_OWN] lot=${lotNo} sta=${actorSta}`);
        }

        // ── base lot (tf_rs_code=0): อยู่ใน TKRunLog เท่านั้น ไม่มีใน t_transfer ─
        //    ถ้าไม่ถูกเลือกใน usedFromLots → INSERT park row ใน t_transfer
        {
          const [baseRows] = await conn.query(
            `SELECT r.lot_no, r.part_id
             FROM ${SAFE_RUNLOG} r
             WHERE r.tk_id = ?
             ORDER BY r.created_at_ts ASC
             LIMIT 1`,
            [master_tk_id]
          );
          const baseLot = baseRows[0]?.lot_no ? String(baseRows[0].lot_no).trim() : null;

          if (baseLot && !usedFromLots.has(baseLot)) {
            // เช็คว่า base lot ยังไม่มีใน t_transfer เลย (ยังไม่เคยถูก park)
            const [existRows] = await conn.query(
              `SELECT transfer_id FROM ${SAFE_TRANSFER}
               WHERE to_lot_no = ? AND (from_tk_id = ? OR to_tk_id = ?)
               LIMIT 1`,
              [baseLot, master_tk_id, master_tk_id]
            );

            if (existRows.length === 0) {
              const baseLotQty = Math.max(1, Math.trunc(good_qty));

              await insertTransfer(conn, {
                from_tk_id:        master_tk_id,
                to_tk_id:          master_tk_id,
                from_lot_no:       baseLot,
                to_lot_no:         baseLot,
                tf_rs_code:        1,
                transfer_qty:      baseLotQty,
                op_sc_id,
                MC_id:             MC_id_val,
                op_sta_id:         actorSta,
                lot_parked_status: 1,
                created_by_u_id:   actor.u_id,
                transfer_ts:       now,
                color_id:          null,
              });
              console.log(`[MARK_PARKED_BASE] lot=${baseLot} qty=${baseLotQty} sta=${actorSta}`);
            }
          }
        }

        // ── cross-TK: lots พักจาก TK อื่นที่อยู่ใน station นี้แต่ไม่ถูกเลือก ─
        //    → UPDATE op_sta_id ให้ตรงกับ station ปัจจุบัน (ยังพักอยู่เหมือนเดิม)
        const crossTkLots = req.body.cross_tk_unselected_lots;
        if (Array.isArray(crossTkLots) && crossTkLots.length > 0) {
          for (const lotNo of crossTkLots) {
            const lot = String(lotNo || "").trim();
            if (!lot || usedFromLots.has(lot)) continue;
            await conn.query(
              `UPDATE ${SAFE_TRANSFER}
               SET op_sta_id = ?
               WHERE to_lot_no         = ?
                 AND lot_parked_status  = 1`,
              [actorSta, lot]
            );
            console.log(`[MARK_PARKED_CROSS] lot=${lot} sta=${actorSta}`);
          }
        }
      }

      // ⑦ update op_scan finish
      await conn.query(
        `UPDATE ${SAFE_OPSCAN}
         SET op_sc_total_qty=?, op_sc_good_qty=?,
             op_sc_scrap_qty=?, tf_rs_code=?,
             lot_no=?,
             op_sta_id=COALESCE(op_sta_id, ?),
             op_sc_finish_ts=?
         WHERE op_sc_id=?`,
        [
          Math.trunc(total_qty), Math.trunc(good_qty), Math.trunc(scrap_qty),
          Number(groups[groups.length - 1].tf_rs_code),
          first_lot_no || "", actorSta, now, op_sc_id,
        ]
      );

      // ⑧ update TKHead/TKDetail status
      const isFinishAtSTA007 = actorSta === "STA007";
      const newTkStatus      = isFinishAtSTA007 ? 1 : 2;
      for (const tbl of ["`TKHead`", SAFE_TKDETAIL]) {
        await conn.query(
          `UPDATE ${tbl} SET tk_status=? WHERE tk_id=?`,
          [newTkStatus, master_tk_id]
        );
      }

      await conn.commit();
      conn.release();
      console.log(`[OPSCAN_FINISH][OK] op_sc_id=${op_sc_id} tk_id=${master_tk_id} sta=${actorSta} good=${Math.trunc(good_qty)} scrap=${Math.trunc(scrap_qty)}`);

      return res.json({
        message: "Finished",
        actor:   { u_id: actor.u_id, u_firstname: actor.u_firstname, u_lastname: actor.u_lastname, role: actor.role },
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
    } catch (e) {
      await conn.rollback(); conn.release(); throw e;
    }
  } catch (err) {
    console.error("[OPSCAN_FINISH][ERROR]", err);
    return res.status(500).json({ message: "Finish failed", actor, error: err.message });
  }
};
