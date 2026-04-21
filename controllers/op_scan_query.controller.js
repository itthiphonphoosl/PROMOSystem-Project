// controllers/op_scan_query.controller.js
// ──────────────────────────────────────────
// GET / READ endpoints สำหรับ op_scan เท่านั้น
// Start / Finish อยู่ใน op_scan.controller.js
// ──────────────────────────────────────────
const { getPool } = require("../config/db");

const OP_SCAN_TABLE  = process.env.OP_SCAN_TABLE  || process.env.OPSCAN_TABLE || "dbo.op_scan";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";
const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";

function safeTableName(fullName) {
  const s = String(fullName || "").replace(/^[A-Za-z0-9_]+\./, "");
  if (!/^[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return `\`${s}\``;
}

const SAFE_OPSCAN   = safeTableName(OP_SCAN_TABLE);
const SAFE_TKDETAIL = safeTableName(TKDETAIL_TABLE);
const SAFE_TRANSFER = safeTableName(TRANSFER_TABLE);

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

// ─────────────────────────────────────────────────────────
// GET /api/op-scan/active
// ─────────────────────────────────────────────────────────
exports.listAllActiveOpScans = async (req, res) => {
  const actor      = actorOf(req);
  const isOperator = actor.u_type === "op";
  const opSta      = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";

  if (isOperator && !opSta) {
    return res.status(400).json({ message: "Missing op_sta_id in token", actor });
  }

  try {
    const pool = getPool();

    const whereExtra = isOperator
      ? "AND TRIM(COALESCE(s.op_sta_id, m.op_sta_id)) = ? AND s.u_id = ?"
      : "";
    const params = isOperator ? [opSta, Number(actor.u_id)] : [];

    const [rows] = await pool.query(
      `SELECT
         s.op_sc_id,
         s.tk_id,
         COALESCE(s.op_sta_id, m.op_sta_id) AS op_sta_id,
         st.op_sta_name,
         s.MC_id,
         s.u_id,
         s.op_sc_total_qty,
         s.op_sc_scrap_qty,
         s.op_sc_good_qty,
         s.tf_rs_code,
         (SELECT d.lot_no FROM ${SAFE_TKDETAIL} d
          WHERE d.tk_id = s.tk_id
          ORDER BY d.tk_created_at_ts ASC LIMIT 1) AS lot_no,
         s.op_sc_ts,
         s.op_sc_finish_ts
       FROM ${SAFE_OPSCAN} s
       LEFT JOIN \`machine\`    m  ON m.MC_id      = s.MC_id
       LEFT JOIN \`op_station\` st ON st.op_sta_id = COALESCE(s.op_sta_id, m.op_sta_id)
       WHERE s.op_sc_ts        IS NOT NULL
         AND s.op_sc_finish_ts IS NULL
         ${whereExtra}
       ORDER BY s.op_sc_ts DESC
       LIMIT 200`,
      params
    );

    return res.json({ actor, active: true, count: rows.length, items: rows });
  } catch (err) {
    console.error("[OPSCAN_ACTIVE_ALL][ERROR]", err);
    return res.status(500).json({ message: "Get active list failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scan/:op_sc_id
// ─────────────────────────────────────────────────────────
exports.getOpScanById = async (req, res) => {
  const actor    = actorOf(req);
  const op_sc_id = String(req.params.op_sc_id || req.params.id || "").trim();
  if (!op_sc_id) return res.status(400).json({ message: "op_sc_id is required", actor });

  try {
    const pool = getPool();

    const [rows] = await pool.query(
      `SELECT
         s.op_sc_id,
         s.tk_id,
         s.op_sta_id,
         st.op_sta_name,
         s.MC_id,
         m.MC_name,
         s.u_id,
         s.op_sc_total_qty,
         s.op_sc_scrap_qty,
         s.op_sc_good_qty,
         s.tf_rs_code,
         (SELECT d.lot_no FROM ${SAFE_TKDETAIL} d
          WHERE d.tk_id = s.tk_id
          ORDER BY d.tk_created_at_ts ASC LIMIT 1) AS lot_no,
         s.op_sc_ts,
         s.op_sc_finish_ts
       FROM ${SAFE_OPSCAN} s
       LEFT JOIN \`op_station\` st ON st.op_sta_id = s.op_sta_id
       LEFT JOIN \`machine\`    m  ON m.MC_id      = s.MC_id
       WHERE s.op_sc_id = ?
       LIMIT 1`,
      [op_sc_id]
    );

    const row = rows[0];
    if (!row) return res.status(404).json({ message: "Not found", actor, op_sc_id });

    const [incomingRows] = await pool.query(
      `SELECT
         t.to_lot_no AS lot_no,
         t.from_lot_no,
         t.tf_rs_code,
         t.transfer_qty AS qty,
         CAST(t.lot_parked_status AS UNSIGNED) AS lot_parked_status,
         t.color_id,
         cp.color_no,
         cp.color_name,
         t.transfer_ts
       FROM ${SAFE_TRANSFER} t
       LEFT JOIN \`color_painting\` cp ON cp.color_id = t.color_id
       WHERE t.op_sc_id = ?
       ORDER BY t.transfer_id ASC`,
      [op_sc_id]
    );

    const [currentRows] = await pool.query(
      `SELECT
         t.to_lot_no AS lot_no,
         t.from_lot_no,
         t.tf_rs_code,
         t.transfer_qty AS qty,
         CAST(t.lot_parked_status AS UNSIGNED) AS lot_parked_status,
         t.color_id,
         cp.color_no,
         cp.color_name,
         t.op_sta_id,
         t.op_sc_id,
         t.transfer_ts
       FROM ${SAFE_TRANSFER} t
       INNER JOIN (
         SELECT to_lot_no, MAX(transfer_id) AS max_transfer_id
         FROM ${SAFE_TRANSFER}
         WHERE to_tk_id = ?
         GROUP BY to_lot_no
       ) latest
         ON latest.to_lot_no = t.to_lot_no
        AND latest.max_transfer_id = t.transfer_id
       LEFT JOIN \`color_painting\` cp ON cp.color_id = t.color_id
       WHERE t.to_tk_id = ?
         AND t.lot_parked_status = 0
         AND NOT EXISTS (
           SELECT 1 FROM ${SAFE_TRANSFER} t2
           WHERE t2.from_lot_no = t.to_lot_no
             AND t2.to_lot_no   != t.to_lot_no
             AND (t2.from_tk_id = ? OR t2.to_tk_id = ?)
             -- ยกเว้น: lot ที่มี self-transfer (แบ่งออกไปบางส่วนแต่ยังเหลืออยู่ในถังเดิม)
             AND NOT EXISTS (
               SELECT 1 FROM ${SAFE_TRANSFER} t3
               WHERE t3.from_lot_no = t.to_lot_no
                 AND t3.to_lot_no   = t.to_lot_no
                 AND t3.lot_parked_status = 0
             )
         )
       ORDER BY t.transfer_id ASC`,
      [row.tk_id, row.tk_id, row.tk_id, row.tk_id]
    );

    // base: original TK document info (part_no / part_name / lot_no) from TKDetail
    // TKDetail ถูกสร้างตอนสร้างเอกสาร → ไม่เปลี่ยนตาม Co-ID ภายหลัง
    const [tkDetailBaseRows] = await pool.query(
      `SELECT d.part_id, d.lot_no, p.part_no, p.part_name
       FROM \`TKDetail\` d
       LEFT JOIN \`part\` p ON p.part_id = d.part_id
       WHERE d.tk_id = ?
       ORDER BY d.tk_created_at_ts ASC
       LIMIT 1`,
      [row.tk_id]
    );
    const tkDetailBase = tkDetailBaseRows[0] ?? null;

    // run_no: ยังคงดึงจาก TKRunLog (lot แรกของ TK นี้)
    const [baseRows] = await pool.query(
      `SELECT r.run_no, r.lot_no, r.part_id, p.part_no, p.part_name
       FROM \`TKRunLog\` r
       LEFT JOIN \`part\` p ON p.part_id = r.part_id
       WHERE r.tk_id = ?
       ORDER BY r.created_at_ts ASC
       LIMIT 1`,
      [row.tk_id]
    );
    const baseRow = baseRows[0] ?? null;

    return res.json({
      actor,
      item: row,
      // part_no / part_name / lot_no → TKDetail (ข้อมูลตอนสร้างเอกสาร ไม่เปลี่ยนตาม Co-ID)
      // run_no → TKRunLog (lot แรกที่ถูกสร้าง)
      base: {
        run_no:    baseRow?.run_no    ?? null,
        lot_no:    tkDetailBase?.lot_no    ?? baseRow?.lot_no    ?? null,
        part_id:   tkDetailBase?.part_id   ?? baseRow?.part_id   ?? null,
        part_no:   tkDetailBase?.part_no   ?? baseRow?.part_no   ?? null,
        part_name: tkDetailBase?.part_name ?? baseRow?.part_name ?? null,
      },
      incoming_lots: incomingRows,
      incoming_lot_count: incomingRows.length,
      current_lots: currentRows,
      current_lot_count: currentRows.length,
    });
  } catch (err) {
    console.error("[OPSCAN_GET][ERROR]", err);
    return res.status(500).json({ message: "Get failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scan/active/:tk_id
// ─────────────────────────────────────────────────────────
exports.getActiveOpScanByTkId = async (req, res) => {
  const actor  = actorOf(req);
  const tk_id  = String(req.params.tk_id || req.params.tkId || req.params.id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "กรุณากรอก Tracking No.", actor });

  const isOperator = actor.u_type === "op";
  const opSta      = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";
  if (isOperator && !opSta) {
    return res.status(400).json({ message: "Missing op_sta_id in token", actor });
  }

  try {
    const pool = getPool();

    if (isOperator) {
      const [[tkHead]] = await pool.query(
        `SELECT tk_active FROM \`TKHead\` WHERE tk_id = ? LIMIT 1`,
        [tk_id]
      );
      if (!tkHead) {
        return res.status(404).json({ message: "ไม่พบ Tracking No. นี้ในระบบ", actor, tk_id });
      }
      if (Number(tkHead.tk_active) !== 1) {
        return res.status(403).json({
          message:   "เอกสารนี้ถูกปิดใช้งานอยู่ กรุณาติดต่อ Admin",
          tk_id,
          tk_active: Number(tkHead.tk_active),
          actor,
        });
      }
    }

    const whereExtra = isOperator
      ? "AND TRIM(COALESCE(s.op_sta_id, m.op_sta_id)) = ?"
      : "";
    const params = isOperator ? [tk_id, opSta] : [tk_id];

    const [rows] = await pool.query(
      `SELECT
         s.op_sc_id,
         s.tk_id,
         COALESCE(s.op_sta_id, m.op_sta_id) AS op_sta_id,
         st.op_sta_name,
         s.MC_id,
         m.MC_name,
         s.u_id,
         s.op_sc_total_qty,
         s.op_sc_scrap_qty,
         s.op_sc_good_qty,
         s.tf_rs_code,
         (SELECT d.lot_no FROM ${SAFE_TKDETAIL} d
          WHERE d.tk_id = s.tk_id
          ORDER BY d.tk_created_at_ts ASC LIMIT 1) AS lot_no,
         s.op_sc_ts,
         s.op_sc_finish_ts
       FROM ${SAFE_OPSCAN} s
       LEFT JOIN \`machine\`    m  ON m.MC_id      = s.MC_id
       LEFT JOIN \`op_station\` st ON st.op_sta_id = COALESCE(s.op_sta_id, m.op_sta_id)
       WHERE s.tk_id = ?
         AND s.op_sc_ts IS NOT NULL
         AND s.op_sc_finish_ts IS NULL
         ${whereExtra}
       ORDER BY s.op_sc_ts DESC
       LIMIT 1`,
      params
    );

    const row = rows[0] || null;
    return res.json({ actor, tk_id, active: !!row, item: row });
  } catch (err) {
    console.error("[OPSCAN_ACTIVE_BY_TK][ERROR]", err);
    return res.status(500).json({ message: "Get active failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scan/summary/:tk_id
// ─────────────────────────────────────────────────────────
exports.getTkSummary = async (req, res) => {
  const actor = actorOf(req);
  const tk_id = String(req.params.tk_id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "กรุณากรอก Tracking No.", actor });

  try {
    const pool = getPool();

    // 1) TKHead
    const [headRows] = await pool.query(
      `SELECT tk_id, tk_status, tk_active, created_by_u_id, tk_created_at_ts
       FROM \`TKHead\`
       WHERE tk_id = ?
       LIMIT 1`,
      [tk_id]
    );
    const head = headRows[0];
    if (!head) {
      return res.status(404).json({
        message: "ไม่พบ Tracking No. นี้ ไม่มีอยู่ในระบบ",
        actor,
        tk_id,
      });
    }

    if (Number(head.tk_active) !== 1 && actor.u_type === "op") {
      return res.status(403).json({
        message: "เอกสารนี้ถูกปิดใช้งานอยู่ กรุณาติดต่อ Admin",
        actor,
        tk_id,
        tk_active: Number(head.tk_active),
      });
    }

    // 2) current detail (ล่าสุด)
    const [detailRows] = await pool.query(
      `SELECT d.tk_id, d.lot_no, d.part_id, p.part_no, p.part_name,
              d.MC_id, d.op_sta_id, d.tk_created_at_ts
       FROM ${SAFE_TKDETAIL} d
       LEFT JOIN \`part\` p ON p.part_id = d.part_id
       WHERE d.tk_id = ?
       ORDER BY d.tk_created_at_ts DESC
       LIMIT 1`,
      [tk_id]
    );
    const detail = detailRows[0] ?? null;

    // 2.1) base lot + part แรกของเอกสาร
    const [baseRows] = await pool.query(
      `SELECT
         r.run_no,
         r.lot_no,
         r.part_id,
         p.part_no,
         p.part_name
       FROM \`TKRunLog\` r
       LEFT JOIN \`part\` p ON p.part_id = r.part_id
       WHERE r.tk_id = ?
       ORDER BY r.created_at_ts ASC
       LIMIT 1`,
      [tk_id]
    );
    const baseRow = baseRows[0] ?? null;

    // 2.2) tk document info ตั้งต้น
    // ใช้ค่าแรกของเอกสารเหมือน base เพื่อให้หน้า summary header
    // แสดง Part No / Part Name เดิมที่ติดมากับเอกสาร
    const tkDoc = baseRow
      ? {
          tk_id,
          part_id: baseRow.part_id ?? null,
          part_no: baseRow.part_no ?? null,
          part_name: baseRow.part_name ?? null,
          lot_no: baseRow.lot_no ?? null,
        }
      : (detail
          ? {
              tk_id,
              part_id: detail.part_id ?? null,
              part_no: detail.part_no ?? null,
              part_name: detail.part_name ?? null,
              lot_no: detail.lot_no ?? null,
            }
          : null);

    // 3) op_scan history
    const [scanRows] = await pool.query(
      `SELECT
         s.op_sc_id, s.op_sta_id, st.op_sta_name,
         s.MC_id, m.MC_name,
         s.u_id, u.u_firstname, u.u_lastname,
         s.op_sc_total_qty, s.op_sc_good_qty, s.op_sc_scrap_qty,
         s.tf_rs_code, s.lot_no,
         s.op_sc_ts, s.op_sc_finish_ts,
         CASE WHEN s.op_sc_finish_ts IS NULL THEN 'IN_PROGRESS' ELSE 'DONE' END AS scan_status
       FROM ${SAFE_OPSCAN} s
       LEFT JOIN \`op_station\` st ON st.op_sta_id = s.op_sta_id
       LEFT JOIN \`machine\`    m  ON m.MC_id      = s.MC_id
       LEFT JOIN \`user\`       u  ON u.u_id       = s.u_id
       WHERE s.tk_id = ?
       ORDER BY s.op_sc_ts ASC`,
      [tk_id]
    );

    // 4) transfer history
    const [transferRows] = await pool.query(
      `SELECT
         t.transfer_id, t.from_tk_id, t.to_tk_id,
         t.from_lot_no, t.to_lot_no,
         t.tf_rs_code, tr.tf_rs_name,
         t.transfer_qty, t.op_sc_id, t.op_sta_id, s.op_sta_name,
         t.MC_id, m.MC_name,
         CAST(t.lot_parked_status AS UNSIGNED) AS lot_parked_status,
         CASE t.lot_parked_status WHEN 0 THEN 'Active' WHEN 1 THEN 'Parked' END AS lot_status_name,
         t.color_id, cp.color_no, cp.color_name,
         t.created_by_u_id, u.u_firstname AS created_by_u_firstname, u.u_lastname AS created_by_u_lastname,
         t.transfer_ts,
         rl_from.tk_id AS from_lot_original_tk
       FROM ${SAFE_TRANSFER} t
       LEFT JOIN \`transfer_reason\` tr ON tr.tf_rs_code = t.tf_rs_code
       LEFT JOIN \`op_station\`      s  ON s.op_sta_id   = t.op_sta_id
       LEFT JOIN \`machine\`         m  ON m.MC_id       = t.MC_id
       LEFT JOIN \`user\`            u  ON u.u_id        = t.created_by_u_id
       LEFT JOIN \`color_painting\`  cp ON cp.color_id   = t.color_id
       LEFT JOIN \`TKRunLog\`        rl_from ON rl_from.lot_no = t.from_lot_no
       WHERE t.from_tk_id = ? OR t.to_tk_id = ?
       ORDER BY t.transfer_ts ASC`,
      [tk_id, tk_id]
    );

    // 5) parked lots ของ TK นี้
    const [parkedRows] = await pool.query(
      `SELECT
         t.transfer_id,
         t.to_lot_no   AS parked_lot_no,
         t.from_lot_no AS came_from_lot,
         t.tf_rs_code, tr.tf_rs_name,
         t.transfer_qty AS parked_qty,
         t.op_sta_id, s.op_sta_name
       FROM ${SAFE_TRANSFER} t
       LEFT JOIN \`transfer_reason\` tr ON tr.tf_rs_code = t.tf_rs_code
       LEFT JOIN \`op_station\`      s  ON s.op_sta_id   = t.op_sta_id
       WHERE (t.from_tk_id = ? OR t.to_tk_id = ?)
         AND t.lot_parked_status = 1
       ORDER BY t.transfer_ts ASC`,
      [tk_id, tk_id]
    );

    const scans     = scanRows || [];
    const transfers = transferRows || [];
    const parked    = parkedRows || [];

    // ── คำนวณ is_used_parked_lot เฉพาะ Co-ID (tf_rs_code=3) ──────────────────
    // เงื่อนไข: row นั้นต้องเป็น Co-ID AND from_lot_no เคยมี row ที่ lot_parked_status=1
    // Master / Split ห้ามขึ้น badge นี้ ไม่ว่า from_lot_no เคยถูก park หรือไม่
    {
      const parkedToLots = new Set(
        transfers
          .filter(t => Number(t.lot_parked_status) === 1)
          .map(t => (t.to_lot_no || "").trim())
          .filter(Boolean)
      );

      transfers.forEach(t => {
        const fromLot  = (t.from_lot_no || "").trim();
        const fromTkId = String(t.from_tk_id || "").trim();
        const toTkId   = String(t.to_tk_id   || "").trim();
        // from_lot_original_tk: TK ที่สร้าง from_lot จาก TKRunLog (JOIN แล้วจาก query)
        // ป้องกันกรณี getLotOwnerTk return null ตอน insertTransfer → from_tk_id ถูก set เป็น master_tk_id ผิด
        const fromLotOriginalTk = String(t.from_lot_original_tk || "").trim();
        const isCrossTk =
          (fromTkId.length > 0 && toTkId.length > 0 && fromTkId !== toTkId) ||
          (fromLotOriginalTk.length > 0 && fromLotOriginalTk !== tk_id);
        t.is_used_parked_lot =
          Number(t.tf_rs_code) === 3 &&        // Co-ID เท่านั้น
          Number(t.lot_parked_status) === 0 &&  // row ที่ active
          fromLot.length > 0 &&
          (parkedToLots.has(fromLot) || isCrossTk);
      });
    }
    // ─────────────────────────────────────────────────────────────────────────

    // 6) derived lots
    const isOperator = actor.u_type === "op";
    const actorSta   = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";

    if (isOperator && !actorSta) {
      return res.status(400).json({ message: "Missing op_sta_id in token", actor });
    }

    const current_station = isOperator
      ? actorSta
      : (detail?.op_sta_id ? String(detail.op_sta_id).trim() : null);

    const lastFinished = [...scans].reverse().find(s => s.op_sc_finish_ts);
    const last_finished_op_sc_id = lastFinished?.op_sc_id
      ? String(lastFinished.op_sc_id).trim()
      : null;

    let incoming_lots = [];
    if (last_finished_op_sc_id) {
      const [inRows] = await pool.query(
        `SELECT
           t.to_lot_no         AS lot_no,
           CAST(latest_s.lot_parked_status AS UNSIGNED) AS lot_parked_status,
           SUM(t.transfer_qty) AS qty,
           MAX(t.transfer_ts)  AS last_ts
         FROM ${SAFE_TRANSFER} t
         INNER JOIN (
           SELECT t2.to_lot_no, t2.lot_parked_status
           FROM ${SAFE_TRANSFER} t2
           INNER JOIN (
             SELECT to_lot_no, MAX(transfer_id) AS max_id
             FROM ${SAFE_TRANSFER}
             GROUP BY to_lot_no
           ) mx ON mx.to_lot_no = t2.to_lot_no AND mx.max_id = t2.transfer_id
         ) latest_s ON latest_s.to_lot_no = t.to_lot_no
         WHERE t.op_sc_id = ?
           AND NOT EXISTS (
             SELECT 1 FROM ${SAFE_TRANSFER} t3
             WHERE t3.from_lot_no = t.to_lot_no
               AND t3.to_lot_no   != t.to_lot_no
               AND (t3.from_tk_id = ? OR t3.to_tk_id = ?)
           )
         GROUP BY t.to_lot_no, latest_s.lot_parked_status
         ORDER BY MAX(t.transfer_ts) DESC`,
        [last_finished_op_sc_id, tk_id, tk_id]
      );
      incoming_lots = inRows || [];
    }

    let parked_lots_station_all = [];
    if (current_station) {
      const [pkStaRows] = await pool.query(
        `SELECT
           t.transfer_id, t.from_tk_id, t.to_tk_id,
           t.to_lot_no   AS parked_lot_no,
           t.from_lot_no AS came_from_lot,
           t.tf_rs_code, tr.tf_rs_name AS parked_reason,
           t.transfer_qty AS parked_qty,
           t.op_sta_id, s.op_sta_name,
           t.transfer_ts  AS parked_at
         FROM ${SAFE_TRANSFER} t
         LEFT JOIN \`transfer_reason\` tr ON tr.tf_rs_code = t.tf_rs_code
         LEFT JOIN \`op_station\`      s  ON s.op_sta_id   = t.op_sta_id
         WHERE t.op_sta_id = ?
           AND t.lot_parked_status = 1
         ORDER BY t.transfer_ts DESC`,
        [current_station]
      );
      parked_lots_station_all = pkStaRows || [];
    }

    const totalGood    = scans.reduce((a, s) => a + (s.op_sc_good_qty  || 0), 0);
    const totalScrap   = scans.reduce((a, s) => a + (s.op_sc_scrap_qty || 0), 0);
    const stationsDone = [...new Set(
      scans.filter(s => s.op_sc_finish_ts).map(s => s.op_sta_id).filter(Boolean)
    )];

    const tk_status_label = {
      0: "NOT_STARTED",
      1: "FINISHED",
      2: "PARTIAL_DONE",
      3: "IN_PROGRESS",
      4: "CANCELLED",
    }[head.tk_status] ?? "UNKNOWN";

    const stationOrder = [...new Set(scans.map(s => s.op_sta_id).filter(Boolean))];

    const stations_history = stationOrder.map((sta_id) => {
      const staScans = scans.filter(s => s.op_sta_id === sta_id);
      const staGood  = staScans.reduce((a, s) => a + (s.op_sc_good_qty  || 0), 0);
      const staScrap = staScans.reduce((a, s) => a + (s.op_sc_scrap_qty || 0), 0);

      const scansGrouped = staScans.map((s, idx) => {
        const scanTransfers = transfers.filter(t => t.op_sc_id === s.op_sc_id);
        const scanParked = parked.filter(p =>
          scanTransfers.some(t => t.transfer_id === p.transfer_id)
        );

        return {
          scan_no: idx + 1,
          op_sc_id: s.op_sc_id,
          tf_rs_name:
            s.tf_rs_code === 1 ? "Master-ID" :
            s.tf_rs_code === 2 ? "Split-ID" :
            s.tf_rs_code === 3 ? "Co-ID" : null,
          u_firstname: s.u_firstname,
          u_lastname: s.u_lastname,
          MC_id: s.MC_id,
          MC_name: s.MC_name,
          op_sc_good_qty: s.op_sc_good_qty,
          op_sc_scrap_qty: s.op_sc_scrap_qty,
          op_sc_total_qty: s.op_sc_total_qty,
          lot_no: s.lot_no ?? (scanTransfers.find(t => t.lot_parked_status === 0)?.to_lot_no ?? null),
          op_sc_ts: s.op_sc_ts,
          op_sc_finish_ts: s.op_sc_finish_ts,
          scan_status: s.scan_status,
          transfers: scanTransfers,
          parked_lots: scanParked,
        };
      });

      return {
        op_sta_id: sta_id,
        op_sta_name: staScans[0]?.op_sta_name ?? null,
        total_good: staGood,
        total_scrap: staScrap,
        scans: scansGrouped,
      };
    });

    return res.json({
      actor,
      tk_id: head.tk_id,
      tk_status: head.tk_status,
      tk_status_label,
      is_finished: head.tk_status === 1,
      tk_active: Number(head.tk_active),
      is_active: Number(head.tk_active) === 1,
      tk_created_at_ts: head.tk_created_at_ts
        ? new Date(head.tk_created_at_ts).toISOString()
        : null,

      // ข้อมูลตั้งต้นของเอกสาร
      tk: tkDoc,

      // lot แรก + part แรกของเอกสาร
      base: baseRow ? {
        run_no: baseRow.run_no ? String(baseRow.run_no).trim() : null,
        lot_no: baseRow.lot_no ?? null,
        part_id: baseRow.part_id ?? null,
        part_no: baseRow.part_no ?? null,
        part_name: baseRow.part_name ?? null,
      } : null,

      // สถานะล่าสุดของเอกสาร
      current: detail ? {
        lot_no: detail.lot_no,
        part_id: detail.part_id,
        part_no: detail.part_no,
        part_name: detail.part_name,
        MC_id: detail.MC_id,
        op_sta_id: detail.op_sta_id,
      } : null,

      current_station,
      last_finished_op_sc_id,
      incoming_lots,

      summary: {
        total_scans: scans.length,
        total_good: totalGood,
        total_scrap: totalScrap,
        stations_done: stationsDone,
        parked_lots_count: parked.length,
      },

      stations_history,
      parked_lots: parked,
      parked_lots_station_all,
    });
  } catch (err) {
    console.error("[TK_SUMMARY][ERROR]", err);
    return res.status(500).json({ message: "Get summary failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/op-scan/parked?op_sta_id=STA001
// ─────────────────────────────────────────────────────────
exports.getParkedLots = async (req, res) => {
  const actor      = actorOf(req);
  const isOperator = actor.u_type === "op";

  const op_sta_id = isOperator
    ? (actor.op_sta_id ? String(actor.op_sta_id).trim() : "")
    : String(req.query.op_sta_id || "").trim();

  if (!op_sta_id) {
    return res.status(400).json({ message: "op_sta_id is required", actor });
  }

  try {
    const pool = getPool();
    const [rows] = await pool.query(
      `SELECT
         t.transfer_id, t.from_tk_id, t.to_tk_id,
         t.to_lot_no   AS parked_lot_no,
         t.from_lot_no AS came_from_lot,
         t.tf_rs_code, tr.tf_rs_name AS parked_reason,
         t.transfer_qty AS parked_qty,
         t.op_sta_id, s.op_sta_name,
         t.transfer_ts  AS parked_at
       FROM ${SAFE_TRANSFER} t
       LEFT JOIN \`transfer_reason\` tr ON tr.tf_rs_code = t.tf_rs_code
       LEFT JOIN \`op_station\`      s  ON s.op_sta_id   = t.op_sta_id
       WHERE t.op_sta_id = ?
         AND t.lot_parked_status = 1
       ORDER BY t.transfer_ts DESC`,
      [op_sta_id]
    );

    return res.json({ actor, op_sta_id, count: rows.length, parked_lots: rows });
  } catch (err) {
    console.error("[PARKED_LOTS][ERROR]", err);
    return res.status(500).json({ message: "Get parked lots failed", actor, error: err.message });
  }
};

async function getCurrentActiveLotsByTk(pool, tk_id) {
  const [rows] = await pool.query(
    `SELECT
       x.to_lot_no AS lot_no,
       x.from_lot_no,
       x.tf_rs_code,
       x.transfer_qty AS qty,
       CAST(x.lot_parked_status AS UNSIGNED) AS lot_parked_status,
       x.op_sta_id,
       x.op_sc_id,
       x.transfer_ts
     FROM ${SAFE_TRANSFER} x
     INNER JOIN (
       SELECT to_lot_no, MAX(transfer_id) AS max_transfer_id
       FROM ${SAFE_TRANSFER}
       WHERE to_tk_id = ?
       GROUP BY to_lot_no
     ) latest
       ON latest.to_lot_no = x.to_lot_no
      AND latest.max_transfer_id = x.transfer_id
     WHERE x.to_tk_id = ?
       AND x.lot_parked_status = 0
       AND NOT EXISTS (
         SELECT 1 FROM ${SAFE_TRANSFER} t2
         WHERE t2.from_lot_no = x.to_lot_no
           AND t2.to_lot_no   != x.to_lot_no
           AND (t2.from_tk_id = ? OR t2.to_tk_id = ?)
           -- ยกเว้น: lot ที่มี self-transfer (แบ่งออกไปบางส่วนแต่ยังเหลืออยู่ในถังเดิม)
           AND NOT EXISTS (
             SELECT 1 FROM ${SAFE_TRANSFER} t3
             WHERE t3.from_lot_no = x.to_lot_no
               AND t3.to_lot_no   = x.to_lot_no
               AND t3.lot_parked_status = 0
           )
       )
     ORDER BY x.transfer_id ASC`,
    [tk_id, tk_id, tk_id, tk_id]
  );

  if (!rows.length) {
    const [baseRows] = await pool.query(
      `SELECT r.lot_no, r.run_no, p.part_no, p.part_name
       FROM \`TKRunLog\` r
       LEFT JOIN \`part\` p ON p.part_id = r.part_id
       WHERE r.tk_id = ?
       ORDER BY r.created_at_ts ASC
       LIMIT 1`,
      [tk_id]
    );

    if (!baseRows[0]) return [];

    return [{
      lot_no: baseRows[0].lot_no,
      from_lot_no: null,
      tf_rs_code: 0,
      qty: null,
      lot_parked_status: 0,
      op_sta_id: null,
      op_sc_id: null,
      transfer_ts: null,
      part_no: baseRows[0].part_no ?? null,
      part_name: baseRows[0].part_name ?? null,
      run_no: baseRows[0].run_no ?? null,
    }];
  }

  return rows;
}

// ─────────────────────────────────────────────────────────
// GET /api/op-scans/lookup-by-lot/:lot_no
// ─────────────────────────────────────────────────────────
exports.lookupTkByLotNo = async (req, res) => {
  const actor  = actorOf(req);
  const lot_no = String(req.params.lot_no || req.query.lot_no || "").trim();

  if (!lot_no) {
    return res.status(400).json({ message: "กรุณาระบุ lot_no", actor });
  }

  try {
    const pool = getPool();

    const [logRows] = await pool.query(
      `SELECT tk_id FROM \`TKRunLog\` WHERE lot_no = ? LIMIT 1`,
      [lot_no]
    );

    if (!logRows[0]) {
      return res.status(404).json({ message: "ไม่พบ Lot No. นี้ในระบบ", actor, lot_no });
    }

    const tk_id = String(logRows[0].tk_id).trim();

    const [[tkHead]] = await pool.query(
      `SELECT tk_id, tk_status, tk_active FROM \`TKHead\` WHERE tk_id = ? LIMIT 1`,
      [tk_id]
    );

    if (!tkHead) {
      return res.status(404).json({ message: "ไม่พบ Tracking No. ของ Lot นี้ในระบบ", actor, lot_no, tk_id });
    }
    if (Number(tkHead.tk_active) !== 1) {
      return res.status(403).json({ message: "เอกสารนี้ถูกปิดใช้งานอยู่ กรุณาติดต่อ Admin", actor, lot_no, tk_id, tk_active: Number(tkHead.tk_active) });
    }
    if (Number(tkHead.tk_status) === 4) {
      return res.status(403).json({ message: "เอกสาร Tracking No. นี้ถูก Cancel ไปแล้ว", actor, lot_no, tk_id });
    }

    const [parkedRows] = await pool.query(
      `SELECT CAST(t.lot_parked_status AS UNSIGNED) AS lot_parked_status,
              t.op_sta_id, s.op_sta_name
       FROM \`t_transfer\` t
       LEFT JOIN \`op_station\` s ON s.op_sta_id = t.op_sta_id
       WHERE t.to_lot_no = ?
       ORDER BY t.transfer_id DESC
       LIMIT 1`,
      [lot_no]
    );

    const parkedRow = parkedRows[0];
    if (parkedRow && (parkedRow.lot_parked_status === 1 || parkedRow.lot_parked_status === true)) {
      const parkedSta     = parkedRow.op_sta_id   ?? "-";
      const parkedStaName = parkedRow.op_sta_name ?? "-";
      return res.status(403).json({
        message: `Lot "${lot_no}" ถูกพักไว้ที่ ${parkedSta} (${parkedStaName}) ยังไม่สามารถเริ่มงานได้`,
        actor,
        lot_no,
        tk_id,
        parked: true,
        parked_at_sta: parkedSta,
        parked_at_sta_name: parkedStaName,
      });
    }

    if (Number(tkHead.tk_status) === 1) {
      const [lastStaRows] = await pool.query(
        `SELECT s.op_sta_id, st.op_sta_name
         FROM \`op_scan\` s
         LEFT JOIN \`op_station\` st ON st.op_sta_id = s.op_sta_id
         WHERE s.tk_id = ? AND s.op_sc_finish_ts IS NOT NULL
         ORDER BY s.op_sc_finish_ts DESC
         LIMIT 1`,
        [tk_id]
      );
      const lastSta = lastStaRows[0];
      return res.json({
        actor,
        lot_no,
        tk_id,
        tk_status: 1,
        is_finished: true,
        finished_at_sta_id:   lastSta?.op_sta_id   ?? null,
        finished_at_sta_name: lastSta?.op_sta_name ?? null,
        message: `Lot No. นี้ เสร็จงานที่ ${lastSta?.op_sta_id ?? ''} (${lastSta?.op_sta_name ?? ''}) เรียบร้อย`,
      });
    }

    const [detailRows] = await pool.query(
      `SELECT d.part_id, p.part_no, p.part_name, d.lot_no, d.tk_status
       FROM ${SAFE_TKDETAIL} d
       LEFT JOIN \`part\` p ON p.part_id = d.part_id
       WHERE d.tk_id = ?
       ORDER BY d.tk_created_at_ts DESC
       LIMIT 1`,
      [tk_id]
    );
    const detail = detailRows[0] ?? null;

    const [lastFinRows] = await pool.query(
      `SELECT s.op_sta_id, st.op_sta_name, s.op_sc_finish_ts
       FROM \`op_scan\` s
       LEFT JOIN \`op_station\` st ON st.op_sta_id = s.op_sta_id
       WHERE s.tk_id = ? AND s.op_sc_finish_ts IS NOT NULL
       ORDER BY s.op_sc_finish_ts DESC
       LIMIT 1`,
      [tk_id]
    );
    const lastFinished = lastFinRows[0] ?? null;

    const current_lots = await getCurrentActiveLotsByTk(pool, tk_id);

    return res.json({
      actor,
      lot_no,
      tk_id,
      tk_status: tkHead.tk_status,
      detail: detail ? {
        part_no: detail.part_no ?? null,
        part_name: detail.part_name ?? null,
        lot_no: detail.lot_no ?? null,
        tk_status: detail.tk_status,
      } : null,
      current_lots,
      current_lot_count: current_lots.length,
      scanned_lot_no: lot_no,
      last_finished_sta:      lastFinished?.op_sta_id   ?? null,
      last_finished_sta_name: lastFinished?.op_sta_name ?? null,
    });
  } catch (err) {
    console.error("[LOOKUP_BY_LOT][ERROR]", err);
    return res.status(500).json({ message: "Lookup failed", actor, error: err.message });
  }
};