// controllers/opScan.controller.js
const { sql, getPool } = require("../config/db");

function yyyymmdd(date = new Date()) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}

async function generateRunningId(tx, tableName, idCol, prefix) {
  const last = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(50), `${prefix}%`)
    .query(`
      SELECT TOP 1 ${idCol} AS lastId
      FROM ${tableName} WITH (UPDLOCK, HOLDLOCK)
      WHERE ${idCol} LIKE @likePrefix
      ORDER BY ${idCol} DESC
    `);

  let running = 1;
  if (last.recordset.length > 0) {
    const lastId = String(last.recordset[0].lastId);
    const tail = lastId.slice(prefix.length);
    const n = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }

  return `${prefix}${String(running).padStart(4, "0")}`;
}

function parseDateOnly(s) {
  // "2026-02-04" -> Date at 00:00:00 local
  if (!s) return null;
  const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]) - 1;
  const d = Number(m[3]);
  return new Date(y, mo, d, 0, 0, 0, 0);
}

/**
 * POST /api/op-scans
 * body: { t_qr_code OR t_id, scrap_qty, condition_code }
 * role: operator
 */
async function createOpScan(req, res) {
  const t_qr_code = String(req.body.t_qr_code || "").trim();
  const t_id = String(req.body.t_id || "").trim();
  const scrapQty = Number(req.body.scrap_qty);
  const conditionCode = Number(req.body.condition_code);

  if (!t_qr_code && !t_id) {
    return res.status(400).json({ message: "t_qr_code or t_id is required" });
  }
  if (!Number.isFinite(scrapQty) || scrapQty < 0) {
    return res.status(400).json({ message: "scrap_qty must be a number >= 0" });
  }
  if (![1, 2, 3].includes(conditionCode)) {
    return res.status(400).json({ message: "condition_code must be 1, 2, or 3" });
  }

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    // Find tray (approved current state)
    const trayResult = await pool
      .request()
      .input("t_qr_code", sql.NVarChar(100), t_qr_code || null)
      .input("t_id", sql.VarChar(30), t_id || null)
      .query(`
        SELECT TOP 1
          t.t_id,
          t.t_qr_code,
          t.t_current_qty,
          t.t_current_op_sta_id,
          t.part_id,
          p.part_no,
          p.part_name,
          s.op_sta_code,
          s.op_sta_name,
          s.op_sta_seq
        FROM tray t
        JOIN part p ON p.part_id = t.part_id
        JOIN op_station s ON s.op_sta_id = t.t_current_op_sta_id
        WHERE (@t_qr_code IS NOT NULL AND t.t_qr_code = @t_qr_code)
           OR (@t_id IS NOT NULL AND t.t_id = @t_id)
      `);

    if (trayResult.recordset.length === 0) {
      return res.status(404).json({ message: "Tray not found" });
    }

    const tray = trayResult.recordset[0];
    const beforeQty = Number(tray.t_current_qty);

    if (scrapQty > beforeQty) {
      return res.status(400).json({ message: "scrap_qty cannot be greater than before_qty" });
    }

    // Rule: prevent pending stacking (API-level)
    const pending = await pool
      .request()
      .input("t_id", sql.VarChar(30), tray.t_id)
      .query(`
        SELECT TOP 1 op_sc_id
        FROM op_scan
        WHERE op_sc_t_id = @t_id
          AND op_sc_status_code = 'PENDING'
        ORDER BY op_sc_created_ts DESC
      `);

    if (pending.recordset.length > 0) {
      return res.status(409).json({
        message: "This tray already has a PENDING record",
        pending_op_sc_id: pending.recordset[0].op_sc_id,
      });
    }

    const goodQty = beforeQty - scrapQty;
    const now = new Date();
    const prefix = `SC${yyyymmdd(now)}`;

    const tx = new sql.Transaction(pool);
    await tx.begin();

    try {
      const op_sc_id = await generateRunningId(tx, "op_scan", "op_sc_id", prefix);

      await new sql.Request(tx)
        .input("op_sc_id", sql.VarChar(30), op_sc_id)
        .input("op_sc_t_id", sql.VarChar(30), tray.t_id)
        .input("op_sc_op_sta_id", sql.Int, tray.t_current_op_sta_id)
        .input("op_sc_before_qty", sql.Int, beforeQty)
        .input("op_sc_scrap_qty", sql.Int, scrapQty)
        .input("op_sc_good_qty", sql.Int, goodQty)
        .input("op_sc_condition_code", sql.Int, conditionCode)
        .input("op_sc_status_code", sql.VarChar(20), "PENDING")
        .input("op_sc_created_by", sql.Int, Number(req.user?.u_id || 0))
        .input("op_sc_created_ts", sql.DateTime2(3), now)
        .query(`
          INSERT INTO op_scan (
            op_sc_id, op_sc_t_id, op_sc_op_sta_id,
            op_sc_before_qty, op_sc_scrap_qty, op_sc_good_qty,
            op_sc_condition_code, op_sc_status_code,
            op_sc_created_by, op_sc_created_ts
          )
          VALUES (
            @op_sc_id, @op_sc_t_id, @op_sc_op_sta_id,
            @op_sc_before_qty, @op_sc_scrap_qty, @op_sc_good_qty,
            @op_sc_condition_code, @op_sc_status_code,
            @op_sc_created_by, @op_sc_created_ts
          )
        `);

      await tx.commit();

      return res.status(201).json({
        message: "Saved as PENDING",
        op_scan: {
          op_sc_id,
          status: "PENDING",
          before_qty: beforeQty,
          scrap_qty: scrapQty,
          good_qty: goodQty,
          condition_code: conditionCode,
          station: {
            op_sta_id: tray.t_current_op_sta_id,
            op_sta_code: tray.op_sta_code,
            op_sta_name: tray.op_sta_name,
            op_sta_seq: tray.op_sta_seq,
          },
          created_by: req.user?.u_id,
          created_ts: now.toISOString(),
        },
        tray: {
          t_id: tray.t_id,
          t_qr_code: tray.t_qr_code,
          part: { part_id: tray.part_id, part_no: tray.part_no, part_name: tray.part_name },
          current_qty: tray.t_current_qty,
        },
      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("CREATE OP_SCAN ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

/**
 * GET /api/op-scans/pending?page=1&limit=20&stationId=1&dateFrom=2026-02-04&dateTo=2026-02-05&qr=TR...
 * role: admin/manager
 */
async function listPending(req, res) {
  const page = Math.max(1, Number(req.query.page || 1));
  const limit = Math.min(100, Math.max(1, Number(req.query.limit || 20)));

  const stationId = req.query.stationId ? Number(req.query.stationId) : null;
  const qr = String(req.query.qr || "").trim();

  const dateFrom = parseDateOnly(req.query.dateFrom);
  const dateTo = parseDateOnly(req.query.dateTo);
  const dateToExclusive = dateTo ? new Date(dateTo.getTime() + 24 * 60 * 60 * 1000) : null;

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    let where = `WHERE s.op_sc_status_code = 'PENDING'`;
    const rCount = pool.request();
    const rData = pool.request();

    if (Number.isFinite(stationId)) {
      where += ` AND s.op_sc_op_sta_id = @stationId`;
      rCount.input("stationId", sql.Int, stationId);
      rData.input("stationId", sql.Int, stationId);
    }

    if (qr) {
      where += ` AND t.t_qr_code LIKE @qr`;
      rCount.input("qr", sql.NVarChar(120), `%${qr}%`);
      rData.input("qr", sql.NVarChar(120), `%${qr}%`);
    }

    if (dateFrom) {
      where += ` AND s.op_sc_created_ts >= @dateFrom`;
      rCount.input("dateFrom", sql.DateTime2(3), dateFrom);
      rData.input("dateFrom", sql.DateTime2(3), dateFrom);
    }

    if (dateToExclusive) {
      where += ` AND s.op_sc_created_ts < @dateTo`;
      rCount.input("dateTo", sql.DateTime2(3), dateToExclusive);
      rData.input("dateTo", sql.DateTime2(3), dateToExclusive);
    }

    const offset = (page - 1) * limit;
    rData.input("offset", sql.Int, offset);
    rData.input("limit", sql.Int, limit);

    const countResult = await rCount.query(`
      SELECT COUNT(1) AS total
      FROM op_scan s
      JOIN tray t ON t.t_id = s.op_sc_t_id
      ${where}
    `);

    const total = Number(countResult.recordset[0]?.total || 0);

    const dataResult = await rData.query(`
      SELECT
        s.op_sc_id,
        s.op_sc_created_ts,
        s.op_sc_op_sta_id,
        st.op_sta_code,
        st.op_sta_name,
        st.op_sta_seq,
        s.op_sc_before_qty,
        s.op_sc_scrap_qty,
        s.op_sc_good_qty,
        s.op_sc_condition_code,
        s.op_sc_created_by,
        u.u_name AS created_by_name,
        t.t_id,
        t.t_qr_code,
        p.part_no,
        p.part_name
      FROM op_scan s
      JOIN tray t ON t.t_id = s.op_sc_t_id
      JOIN part p ON p.part_id = t.part_id
      JOIN op_station st ON st.op_sta_id = s.op_sc_op_sta_id
      LEFT JOIN [user] u ON u.u_id = s.op_sc_created_by
      ${where}
      ORDER BY s.op_sc_created_ts DESC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `);

    return res.json({
      page,
      limit,
      total,
      totalPages: Math.ceil(total / limit),
      items: dataResult.recordset,
    });
  } catch (err) {
    console.error("LIST PENDING ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

/**
 * POST /api/op-scans/:id/approve
 * role: admin/manager
 */
async function approve(req, res) {
  const id = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ message: "op_sc_id is required" });

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const now = new Date();
    const approverId = Number(req.user?.u_id || 0);

    const tx = new sql.Transaction(pool);
    await tx.begin();

    try {
      // Lock scan row
      const scanRes = await new sql.Request(tx)
        .input("id", sql.VarChar(30), id)
        .query(`
          SELECT TOP 1 *
          FROM op_scan WITH (UPDLOCK, HOLDLOCK)
          WHERE op_sc_id = @id
        `);

      if (scanRes.recordset.length === 0) {
        await tx.rollback();
        return res.status(404).json({ message: "op_scan not found" });
      }

      const scan = scanRes.recordset[0];

      if (String(scan.op_sc_status_code) !== "PENDING") {
        await tx.rollback();
        return res.status(409).json({ message: `Cannot approve. Current status = ${scan.op_sc_status_code}` });
      }

      // Lock tray row
      const trayRes = await new sql.Request(tx)
        .input("t_id", sql.VarChar(30), scan.op_sc_t_id)
        .query(`
          SELECT TOP 1 *
          FROM tray WITH (UPDLOCK, HOLDLOCK)
          WHERE t_id = @t_id
        `);

      if (trayRes.recordset.length === 0) {
        await tx.rollback();
        return res.status(404).json({ message: "tray not found" });
      }

      const tray = trayRes.recordset[0];

      // Validate station + qty
      if (Number(tray.t_current_op_sta_id) !== Number(scan.op_sc_op_sta_id)) {
        await tx.rollback();
        return res.status(409).json({ message: "Station mismatch (tray moved already)" });
      }

      if (Number(tray.t_current_qty) !== Number(scan.op_sc_before_qty)) {
        await tx.rollback();
        return res.status(409).json({ message: "Qty mismatch (tray qty changed already)" });
      }

      // Find next station by seq
      const curStaRes = await new sql.Request(tx)
        .input("curId", sql.Int, Number(scan.op_sc_op_sta_id))
        .query(`
          SELECT TOP 1 op_sta_seq
          FROM op_station
          WHERE op_sta_id = @curId
        `);

      if (curStaRes.recordset.length === 0) {
        await tx.rollback();
        return res.status(500).json({ message: "Current station not found in op_station" });
      }

      const curSeq = Number(curStaRes.recordset[0].op_sta_seq);

      const nextStaRes = await new sql.Request(tx)
        .input("nextSeq", sql.Int, curSeq + 1)
        .query(`
          SELECT TOP 1 op_sta_id, op_sta_code, op_sta_name, op_sta_seq
          FROM op_station
          WHERE op_sta_seq = @nextSeq
        `);

      const hasNext = nextStaRes.recordset.length > 0;
      const nextSta = hasNext ? nextStaRes.recordset[0] : null;

      // Update scan -> APPROVED
      await new sql.Request(tx)
        .input("id", sql.VarChar(30), id)
        .input("by", sql.Int, approverId)
        .input("ts", sql.DateTime2(3), now)
        .query(`
          UPDATE op_scan
          SET op_sc_status_code = 'APPROVED',
              op_sc_approved_by = @by,
              op_sc_approved_ts = @ts
          WHERE op_sc_id = @id
            AND op_sc_status_code = 'PENDING'
        `);

      // Update tray (apply good qty + move next station)
      const completedCode = Number(process.env.TRAY_STATUS_COMPLETED || 9);
      const inProgressCode = Number(process.env.TRAY_STATUS_IN_PROGRESS || 1);

      if (hasNext) {
        await new sql.Request(tx)
          .input("t_id", sql.VarChar(30), tray.t_id)
          .input("qty", sql.Int, Number(scan.op_sc_good_qty))
          .input("nextStaId", sql.Int, Number(nextSta.op_sta_id))
          .input("status", sql.Int, inProgressCode)
          .query(`
            UPDATE tray
            SET t_current_qty = @qty,
                t_current_op_sta_id = @nextStaId,
                t_status_code = @status
            WHERE t_id = @t_id
          `);
      } else {
        // OP7 approved -> complete (keep station as current OP7)
        await new sql.Request(tx)
          .input("t_id", sql.VarChar(30), tray.t_id)
          .input("qty", sql.Int, Number(scan.op_sc_good_qty))
          .input("status", sql.Int, completedCode)
          .query(`
            UPDATE tray
            SET t_current_qty = @qty,
                t_status_code = @status
            WHERE t_id = @t_id
          `);
      }

      // Re-query tray card to return
      const out = await new sql.Request(tx)
        .input("t_id", sql.VarChar(30), tray.t_id)
        .query(`
          SELECT TOP 1
            t.t_id, t.t_qr_code, t.t_current_qty, t.t_status_code,
            t.part_id, p.part_no, p.part_name,
            t.t_current_op_sta_id, st.op_sta_code, st.op_sta_name, st.op_sta_seq
          FROM tray t
          JOIN part p ON p.part_id = t.part_id
          JOIN op_station st ON st.op_sta_id = t.t_current_op_sta_id
          WHERE t.t_id = @t_id
        `);

      await tx.commit();

      return res.json({
        message: "Approved",
        tray: out.recordset[0],
        movedTo: hasNext ? nextSta : null,
      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("APPROVE ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

/**
 * POST /api/op-scans/:id/reject
 * body: { reason? }
 * role: admin/manager
 */
async function reject(req, res) {
  const id = String(req.params.id || "").trim();
  const reason = String(req.body.reason || "").trim();

  if (!id) return res.status(400).json({ message: "op_sc_id is required" });

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const now = new Date();
    const approverId = Number(req.user?.u_id || 0);

    const tx = new sql.Transaction(pool);
    await tx.begin();

    try {
      const scanRes = await new sql.Request(tx)
        .input("id", sql.VarChar(30), id)
        .query(`
          SELECT TOP 1 *
          FROM op_scan WITH (UPDLOCK, HOLDLOCK)
          WHERE op_sc_id = @id
        `);

      if (scanRes.recordset.length === 0) {
        await tx.rollback();
        return res.status(404).json({ message: "op_scan not found" });
      }

      const scan = scanRes.recordset[0];

      if (String(scan.op_sc_status_code) !== "PENDING") {
        await tx.rollback();
        return res.status(409).json({ message: `Cannot reject. Current status = ${scan.op_sc_status_code}` });
      }

      await new sql.Request(tx)
        .input("id", sql.VarChar(30), id)
        .input("by", sql.Int, approverId)
        .input("ts", sql.DateTime2(3), now)
        .input("reason", sql.NVarChar(500), reason || null)
        .query(`
          UPDATE op_scan
          SET op_sc_status_code = 'REJECTED',
              op_sc_approved_by = @by,
              op_sc_approved_ts = @ts,
              op_sc_reject_reason = @reason
          WHERE op_sc_id = @id
            AND op_sc_status_code = 'PENDING'
        `);

      await tx.commit();

      return res.json({ message: "Rejected", op_sc_id: id, rejected_ts: now.toISOString() });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("REJECT ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

module.exports = { createOpScan, listPending, approve, reject };
