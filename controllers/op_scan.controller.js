// controllers/op_scan.controller.js
const sql = require("mssql");
const { getPool } = require("../config/db");

const OP_SCAN_TABLE =
  process.env.OP_SCAN_TABLE || process.env.OPSCAN_TABLE || "dbo.op_scan";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";
const MACHINE_TABLE = process.env.MACHINE_TABLE || "dbo.machine";
const PART_TABLE = process.env.PART_TABLE || "dbo.part";
const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";

const SAFE_OPSCAN = safeTableName(OP_SCAN_TABLE);
const SAFE_TKDETAIL = safeTableName(TKDETAIL_TABLE);
const SAFE_MACHINE = safeTableName(MACHINE_TABLE);
const SAFE_PART = safeTableName(PART_TABLE);
const SAFE_TRANSFER = safeTableName(TRANSFER_TABLE);

function safeTableName(fullName) {
  const s = String(fullName || "");
  if (!/^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/.test(s)) {
    throw new Error(`Invalid table name: ${s}`);
  }
  return s;
}
function pad(n, len) {
  return String(n).padStart(len, "0");
}
function yymmdd(d) {
  const yy = String(d.getFullYear()).slice(-2);
  const mm = pad(d.getMonth() + 1, 2);
  const dd = pad(d.getDate(), 2);
  return `${yy}${mm}${dd}`;
}

function normalizeClientType(raw) {
  const v = String(raw || "").trim().toUpperCase();
  if (v === "HH" || v === "FLUTTER") return "HH";
  if (v === "PC" || v === "REACT") return "PC";
  return "UNKNOWN";
}

function actorOf(req) {
  return {
    u_id: req.user?.u_id ?? null,
    u_name: req.user?.u_name ?? "unknown",
    role: req.user?.role ?? "unknown",
    u_type: req.user?.u_type ?? "unknown",
    op_sta_id: req.user?.op_sta_id ?? null,
    op_sta_name: req.user?.op_sta_name ?? null,
    clientType: normalizeClientType(req.headers["x-client-type"]),
  };
}

function forbid(res, message, actor) {
  return res.status(403).json({ message, actor });
}

async function genOpScId(tx, now) {
  const prefix = `SC${yymmdd(now)}`;
  const r = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(20), `${prefix}%`)
    .query(`
      SELECT TOP 1 op_sc_id
      FROM ${SAFE_OPSCAN} WITH (UPDLOCK, HOLDLOCK)
      WHERE op_sc_id LIKE @likePrefix
      ORDER BY op_sc_id DESC
    `);

  let running = 1;
  if (r.recordset?.length) {
    const lastId = String(r.recordset[0].op_sc_id || "");
    const tail = lastId.slice(prefix.length);
    const n = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }
  return `${prefix}${pad(running, 4)}`;
}

function buildResultString(goodQty, scrapQty) {
  const g = Number(goodQty || 0);
  const s = Number(scrapQty || 0);
  if (g > 0 && s > 0) return "OK, NG";
  if (g > 0) return "OK";
  if (s > 0) return "NG";
  return null;
}

async function getLotNoByTkId(txOrPool, tk_id) {
  const req =
    txOrPool instanceof sql.Transaction ? new sql.Request(txOrPool) : txOrPool.request();
  const r = await req
    .input("tk_id", sql.VarChar(20), tk_id)
    .query(`
      SELECT TOP 1 lot_no
      FROM ${SAFE_TKDETAIL} WITH (NOLOCK)
      WHERE tk_id = @tk_id
      ORDER BY tk_created_at_ts DESC
    `);

  return r.recordset?.[0]?.lot_no ?? null;
}

function normalizeSplits(body) {
  if (Array.isArray(body?.splits) && body.splits.length > 0) {
    return body.splits.map((x) => ({
      out_part_no: String(x?.out_part_no || "").trim(),
      qty: Number(x?.qty),
    }));
  }
  return [];
}

async function getPartByNo(tx, part_no) {
  const r = await new sql.Request(tx)
    .input("part_no", sql.VarChar(100), String(part_no).trim())
    .query(`
      SELECT TOP 1 part_id, part_no, part_name
      FROM ${SAFE_PART} WITH (NOLOCK)
      WHERE part_no = @part_no
    `);
  return r.recordset?.[0] ?? null;
}

async function genTransferId(tx, now) {
  // ใช้รูปแบบ TFyymmdd#### เหมือนเดิม
  const prefix = `TF${yymmdd(now)}`;
  const r = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(20), `${prefix}%`)
    .query(`
      SELECT TOP 1 transfer_id
      FROM ${SAFE_TRANSFER} WITH (UPDLOCK, HOLDLOCK)
      WHERE transfer_id LIKE @likePrefix
      ORDER BY transfer_id DESC
    `);

  let running = 1;
  if (r.recordset?.length) {
    const lastId = String(r.recordset[0].transfer_id || "");
    const tail = lastId.slice(prefix.length);
    const n = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }
  return `${prefix}${pad(running, 4)}`;
}

exports.listAllActiveOpScans = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT TOP (200)
        s.op_sc_id,
        s.tk_id,
        s.MC_id,
        s.u_id,
        s.op_sc_total_qty,
        s.op_sc_scrap_qty,
        s.op_sc_good_qty,
        s.tf_rs_code,
        td.lot_no,
        s.op_sc_ts,
        s.op_sc_finish_ts
      FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
      OUTER APPLY (
        SELECT TOP 1 d.lot_no
        FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
        WHERE d.tk_id = s.tk_id
        ORDER BY d.tk_created_at_ts DESC
      ) td
      WHERE s.op_sc_ts IS NOT NULL
        AND s.op_sc_finish_ts IS NULL
      ORDER BY s.op_sc_ts DESC
    `);

    return res.json({
      actor,
      active: true,
      count: r.recordset.length,
      items: r.recordset,
    });
  } catch (err) {
    console.error("[OPSCAN_ACTIVE_ALL][ERROR]", err);
    return res.status(500).json({ message: "Get active list failed", actor, error: err.message });
  }
};

exports.listOpScansByTkId = async (req, res) => {
  const actor = actorOf(req);
  const tk_id = String(req.params.tk_id || req.params.id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "tk_id is required", actor });

  try {
    const pool = await getPool();
    const r = await pool
      .request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT TOP (200)
          s.op_sc_id, s.tk_id, s.MC_id, s.u_id,
          s.op_sc_total_qty, s.op_sc_scrap_qty, s.op_sc_good_qty,
          s.tf_rs_code,
          lot_latest.lot_no AS lot_no,
          s.op_sc_ts, s.op_sc_finish_ts
        FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
        OUTER APPLY (
          SELECT TOP 1 d.lot_no
          FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
          WHERE d.tk_id = s.tk_id
          ORDER BY d.tk_created_at_ts DESC
        ) lot_latest
        WHERE s.tk_id = @tk_id
        ORDER BY s.op_sc_ts DESC
      `);

    return res.json({ actor, tk_id, items: r.recordset });
  } catch (err) {
    console.error("[OPSCAN_LIST][ERROR]", err);
    return res.status(500).json({ message: "List failed", actor, error: err.message });
  }
};

exports.getOpScanById = async (req, res) => {
  const actor = actorOf(req);
  const op_sc_id = String(req.params.op_sc_id || req.params.id || "").trim();
  if (!op_sc_id) return res.status(400).json({ message: "op_sc_id is required", actor });

  try {
    const pool = await getPool();
    const r = await pool
      .request()
      .input("op_sc_id", sql.Char(12), op_sc_id)
      .query(`
        SELECT TOP 1
          s.op_sc_id, s.tk_id, s.MC_id, s.u_id,
          s.op_sc_total_qty, s.op_sc_scrap_qty, s.op_sc_good_qty,
          s.tf_rs_code,
          lot_latest.lot_no AS lot_no,
          s.op_sc_ts, s.op_sc_finish_ts
        FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
        OUTER APPLY (
          SELECT TOP 1 d.lot_no
          FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
          WHERE d.tk_id = s.tk_id
          ORDER BY d.tk_created_at_ts DESC
        ) lot_latest
        WHERE s.op_sc_id = @op_sc_id
      `);

    const row = r.recordset?.[0];
    if (!row) return res.status(404).json({ message: "Not found", actor, op_sc_id });

    return res.json({ actor, item: row });
  } catch (err) {
    console.error("[OPSCAN_GET][ERROR]", err);
    return res.status(500).json({ message: "Get failed", actor, error: err.message });
  }
};

exports.getActiveOpScanByTkId = async (req, res) => {
  const actor = actorOf(req);
  const tk_id = String(req.params.tk_id || req.params.id || "").trim();
  if (!tk_id) return res.status(400).json({ message: "tk_id is required", actor });

  try {
    const pool = await getPool();
    const r = await pool
      .request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .query(`
        SELECT TOP 1
          s.op_sc_id, s.tk_id, s.MC_id, s.u_id,
          s.op_sc_total_qty, s.op_sc_scrap_qty, s.op_sc_good_qty,
          s.tf_rs_code,
          lot_latest.lot_no AS lot_no,
          s.op_sc_ts, s.op_sc_finish_ts
        FROM ${SAFE_OPSCAN} s WITH (NOLOCK)
        OUTER APPLY (
          SELECT TOP 1 d.lot_no
          FROM ${SAFE_TKDETAIL} d WITH (NOLOCK)
          WHERE d.tk_id = s.tk_id
          ORDER BY d.tk_created_at_ts DESC
        ) lot_latest
        WHERE s.tk_id = @tk_id
          AND s.op_sc_finish_ts IS NULL
        ORDER BY s.op_sc_ts DESC
      `);

    const row = r.recordset?.[0];
    if (!row) return res.json({ actor, tk_id, active: false, item: null });

    return res.json({ actor, tk_id, active: true, item: row });
  } catch (err) {
    console.error("[OPSCAN_ACTIVE][ERROR]", err);
    return res.status(500).json({ message: "Get active failed", actor, error: err.message });
  }
};

exports.startOpScan = async (req, res) => {
  const actor = actorOf(req);

  if (actor.u_type !== "op") return forbid(res, "Forbidden: u_type must be op", actor);
  if (actor.clientType !== "HH") return forbid(res, "Forbidden: clientType must be HH", actor);
  if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });

  const tk_id = String(req.body.tk_id || "").trim();
  const MC_id = String(req.body.MC_id || "").trim();

  if (!tk_id) return res.status(400).json({ message: "tk_id is required", actor });
  if (!MC_id) return res.status(400).json({ message: "MC_id is required", actor });

  // ✅ station ต้องมาจาก token (login HH)
  const op_sta_id = actor.op_sta_id ? String(actor.op_sta_id).trim() : "";
  if (!op_sta_id) return res.status(400).json({ message: "op_sta_id missing in token", actor });

  try {
    const pool = await getPool();
    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      const now = new Date();

      // lock tkdetail
      const tkDetailR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          SELECT TOP 1
            d.tk_id,
            d.MC_id,
            d.op_sta_id,
            d.part_id,
            p.part_no,
            p.part_name,
            d.lot_no,
            d.tk_status,
            d.tk_created_at_ts
          FROM ${SAFE_TKDETAIL} d WITH (UPDLOCK, HOLDLOCK)
          LEFT JOIN dbo.part p ON p.part_id = d.part_id
          WHERE d.tk_id = @tk_id
          ORDER BY d.tk_created_at_ts DESC
        `);

      const tkDoc = tkDetailR.recordset?.[0];
      if (!tkDoc) {
        await tx.rollback();
        return res.status(404).json({ message: "tk_id not found", actor, tk_id });
      }

      const lot_no = tkDoc.lot_no || null;

      // ✅ validate machine + ต้องอยู่ station เดียวกับที่ login มา
      const mcR = await new sql.Request(tx)
        .input("MC_id", sql.VarChar(10), MC_id)
        .query(`
          SELECT TOP 1 MC_id, op_sta_id
          FROM ${SAFE_MACHINE} WITH (NOLOCK)
          WHERE MC_id=@MC_id
        `);

      const mcRow = mcR.recordset?.[0];
      if (!mcRow) {
        await tx.rollback();
        return res.status(400).json({ message: "MC_id not found", actor, MC_id });
      }

      const mc_sta = mcRow.op_sta_id ? String(mcRow.op_sta_id).trim() : "";
      if (mc_sta && mc_sta !== op_sta_id) {
        await tx.rollback();
        return res.status(400).json({
          message: "MC_id does not belong to your station",
          actor,
          MC_id,
          mc_op_sta_id: mc_sta,
          token_op_sta_id: op_sta_id,
        });
      }

      // check active scan
      const activeR = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .query(`
          SELECT TOP 1 op_sc_id
          FROM ${SAFE_OPSCAN} WITH (UPDLOCK, HOLDLOCK)
          WHERE tk_id=@tk_id AND op_sc_finish_ts IS NULL
          ORDER BY op_sc_ts DESC
        `);

      if (activeR.recordset?.[0]) {
        await tx.rollback();
        return res.status(409).json({
          message: "This tk_id already has an active scan (not finished yet)",
          actor,
          tk_id,
          op_sc_id: activeR.recordset[0].op_sc_id,
        });
      }

      // ✅ update TKDetail ให้มี MC_id/op_sta_id (แก้ปัญหาที่คุณถาม)
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("MC_id", sql.VarChar(10), MC_id)
        .input("op_sta_id", sql.VarChar(20), op_sta_id)
        .query(`
          UPDATE ${SAFE_TKDETAIL}
          SET
            MC_id = @MC_id,
            op_sta_id = @op_sta_id
          WHERE tk_id = @tk_id
        `);

      const op_sc_id = await genOpScId(tx, now);

      await new sql.Request(tx)
        .input("op_sc_id", sql.Char(12), op_sc_id)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("MC_id", sql.VarChar(10), MC_id)
        .input("u_id", sql.Int, Number(actor.u_id))
        .input("lot_no", sql.NVarChar(300), lot_no)
        .input("op_sc_ts", sql.DateTime2(3), now)
        .query(`
          INSERT INTO ${SAFE_OPSCAN}
            (op_sc_id, tk_id, MC_id, u_id,
             op_sc_total_qty, op_sc_scrap_qty, op_sc_good_qty,
             tf_rs_code,
             lot_no,
             op_sc_ts, op_sc_finish_ts)
          VALUES
            (@op_sc_id, @tk_id, @MC_id, @u_id,
             0, 0, 0,
             NULL,
             @lot_no,
             @op_sc_ts, NULL)
        `);

      await tx.commit();

      console.log(
        `[OPSCAN_START] op_sc_id=${op_sc_id} tk_id=${tk_id} MC_id=${MC_id} op_sta_id=${op_sta_id} u_id=${actor.u_id} u_name=${actor.u_name} ts=${now.toISOString()}`
      );

      return res.status(201).json({
        message: "Started",
        actor: { u_id: actor.u_id, u_name: actor.u_name, role: actor.role },
        op_sc_id,
        MC_id,
        op_sc_total_qty: 0,
        tk_doc: {
          tk_id: tkDoc.tk_id,
          lot_no: tkDoc.lot_no,
          part_id: tkDoc.part_id,
          part_no: tkDoc.part_no,
          part_name: tkDoc.part_name,
          op_sta_id: op_sta_id,
          op_sta_name: actor.op_sta_name ?? null,
          tk_status: tkDoc.tk_status,
          tk_created_at_ts: tkDoc.tk_created_at_ts
            ? new Date(tkDoc.tk_created_at_ts).toISOString()
            : null,
        },
      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("[OPSCAN_START][ERROR]", err);
    return res.status(500).json({ message: "Start failed", actor, error: err.message });
  }
};

exports.finishOpScan = async (req, res) => {
  const actor = actorOf(req);

  if (actor.u_type !== "op") return forbid(res, "Forbidden: u_type must be op", actor);
  if (actor.clientType !== "HH") return forbid(res, "Forbidden: clientType must be HH", actor);
  if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });

  const op_sc_id = String(req.body.op_sc_id || "").trim();
  const good_qty_raw = Number(req.body.good_qty);
  const scrap_qty_raw = Number(req.body.scrap_qty);

  if (!op_sc_id) return res.status(400).json({ message: "op_sc_id is required", actor });
  if (![good_qty_raw, scrap_qty_raw].every(Number.isFinite)) {
    return res.status(400).json({ message: "good_qty/scrap_qty must be numbers", actor });
  }

  const good_qty = Math.abs(good_qty_raw);
  const scrap_qty = Math.abs(scrap_qty_raw);
  const total_qty = good_qty + scrap_qty;

  if (good_qty === 0 && scrap_qty === 0) {
    return res.status(400).json({ message: "good_qty and scrap_qty cannot both be 0", actor });
  }

  const tf_rs_code = Number(req.body.tf_rs_code || 0);
  if (![1, 2, 3].includes(tf_rs_code)) {
    return res.status(400).json({ message: "tf_rs_code must be 1,2,3", actor });
  }

  // --- helpers (local) ---
  const SAFE_TRANSFER = safeTableName(process.env.TRANSFER_TABLE || "dbo.t_transfer");
  const SAFE_PART = safeTableName(process.env.PART_TABLE || "dbo.part");

  const normalizeSplits = (body) => {
    if (Array.isArray(body?.splits) && body.splits.length > 0) {
      return body.splits.map((x) => ({
        out_part_no: String(x?.out_part_no || "").trim(),
        qty: Number(x?.qty),
      }));
    }
    return [];
  };

  const splits = normalizeSplits(req.body);

  if (tf_rs_code === 2) {
    if (splits.length === 0) {
      return res.status(400).json({ message: "splits[] is required when tf_rs_code=2", actor });
    }
    for (const s of splits) {
      if (!s.out_part_no) return res.status(400).json({ message: "out_part_no is required", actor });
      if (!Number.isFinite(s.qty) || s.qty <= 0) return res.status(400).json({ message: "qty must be > 0", actor });
    }
    const sumQty = splits.reduce((acc, s) => acc + Math.trunc(s.qty), 0);
    if (sumQty !== Math.trunc(good_qty)) {
      return res.status(400).json({
        message: "Sum of splits qty must equal good_qty",
        actor,
        good_qty: Math.trunc(good_qty),
        sum_splits_qty: sumQty,
      });
    }
  }

  const out_part_no = String(req.body.out_part_no || "").trim();

  try {
    const pool = await getPool();
    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      const now = new Date();

      // 1) lock op_scan row
      const rowR = await new sql.Request(tx)
        .input("op_sc_id", sql.Char(12), op_sc_id)
        .query(`
          SELECT TOP 1 *
          FROM ${SAFE_OPSCAN} WITH (UPDLOCK, HOLDLOCK)
          WHERE op_sc_id = @op_sc_id
        `);

      const row = rowR.recordset?.[0];
      if (!row) {
        await tx.rollback();
        return res.status(404).json({ message: "op_sc_id not found", actor, op_sc_id });
      }
      if (row.op_sc_finish_ts) {
        await tx.rollback();
        return res.status(409).json({
          message: "Already finished",
          actor: { u_id: actor.u_id, u_name: actor.u_name, role: actor.role },
          op_sc_id,
        });
      }

      const master_tk_id = String(row.tk_id || "").trim();
      if (!master_tk_id) {
        await tx.rollback();
        return res.status(400).json({ message: "op_scan.tk_id is NULL", actor, op_sc_id });
      }

      // master lot_no
      const master_lot_no = await getLotNoByTkId(tx, master_tk_id);

      // 2) update op_scan finish
      await new sql.Request(tx)
        .input("op_sc_id", sql.Char(12), op_sc_id)
        .input("total_qty", sql.Int, Math.trunc(total_qty))
        .input("good_qty", sql.Int, Math.trunc(good_qty))
        .input("scrap_qty", sql.Int, Math.trunc(scrap_qty))
        .input("tf_rs_code", sql.Int, tf_rs_code)
        .input("lot_no", sql.NVarChar(300), master_lot_no)
        .input("finish_ts", sql.DateTime2(3), now)
        .query(`
          UPDATE ${SAFE_OPSCAN}
          SET
            op_sc_total_qty = @total_qty,
            op_sc_good_qty = @good_qty,
            op_sc_scrap_qty = @scrap_qty,
            tf_rs_code = @tf_rs_code,
            lot_no = @lot_no,
            op_sc_finish_ts = @finish_ts
          WHERE op_sc_id = @op_sc_id
        `);

      // 3) split (tf_rs_code=2) -> gen new lot_no (same tk_id) + insert t_transfer
      const created_children = [];

      if (tf_rs_code === 2) {
        for (const s of splits) {
          const partR = await new sql.Request(tx)
            .input("part_no", sql.VarChar(100), s.out_part_no)
            .query(`
              SELECT TOP 1 part_id, part_no, part_name
              FROM ${SAFE_PART} WITH (NOLOCK)
              WHERE part_no = @part_no
            `);

          const part = partR.recordset?.[0];
          if (!part) {
            await tx.rollback();
            return res.status(400).json({ message: "out_part_no not found", actor, out_part_no: s.out_part_no });
          }

          // gen run_no + new lot_no (same tk_id)
          const sp = await new sql.Request(tx)
            .input("tk_id", sql.VarChar(20), master_tk_id)
            .input("part_id", sql.Int, Number(part.part_id))
            .input("created_by_u_id", sql.Int, Number(actor.u_id))
            .output("run_no", sql.Char(14))
            .output("lot_no", sql.NVarChar(300))
            .execute("dbo.usp_TKRunLog_Create");

          const run_no = sp.output.run_no;
          const child_lot_no = sp.output.lot_no;

          if (!run_no || !child_lot_no) {
            throw new Error("DB did not return run_no/lot_no from dbo.usp_TKRunLog_Create");
          }

          // insert t_transfer (ไม่ใส่ transfer_id เพราะเป็น IDENTITY)
          await new sql.Request(tx)
            .input("from_tk_id", sql.VarChar(20), master_tk_id)
            .input("to_tk_id", sql.VarChar(20), master_tk_id) // tk_id เดิมตาม requirement
            .input("from_lot_no", sql.NVarChar(300), master_lot_no)
            .input("to_lot_no", sql.NVarChar(300), String(child_lot_no))
            .input("tf_rs_code", sql.Int, 2)
            .input("transfer_qty", sql.Int, Math.trunc(s.qty))
            .input("op_sc_id", sql.Char(12), op_sc_id)
            .input("MC_id", sql.VarChar(10), row.MC_id ? String(row.MC_id).trim() : null)
            .input("created_by_u_id", sql.Int, Number(actor.u_id))
            .input("transfer_ts", sql.DateTime2(3), now)
            .query(`
              INSERT INTO ${SAFE_TRANSFER}
                (from_tk_id, to_tk_id, from_lot_no, to_lot_no,
                 tf_rs_code, transfer_qty,
                 op_sc_id, MC_id, created_by_u_id,
                 transfer_ts)
              VALUES
                (@from_tk_id, @to_tk_id, @from_lot_no, @to_lot_no,
                 @tf_rs_code, @transfer_qty,
                 @op_sc_id, @MC_id, @created_by_u_id,
                 @transfer_ts)
            `);

          created_children.push({
            tk_id: master_tk_id,
            run_no: String(run_no).trim(),
            lot_no: String(child_lot_no),
            out_part_no: String(part.part_no),
            out_part_name: part.part_name ? String(part.part_name) : null,
            qty: Math.trunc(s.qty),
          });
        }
      }

      await tx.commit();

      console.log(
        `[OPSCAN_FINISH] op_sc_id=${op_sc_id} tk_id=${master_tk_id} MC_id=${row.MC_id ?? "-"} tf_rs_code=${tf_rs_code} good=${Math.trunc(
          good_qty
        )} scrap=${Math.trunc(scrap_qty)} total=${Math.trunc(total_qty)} children=${created_children.length} u_id=${actor.u_id} u_name=${actor.u_name} ts=${now.toISOString()}`
      );

      return res.json({
        message: "Finished",
        actor: { u_id: actor.u_id, u_name: actor.u_name, role: actor.role },

        op_sc_id,
        tk_id: master_tk_id,
        MC_id: row.MC_id ?? null,
        lot_no: master_lot_no,

        op_sc_total_qty: Math.trunc(total_qty),
        op_sc_good_qty: Math.trunc(good_qty),
        op_sc_scrap_qty: Math.trunc(scrap_qty),
        tf_rs_code,

        out_part_no,

        created_children_count: created_children.length,
        created_children,

        op_sc_ts: row.op_sc_ts ? new Date(row.op_sc_ts).toISOString() : null,
        op_sc_finish_ts: now.toISOString(),
      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("[OPSCAN_FINISH][ERROR]", err);
    return res.status(500).json({ message: "Finish failed", actor, error: err.message });
  }
};