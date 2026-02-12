// controllers/tk_document.controller.js
// TKHead / TKDetail Controller (PC: React, HH: Flutter)

const sql = require("mssql");
const { getPool } = require("../config/db");

// ===== TABLES (override via env) =====
const TKHEAD_TABLE = process.env.TKHEAD_TABLE || "dbo.TKHead";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";

// ===== helpers =====
function pad(n, len) {
  return String(n).padStart(len, "0");
}

function yymmdd(d) {
  const yy = String(d.getFullYear()).slice(-2);
  const mm = pad(d.getMonth() + 1, 2);
  const dd = pad(d.getDate(), 2);
  return `${yy}${mm}${dd}`; // 6
}

function actorOf(req) {
  return {
    u_name: req.user?.u_name || "unknown",
    role: req.user?.role || "unknown",
    u_id: req.user?.u_id ?? null,
  };
}

// Prevent SQL injection via env table names
function safeTableName(fullName) {
  const s = String(fullName || "");
  if (!/^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/.test(s)) {
    throw new Error(`Invalid table name: ${s}`);
  }
  return s;
}

const SAFE_TKHEAD = safeTableName(TKHEAD_TABLE);
const SAFE_TKDETAIL = safeTableName(TKDETAIL_TABLE);

/**
 * Build lot_no format (reference)
 * yymmdd-part_no-part_name-run14
 * NOTE: In production we generate lot_no at DB via stored procedure.
 */
function buildLotNo({ docDate, partNo, partName, runNo14 }) {
  const date6 = yymmdd(docDate); // YYMMDD
  const pNo = String(partNo || "").trim();
  const pName = String(partName || "").trim();
  const r14 = String(runNo14 || "").padStart(14, "0").slice(-14);
  return `${date6}-${pNo}-${pName}-${r14}`;
}

// gen tk_id: TK + YYMMDD + #### (e.g., TK2602090001)
async function genTkId(tx, docDate) {
  const prefix = `TK${yymmdd(docDate)}`;

  const r = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(30), `${prefix}%`)
    .query(`
      SELECT TOP 1 tk_id
      FROM ${SAFE_TKHEAD} WITH (UPDLOCK, HOLDLOCK)
      WHERE tk_id LIKE @likePrefix
      ORDER BY tk_id DESC
    `);

  let running = 1;
  if (r.recordset.length > 0) {
    const lastId = String(r.recordset[0].tk_id || "");
    const tail = lastId.slice(prefix.length); // ####
    const n = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }

  const tk_id = `${prefix}${pad(running, 4)}`;
  return { tk_id, runNo: running };
}

/**
 * POST /TKDocs  (admin only + PC only enforced by routes, but we double-check)
 * Create:
 *  - TKHead (document)
 *  - TKRunLog (DB generates run_no & lot_no, inserts log)
 *  - TKDetail (first tray) using the generated lot_no
 *
 * Body:
 * {
 *   "part_no": "382-B42-002D",
 *   "created_op_sta_id": "STA005",
 *   "qty": 50
 * }
 */
exports.createTkDocument = async (req, res) => {
  const actor = actorOf(req);

  if (actor.role !== "admin") {
    return res.status(403).json({ message: "Forbidden (admin only)", actor });
  }

  try {
    const pool = await getPool();

    const part_no = String(req.body.part_no || "").trim();
    const MC_id = String(req.body.MC_id || "").trim(); // ✅ new input

    if (!part_no) return res.status(400).json({ message: "part_no is required", actor });
    if (!MC_id) return res.status(400).json({ message: "MC_id is required", actor });
    if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });

    // 1) lookup part
    const partResult = await pool.request()
      .input("part_no", sql.VarChar(50), part_no)
      .query(`
        SELECT TOP 1 part_id, part_no, part_name
        FROM dbo.part
        WHERE part_no = @part_no
      `);

    const partRow = partResult.recordset?.[0];
    if (!partRow) {
      return res.status(400).json({
        message: "Create failed: part_no not found",
        actor,
        error: `part_no not found: ${part_no}`,
      });
    }

    // 2) lookup machine -> op_sta_id
    const mcResult = await pool.request()
      .input("MC_id", sql.VarChar(10), MC_id)
      .query(`
        SELECT TOP 1 MC_id, op_sta_id, MC_active, MC_name
        FROM dbo.machine
        WHERE MC_id = @MC_id
      `);

    const mcRow = mcResult.recordset?.[0];
    if (!mcRow) {
      return res.status(400).json({
        message: "Create failed: MC_id not found",
        actor,
        error: `MC_id not found: ${MC_id}`,
      });
    }

    const op_sta_id = mcRow.op_sta_id ? String(mcRow.op_sta_id).trim() : null;
    if (!op_sta_id) {
      return res.status(400).json({
        message: "Create failed: machine has no op_sta_id",
        actor,
        error: `machine ${MC_id} has op_sta_id = NULL`,
      });
    }

    // (optional) validate station exists
    const staCheck = await pool.request()
      .input("id", sql.VarChar(20), op_sta_id)
      .query(`SELECT TOP 1 op_sta_id FROM dbo.op_station WHERE op_sta_id = @id`);

    if (!staCheck.recordset?.[0]) {
      return res.status(400).json({
        message: "Create failed: op_sta_id not found in op_station",
        actor,
        error: `op_sta_id not found: ${op_sta_id}`,
      });
    }

    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      const now = new Date();

      // 3) gen tk_id
      const { tk_id } = await genTkId(tx, now);

      // 4) insert TKHead (✅ include MC_id)
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("created_by_u_id", sql.Int, Number(actor.u_id))
        .input("tk_status", sql.Int, 0)
        .input("tk_created_at_ts", sql.DateTime2(3), now)
        .input("MC_id", sql.VarChar(10), MC_id)
        .query(`
          INSERT INTO ${SAFE_TKHEAD}
            (tk_id, created_by_u_id, tk_status, tk_created_at_ts, MC_id)
          VALUES
            (@tk_id, @created_by_u_id, @tk_status, @tk_created_at_ts, @MC_id)
        `);

      // 5) DB generates run_no(CHAR14) + lot_no and inserts TKRunLog
      const spResult = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("part_id", sql.Int, Number(partRow.part_id)) // ถ้า part_id เป็น BIGINT ค่อยปรับเป็น sql.BigInt
        .input("created_by_u_id", sql.Int, Number(actor.u_id))
        .output("run_no", sql.Char(14))
        .output("lot_no", sql.NVarChar(300))
        .execute("dbo.usp_TKRunLog_Create");

      const run_no14 = spResult.output.run_no;
      const lot_no = spResult.output.lot_no;

      if (!run_no14 || !lot_no) {
        throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");
      }

      // 6) insert TKDetail (✅ new schema fields)
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("MC_id", sql.VarChar(10), MC_id)
        .input("op_sta_id", sql.VarChar(20), op_sta_id)
        .input("tk_parent_id", sql.VarChar(20), tk_id)
        .input("u_id", sql.Int, Number(actor.u_id))
        .input("part_id", sql.Int, Number(partRow.part_id))
        .input("lot_no", sql.NVarChar(300), lot_no)
        .input("tk_status", sql.Int, 0)
        .input("tk_created_at_ts", sql.DateTime2(3), now)
        .query(`
          INSERT INTO ${SAFE_TKDETAIL}
            (tk_id, MC_id, op_sta_id, tk_parent_id, u_id, part_id, lot_no, tk_status, tk_created_at_ts)
          VALUES
            (@tk_id, @MC_id, @op_sta_id, @tk_parent_id, @u_id, @part_id, @lot_no, @tk_status, @tk_created_at_ts)
        `);

      await tx.commit();

      return res.status(201).json({
        message: "Created TK document",
        actor,
        id: tk_id,
        run_no: run_no14,
        lot_no, // ✅ should match TKRunLog.lot_no
        machine: {
          MC_id,
          MC_name: mcRow.MC_name,
          op_sta_id,
        },
        detail: {
          tk_id,
          MC_id,
          op_sta_id,
          tk_parent_id: tk_id,
          u_id: Number(actor.u_id),
          part_id: Number(partRow.part_id),
          tk_status: 0,
          tk_created_at_ts: now.toISOString(),
          lot_no,
        },
      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("[TK][CREATE][ERROR]", err);
    return res.status(500).json({
      message: "Create failed",
      actor,
      error: err.message,
    });
  }
};

// alias ให้ routes ใช้ได้เหมือนเดิม
exports.createTkDoc = exports.createTkDocument;
/**
 * GET /TKDocs  (PC only)
 * List documents + first tray info
 */
exports.listTkDocs = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool = await getPool();

    const r = await pool.request().query(`
      SELECT
        h.tk_id,
        h.tk_status,
        h.created_by_u_id,
        h.tk_created_at_ts,

        d.part_id,
        p.part_no,
        p.part_name,
        d.lot_no,
        d.tk_current_qty,
        d.current_op_sta_id
      FROM ${SAFE_TKHEAD} h
      LEFT JOIN ${SAFE_TKDETAIL} d
        ON d.tk_id = h.tk_id
      LEFT JOIN dbo.part p
        ON p.part_id = d.part_id
      ORDER BY h.tk_created_at_ts DESC
    `);

    return res.json({ actor, items: r.recordset });
  } catch (err) {
    console.error("[TK][LIST][ERROR]", err);
    return res.status(500).json({
      message: "List failed",
      actor,
      error: err.message,
    });
  }
};

/**
 * GET /TKDocs/:id  (PC + HH)
 * Return:
 *  - head
 *  - trays under this head (tk_parent_id = head tk_id)
 *  - runlogs in TKRunLog for this tk_id
 */
exports.getTkDocById = async (req, res) => {
  const actor = actorOf(req);
  const id = String(req.params.id || "").trim();

  if (!id) return res.status(400).json({ message: "id is required", actor });

  try {
    const pool = await getPool();

    const headR = await pool.request()
      .input("id", sql.VarChar(20), id)
      .query(`SELECT TOP 1 * FROM ${SAFE_TKHEAD} WHERE tk_id = @id`);

    const head = headR.recordset?.[0];
    if (!head) return res.status(404).json({ message: "Not found", id, actor });

    const traysR = await pool.request()
      .input("id", sql.VarChar(20), id)
      .query(`
        SELECT
          d.*,
          p.part_no,
          p.part_name
        FROM ${SAFE_TKDETAIL} d
        LEFT JOIN dbo.part p ON p.part_id = d.part_id
        WHERE d.tk_parent_id = @id
        ORDER BY d.tk_created_at_ts ASC, d.tk_id ASC
      `);

    const runlogR = await pool.request()
      .input("id", sql.VarChar(20), id)
      .query(`
        SELECT
          run_no,
          lot_no,
          created_at_ts,
          created_by_u_id
        FROM dbo.TKRunLog
        WHERE tk_id = @id
        ORDER BY created_at_ts DESC
      `);

    return res.json({
      actor,
      head,
      trays: traysR.recordset,
      runlogs: runlogR.recordset,
    });
  } catch (err) {
    console.error("[TK][GET][ERROR]", err);
    return res.status(500).json({
      message: "Get failed",
      actor,
      error: err.message,
    });
  }
};