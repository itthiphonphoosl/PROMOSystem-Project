// controllers/transfers.controller.js
const sql = require("mssql");
const { getPool } = require("../config/db");

const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";

function safeTableName(fullName) {
  const s = String(fullName || "");
  if (!/^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/.test(s)) {
    throw new Error(`Invalid table name: ${s}`);
  }
  return s;
}

const SAFE_TRANSFER = safeTableName(TRANSFER_TABLE);

function actorOf(req) {
  return {
    u_id: req.user?.u_id ?? null,
    u_name: req.user?.u_name ?? "unknown",
    role: req.user?.role ?? "unknown",
    u_type: req.user?.u_type ?? "unknown",
  };
}

// GET /api/transfers?tk_id=TKxxxx&limit=200
exports.listTransfers = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool = await getPool();

    const tk_id = req.query.tk_id ? String(req.query.tk_id).trim() : null;
    const limitRaw = req.query.limit ? Number(req.query.limit) : 200;
    const limit = Number.isFinite(limitRaw) ? Math.min(Math.max(limitRaw, 1), 500) : 200;

    const r = await pool
      .request()
      .input("tk_id", sql.VarChar(20), tk_id)
      .input("limit", sql.Int, limit)
      .query(`
        SELECT TOP (@limit)
          transfer_id,
          tk_id,
          from_lot_no,
          to_lot_no,
          tf_rs_code,
          transfer_qty,
          op_sc_id,
          MC_id,
          created_by_u_id,
          transfer_ts
        FROM ${SAFE_TRANSFER} WITH (NOLOCK)
        WHERE (@tk_id IS NULL OR tk_id = @tk_id)
        ORDER BY transfer_id DESC
      `);

    return res.json({
      actor,
      count: r.recordset.length,
      items: r.recordset,
    });
  } catch (err) {
    console.error("[TRANSFER_LIST][ERROR]", err);
    return res.status(500).json({ message: "List transfers failed", actor, error: err.message });
  }
};

// GET /api/transfers/:transfer_id
exports.getTransferById = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool = await getPool();
    const transfer_id = Number(req.params.transfer_id);

    if (!Number.isFinite(transfer_id)) {
      return res.status(400).json({ message: "transfer_id must be a number", actor });
    }

    const r = await pool
      .request()
      .input("transfer_id", sql.Int, transfer_id)
      .query(`
        SELECT TOP 1
          transfer_id,
          tk_id,
          from_lot_no,
          to_lot_no,
          tf_rs_code,
          transfer_qty,
          op_sc_id,
          MC_id,
          created_by_u_id,
          transfer_ts
        FROM ${SAFE_TRANSFER} WITH (NOLOCK)
        WHERE transfer_id = @transfer_id
      `);

    const row = r.recordset?.[0];
    if (!row) return res.status(404).json({ message: "Not found", actor, transfer_id });

    return res.json({ actor, item: row });
  } catch (err) {
    console.error("[TRANSFER_GET][ERROR]", err);
    return res.status(500).json({ message: "Get transfer failed", actor, error: err.message });
  }
};