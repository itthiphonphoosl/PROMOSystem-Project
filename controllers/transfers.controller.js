// controllers/transfers.controller.js
const { getPool } = require("../config/db");

const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";

function safeTableName(fullName) {
  const s = String(fullName || "").replace(/^[A-Za-z0-9_]+\./, "");
  if (!/^[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return `\`${s}\``;
}

const SAFE_TRANSFER = safeTableName(TRANSFER_TABLE);

function actorOf(req) {
  return {
    u_id:   req.user?.u_id   ?? null,
    u_firstname: req.user?.u_firstname ?? "",
    u_lastname:  req.user?.u_lastname  ?? "",
    role:   req.user?.role   ?? "unknown",
    u_type: req.user?.u_type ?? "unknown",
  };
}

// GET /api/transfers?tk_id=TKxxxx&limit=200
exports.listTransfers = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool     = getPool();
    const tk_id    = req.query.tk_id ? String(req.query.tk_id).trim() : null;
    const limitRaw = req.query.limit  ? Number(req.query.limit) : 200;
    const limit    = Number.isFinite(limitRaw) ? Math.min(Math.max(limitRaw, 1), 500) : 200;

    const params = [];
    let whereClause = "";

    if (tk_id) {
      whereClause = "WHERE t.from_tk_id = ? OR t.to_tk_id = ?";
      params.push(tk_id, tk_id);
    }

    params.push(limit);

    const [rows] = await pool.query(
      `SELECT
         t.transfer_id, t.from_tk_id, t.to_tk_id, t.from_lot_no, t.to_lot_no,
         t.tf_rs_code, tr.tf_rs_name, t.transfer_qty, t.op_sc_id,
         t.MC_id, m.MC_name, s.op_sta_id, s.op_sta_name,
         t.created_by_u_id, u.u_firstname AS created_by_u_firstname, u.u_lastname AS created_by_u_lastname, t.transfer_ts
       FROM ${SAFE_TRANSFER} t
       LEFT JOIN \`transfer_reason\` tr ON tr.tf_rs_code = t.tf_rs_code
       LEFT JOIN \`machine\`         m  ON m.MC_id       = t.MC_id
       LEFT JOIN \`op_station\`      s  ON s.op_sta_id   = m.op_sta_id
       LEFT JOIN \`user\`            u  ON u.u_id        = t.created_by_u_id
       ${whereClause}
       ORDER BY t.transfer_id DESC
       LIMIT ?`,
      params
    );

    return res.json({ actor, count: rows.length, items: rows });
  } catch (err) {
    console.error("[TRANSFER_LIST][ERROR]", err);
    return res.status(500).json({ message: "List transfers failed", actor, error: err.message });
  }
};

// GET /api/transfers/:transfer_id
exports.getTransferById = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool        = getPool();
    const transfer_id = Number(req.params.transfer_id);

    if (!Number.isFinite(transfer_id)) {
      return res.status(400).json({ message: "transfer_id must be a number", actor });
    }

    const [rows] = await pool.query(
      `SELECT
         t.transfer_id, t.from_tk_id, t.to_tk_id, t.from_lot_no, t.to_lot_no,
         t.tf_rs_code, tr.tf_rs_name, t.transfer_qty, t.op_sc_id,
         t.MC_id, m.MC_name, s.op_sta_id, s.op_sta_name,
         t.created_by_u_id, u.u_firstname AS created_by_u_firstname, u.u_lastname AS created_by_u_lastname, t.transfer_ts
       FROM ${SAFE_TRANSFER} t
       LEFT JOIN \`transfer_reason\` tr ON tr.tf_rs_code = t.tf_rs_code
       LEFT JOIN \`machine\`         m  ON m.MC_id       = t.MC_id
       LEFT JOIN \`op_station\`      s  ON s.op_sta_id   = m.op_sta_id
       LEFT JOIN \`user\`            u  ON u.u_id        = t.created_by_u_id
       WHERE t.transfer_id = ?
       LIMIT 1`,
      [transfer_id]
    );

    const row = rows[0];
    if (!row) return res.status(404).json({ message: "Not found", actor, transfer_id });

    return res.json({ actor, item: row });
  } catch (err) {
    console.error("[TRANSFER_GET][ERROR]", err);
    return res.status(500).json({ message: "Get transfer failed", actor, error: err.message });
  }
};