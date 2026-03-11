// controllers/color.controller.js
const { getPool } = require("../config/db");

const COLOR_TABLE = process.env.COLOR_TABLE || "dbo.color_painting";

function safeTableName(fullName) {
  const s = String(fullName || "").replace(/^[A-Za-z0-9_]+\./, "");
  if (!/^[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return `\`${s}\``;
}

const SAFE_COLOR = safeTableName(COLOR_TABLE);

function actorOf(req) {
  return {
    u_id:   req.user?.u_id   ?? null,
    u_name: req.user?.u_name ?? "unknown",
    role:   req.user?.role   ?? "unknown",
  };
}

// GET /api/colors
exports.listColors = async (req, res) => {
  const actor   = actorOf(req);
  const showAll = req.query.all === "true";

  try {
    const pool = getPool();
    const [rows] = await pool.query(`
      SELECT
        color_id, color_no, color_name, color_status,
        CASE color_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS color_status_label
      FROM ${SAFE_COLOR}
      ${showAll ? "" : "WHERE color_status = 1"}
      ORDER BY color_id ASC
    `);

    return res.json({ actor, show_all: showAll, count: rows.length, items: rows });
  } catch (err) {
    console.error("[COLOR_LIST][ERROR]", err);
    return res.status(500).json({ message: "List colors failed", actor, error: err.message });
  }
};

// GET /api/colors/:color_id
exports.getColorById = async (req, res) => {
  const actor    = actorOf(req);
  const color_id = Number(req.params.color_id);

  if (!Number.isFinite(color_id) || color_id <= 0) {
    return res.status(400).json({ message: "color_id must be a positive number", actor });
  }

  try {
    const pool = getPool();
    const [rows] = await pool.query(
      `SELECT
         color_id, color_no, color_name, color_status,
         CASE color_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS color_status_label
       FROM ${SAFE_COLOR}
       WHERE color_id = ?`,
      [color_id]
    );

    const row = rows[0];
    if (!row) return res.status(404).json({ message: "Color not found", actor, color_id });

    return res.json({ actor, item: row });
  } catch (err) {
    console.error("[COLOR_GET][ERROR]", err);
    return res.status(500).json({ message: "Get color failed", actor, error: err.message });
  }
};

// PUT /api/colors/:color_id
exports.updateColor = async (req, res) => {
  const actor    = actorOf(req);
  const color_id = Number(req.params.color_id);

  if (!Number.isFinite(color_id) || color_id <= 0) {
    return res.status(400).json({ message: "color_id must be a positive number", actor });
  }

  const { color_no, color_name, color_status } = req.body ?? {};

  if (color_no === undefined && color_name === undefined && color_status === undefined) {
    return res.status(400).json({ message: "ต้องระบุอย่างน้อย 1 field: color_no, color_name, color_status", actor });
  }

  if (color_status !== undefined) {
    const cs = Number(color_status);
    if (cs !== 0 && cs !== 1) {
      return res.status(400).json({ message: "color_status ต้องเป็น 0 หรือ 1 เท่านั้น", actor });
    }
  }

  try {
    const pool = getPool();

    const [checkRows] = await pool.query(
      `SELECT color_id FROM ${SAFE_COLOR} WHERE color_id = ? LIMIT 1`,
      [color_id]
    );
    if (!checkRows[0]) return res.status(404).json({ message: "Color not found", actor, color_id });

    const setClauses = [];
    const params     = [];

    if (color_no !== undefined) {
      setClauses.push("color_no = ?");
      params.push(String(color_no).trim());
    }
    if (color_name !== undefined) {
      setClauses.push("color_name = ?");
      params.push(String(color_name).trim());
    }
    if (color_status !== undefined) {
      setClauses.push("color_status = ?");
      params.push(Number(color_status));
    }

    params.push(color_id);
    await pool.query(`UPDATE ${SAFE_COLOR} SET ${setClauses.join(", ")} WHERE color_id = ?`, params);

    const [updRows] = await pool.query(
      `SELECT color_id, color_no, color_name, color_status,
              CASE color_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS color_status_label
       FROM ${SAFE_COLOR}
       WHERE color_id = ?`,
      [color_id]
    );

    return res.json({ actor, message: "อัปเดตสีสำเร็จ", item: updRows[0] });
  } catch (err) {
    console.error("[COLOR_UPDATE][ERROR]", err);
    return res.status(500).json({ message: "Update color failed", actor, error: err.message });
  }
};