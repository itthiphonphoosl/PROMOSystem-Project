// controllers/part.controller.js
const { getPool } = require("../config/db");

const PART_TABLE = process.env.PART_TABLE || "dbo.part";

function safeTableName(fullName) {
  const s = String(fullName || "").replace(/^[A-Za-z0-9_]+\./, "");
  if (!/^[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return `\`${s}\``;
}

const SAFE_PART = safeTableName(PART_TABLE);

// GET /api/parts
exports.listParts = async (req, res) => {
  try {
    const pool    = getPool();
    const q       = String(req.query.q || "").trim();
    const limit   = Math.min(Math.max(Number(req.query.limit || 500), 1), 2000);
    const showAll = req.query.all === "true";

    const params = [];
    let whereExtra = showAll ? "" : "AND part_status = 1";

    let qFilter = "";
    if (q) {
      qFilter = "AND (part_no LIKE ? OR part_name LIKE ?)";
      params.push(`%${q}%`, `%${q}%`);
    }

    params.push(limit);

    const [rows] = await pool.query(
      `SELECT part_id, part_no, part_name, part_status,
              CASE part_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS part_status_label
       FROM ${SAFE_PART}
       WHERE 1=1 ${qFilter} ${whereExtra}
       ORDER BY part_no ASC
       LIMIT ?`,
      params
    );

    return res.json({ show_all: showAll, count: rows.length, parts: rows });
  } catch (err) {
    return res.status(500).json({ message: "List parts failed", error: err.message });
  }
};

// GET /api/parts/:part_id
exports.getPartById = async (req, res) => {
  const part_id = Number(req.params.part_id);
  if (!Number.isFinite(part_id) || part_id <= 0) {
    return res.status(400).json({ message: "part_id must be a positive number" });
  }

  try {
    const pool = getPool();
    const [rows] = await pool.query(
      `SELECT part_id, part_no, part_name, part_status,
              CASE part_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS part_status_label
       FROM ${SAFE_PART}
       WHERE part_id = ?`,
      [part_id]
    );

    const row = rows[0];
    if (!row) return res.status(404).json({ message: "Part not found", part_id });

    return res.json({ item: row });
  } catch (err) {
    return res.status(500).json({ message: "Get part failed", error: err.message });
  }
};

// PUT /api/parts/:part_id
exports.updatePart = async (req, res) => {
  const part_id = Number(req.params.part_id);
  if (!Number.isFinite(part_id) || part_id <= 0) {
    return res.status(400).json({ message: "part_id must be a positive number" });
  }

  const { part_no, part_name, part_status } = req.body ?? {};

  if (part_no === undefined && part_name === undefined && part_status === undefined) {
    return res.status(400).json({ message: "ต้องระบุอย่างน้อย 1 field: part_no, part_name, part_status" });
  }

  if (part_status !== undefined && Number(part_status) !== 0 && Number(part_status) !== 1) {
    return res.status(400).json({ message: "part_status ต้องเป็น 0 หรือ 1 เท่านั้น" });
  }

  try {
    const pool = getPool();

    const [checkRows] = await pool.query(`SELECT part_id FROM ${SAFE_PART} WHERE part_id = ? LIMIT 1`, [part_id]);
    if (!checkRows[0]) return res.status(404).json({ message: "Part not found", part_id });

    const setClauses = [];
    const params     = [];

    if (part_no !== undefined)     { setClauses.push("part_no = ?");     params.push(String(part_no).trim()); }
    if (part_name !== undefined)   { setClauses.push("part_name = ?");   params.push(String(part_name).trim()); }
    if (part_status !== undefined) { setClauses.push("part_status = ?"); params.push(Number(part_status)); }

    params.push(part_id);
    await pool.query(`UPDATE ${SAFE_PART} SET ${setClauses.join(", ")} WHERE part_id = ?`, params);

    const [updRows] = await pool.query(
      `SELECT part_id, part_no, part_name, part_status,
              CASE part_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS part_status_label
       FROM ${SAFE_PART} WHERE part_id = ?`,
      [part_id]
    );

    return res.json({ message: "อัปเดต part สำเร็จ", item: updRows[0] });
  } catch (err) {
    return res.status(500).json({ message: "Update part failed", error: err.message });
  }
};
// POST /api/parts
exports.createPart = async (req, res) => {
  const { part_no, part_name, part_status } = req.body ?? {};

  if (!part_no || !String(part_no).trim()) {
    return res.status(400).json({ message: "กรุณาระบุ part_no" });
  }
  if (!part_name || !String(part_name).trim()) {
    return res.status(400).json({ message: "กรุณาระบุ part_name" });
  }

  const status = part_status !== undefined ? Number(part_status) : 1;
  if (status !== 0 && status !== 1) {
    return res.status(400).json({ message: "part_status ต้องเป็น 0 หรือ 1 เท่านั้น" });
  }

  try {
    const pool = getPool();

    // ตรวจ duplicate part_no
    const [dup] = await pool.query(
      `SELECT part_id FROM ${SAFE_PART} WHERE part_no = ? LIMIT 1`,
      [String(part_no).trim()]
    );
    if (dup[0]) {
      return res.status(409).json({ message: `part_no "${part_no}" มีอยู่ในระบบแล้ว` });
    }

    const [result] = await pool.query(
      `INSERT INTO ${SAFE_PART} (part_no, part_name, part_status) VALUES (?, ?, ?)`,
      [String(part_no).trim(), String(part_name).trim(), status]
    );

    const [newRow] = await pool.query(
      `SELECT part_id, part_no, part_name, part_status,
              CASE part_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS part_status_label
       FROM ${SAFE_PART} WHERE part_id = ?`,
      [result.insertId]
    );

    return res.status(201).json({ message: "เพิ่ม part สำเร็จ", item: newRow[0] });
  } catch (err) {
    return res.status(500).json({ message: "Create part failed", error: err.message });
  }
};