// controllers/part.controller.js
const sql = require("mssql");
const { getPool } = require("../config/db");

const PART_TABLE = process.env.PART_TABLE || "dbo.part";

function safeTableName(fullName) {
  const s = String(fullName || "");
  if (!/^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return s;
}

const SAFE_PART = safeTableName(PART_TABLE);

// ─────────────────────────────────────────────────────────
// GET /api/parts
// list ทั้งหมด — ?q=ค้นหา, ?all=true รวม inactive
// ─────────────────────────────────────────────────────────
exports.listParts = async (req, res) => {
  try {
    const pool    = await getPool();
    const q       = String(req.query.q || "").trim();
    const limit   = Math.min(Math.max(Number(req.query.limit || 500), 1), 2000);
    const showAll = req.query.all === "true";

    const r = await pool
      .request()
      .input("limit", sql.Int, limit)
      .input("q", sql.NVarChar(200), `%${q}%`)
      .query(`
        SELECT TOP (@limit)
          part_id, part_no, part_name, part_status,
          CASE part_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS part_status_label
        FROM ${SAFE_PART} WITH (NOLOCK)
        WHERE (@q = '%%' OR part_no LIKE @q OR part_name LIKE @q)
          ${showAll ? "" : "AND part_status = 1"}
        ORDER BY part_no ASC
      `);

    return res.json({ show_all: showAll, count: r.recordset.length, parts: r.recordset || [] });
  } catch (err) {
    return res.status(500).json({ message: "List parts failed", error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/parts/:part_id
// ดู part รายการเดียวตาม part_id
// ─────────────────────────────────────────────────────────
exports.getPartById = async (req, res) => {
  const part_id = Number(req.params.part_id);
  if (!Number.isFinite(part_id) || part_id <= 0) {
    return res.status(400).json({ message: "part_id must be a positive number" });
  }

  try {
    const pool = await getPool();
    const r = await pool
      .request()
      .input("part_id", sql.Int, part_id)
      .query(`
        SELECT
          part_id, part_no, part_name, part_status,
          CASE part_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS part_status_label
        FROM ${SAFE_PART} WITH (NOLOCK)
        WHERE part_id = @part_id
      `);

    const row = r.recordset?.[0];
    if (!row) return res.status(404).json({ message: "Part not found", part_id });

    return res.json({ item: row });
  } catch (err) {
    return res.status(500).json({ message: "Get part failed", error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// PUT /api/parts/:part_id
// แก้ไข part — แก้ได้ทุก field ยกเว้น part_id
// body: { part_no?, part_name?, part_status? }
// ─────────────────────────────────────────────────────────
exports.updatePart = async (req, res) => {
  const part_id = Number(req.params.part_id);
  if (!Number.isFinite(part_id) || part_id <= 0) {
    return res.status(400).json({ message: "part_id must be a positive number" });
  }

  const { part_no, part_name, part_status } = req.body ?? {};

  if (part_no === undefined && part_name === undefined && part_status === undefined) {
    return res.status(400).json({
      message: "ต้องระบุอย่างน้อย 1 field: part_no, part_name, part_status",
    });
  }

  if (part_status !== undefined) {
    if (Number(part_status) !== 0 && Number(part_status) !== 1) {
      return res.status(400).json({ message: "part_status ต้องเป็น 0 หรือ 1 เท่านั้น" });
    }
  }

  try {
    const pool = await getPool();

    const check = await pool
      .request()
      .input("part_id", sql.Int, part_id)
      .query(`SELECT TOP 1 part_id FROM ${SAFE_PART} WHERE part_id = @part_id`);

    if (!check.recordset?.[0]) {
      return res.status(404).json({ message: "Part not found", part_id });
    }

    const setClauses = [];
    const rq = pool.request().input("part_id", sql.Int, part_id);

    if (part_no !== undefined) {
      setClauses.push("part_no = @part_no");
      rq.input("part_no", sql.VarChar(50), String(part_no).trim());
    }
    if (part_name !== undefined) {
      setClauses.push("part_name = @part_name");
      rq.input("part_name", sql.NVarChar(200), String(part_name).trim());
    }
    if (part_status !== undefined) {
      setClauses.push("part_status = @part_status");
      rq.input("part_status", sql.TinyInt, Number(part_status));
    }

    await rq.query(`
      UPDATE ${SAFE_PART}
      SET ${setClauses.join(", ")}
      WHERE part_id = @part_id
    `);

    const updated = await pool
      .request()
      .input("part_id", sql.Int, part_id)
      .query(`
        SELECT
          part_id, part_no, part_name, part_status,
          CASE part_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS part_status_label
        FROM ${SAFE_PART}
        WHERE part_id = @part_id
      `);

    return res.json({ message: "อัปเดต part สำเร็จ", item: updated.recordset?.[0] });
  } catch (err) {
    return res.status(500).json({ message: "Update part failed", error: err.message });
  }
};