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

exports.listParts = async (req, res) => {
  try {
    const pool = await getPool();
    const q = String(req.query.q || "").trim();
    const limit = Math.min(Math.max(Number(req.query.limit || 500), 1), 2000);

    const r = await pool
      .request()
      .input("limit", sql.Int, limit)
      .input("q", sql.NVarChar(200), `%${q}%`)
      .query(`
        SELECT TOP (@limit)
          part_id, part_no, part_name
        FROM ${SAFE_PART} WITH (NOLOCK)
        WHERE (@q = '%%' OR part_no LIKE @q OR part_name LIKE @q)
        ORDER BY part_no ASC
      `);

    return res.json({ parts: r.recordset || [] });
  } catch (err) {
    return res.status(500).json({ message: "List parts failed", error: err.message });
  }
};