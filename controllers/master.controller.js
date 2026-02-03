// controllers/master.controller.js
const { sql, getPool } = require("../config/db");

async function listParts(req, res) {
  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const result = await pool.request().query(`
      SELECT part_id, part_no, part_name
      FROM part
      ORDER BY part_no ASC
    `);

    return res.json({ items: result.recordset });
  } catch (err) {
    console.error("LIST PARTS ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

async function listStations(req, res) {
  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const result = await pool.request().query(`
      SELECT op_sta_id, op_sta_code, op_sta_name, op_sta_seq
      FROM op_station
      ORDER BY op_sta_seq ASC
    `);

    return res.json({ items: result.recordset });
  } catch (err) {
    console.error("LIST STATIONS ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

module.exports = { listParts, listStations };
