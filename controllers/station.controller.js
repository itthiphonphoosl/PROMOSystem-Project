// controllers/station.controller.js
const { getPool } = require("../config/db");

function isValidStationId(id) {
  return /^STA\d+$/i.test(String(id || "").trim());
}

// GET /api/stations
async function listStations(req, res) {
  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const [rows] = await pool.query(
      `SELECT op_sta_id, op_sta_code, op_sta_name, CAST(op_sta_active AS UNSIGNED) AS op_sta_active
       FROM op_station
       ORDER BY op_sta_id ASC`
    );

    return res.json({ total: rows.length, stations: rows });
  } catch (err) {
    console.error("LIST STATIONS ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

// GET /api/stations/:id
async function getStationById(req, res) {
  const id = String(req.params.id || "").trim();
  if (!isValidStationId(id)) return res.status(400).json({ message: "Invalid station id" });

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const [rows] = await pool.query(
      `SELECT op_sta_id, op_sta_code, op_sta_name, CAST(op_sta_active AS UNSIGNED) AS op_sta_active
       FROM op_station
       WHERE op_sta_id = ?
       LIMIT 1`,
      [id]
    );

    const station = rows[0];
    if (!station) return res.status(404).json({ message: "Station not found" });

    return res.json({ station });
  } catch (err) {
    console.error("GET STATION ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

// PUT /api/stations/:id
async function updateStation(req, res) {
  const op_sta_id = String(req.params.id || "").trim();
  if (!isValidStationId(op_sta_id)) return res.status(400).json({ message: "Invalid station id" });

  const hasCode   = Object.prototype.hasOwnProperty.call(req.body, "op_sta_code");
  const hasName   = Object.prototype.hasOwnProperty.call(req.body, "op_sta_name");
  const hasActive = Object.prototype.hasOwnProperty.call(req.body, "op_sta_active");

  if (!hasCode && !hasName && !hasActive) {
    return res.status(400).json({ message: "ต้องส่งอย่างน้อย 1 ฟิลด์: op_sta_code หรือ op_sta_name หรือ op_sta_active" });
  }

  const sets    = [];
  const params  = [];
  const changed = {};

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    if (hasCode) {
      const op_sta_code = String(req.body.op_sta_code || "").trim();
      if (!op_sta_code) return res.status(400).json({ message: "op_sta_code ห้ามว่าง" });
      sets.push("op_sta_code = ?");
      params.push(op_sta_code);
      changed.op_sta_code = op_sta_code;
    }

    if (hasName) {
      const op_sta_name = String(req.body.op_sta_name || "").trim();
      if (!op_sta_name) return res.status(400).json({ message: "op_sta_name ห้ามว่าง" });
      sets.push("op_sta_name = ?");
      params.push(op_sta_name);
      changed.op_sta_name = op_sta_name;
    }

    if (hasActive) {
      const rawActive = req.body.op_sta_active;
      const op_sta_active = rawActive === true ? 1 : rawActive === false ? 0 : Number(rawActive);
      if (![0, 1].includes(op_sta_active)) return res.status(400).json({ message: "op_sta_active ต้องเป็น 0 หรือ 1" });
      sets.push("op_sta_active = ?");
      params.push(op_sta_active);
      changed.op_sta_active = op_sta_active;
    }

    params.push(op_sta_id);
    const [result] = await pool.query(`UPDATE op_station SET ${sets.join(", ")} WHERE op_sta_id = ?`, params);

    if ((result.affectedRows || 0) === 0) return res.status(404).json({ message: "Station not found" });

    const [afterRows] = await pool.query(
      `SELECT op_sta_id, op_sta_code, op_sta_name, CAST(op_sta_active AS UNSIGNED) AS op_sta_active
       FROM op_station WHERE op_sta_id = ? LIMIT 1`,
      [op_sta_id]
    );

    console.log(`[STATION][UPDATE] u_id=${req.user?.u_id ?? "?"} role=${req.user?.role ?? "?"} op_sta_id=${op_sta_id} updated=${JSON.stringify(changed)}`);

    return res.json({ message: "success", updated: changed, station: afterRows[0] });
  } catch (err) {
    console.error("UPDATE STATION ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

module.exports = { listStations, getStationById, updateStation };