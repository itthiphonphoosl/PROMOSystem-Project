// controllers/station.controller.js
const { sql, getPool } = require("../config/db");

function isValidStationId(id) {
  return /^STA\d+$/i.test(String(id || "").trim());
}

// GET /api/stations
async function listStations(req, res) {
  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const result = await pool.request().query(`
      SELECT
        op_sta_id,
        op_sta_code,
        op_sta_name,
        CAST(op_sta_active AS INT) AS op_sta_active
      FROM [dbo].[op_station]
      ORDER BY op_sta_id ASC
    `);

    const stations = result.recordset || [];
    return res.json({
      total: stations.length,
      stations,
    });
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
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const result = await pool
      .request()
      .input("id", sql.VarChar(20), id)
      .query(`
        SELECT TOP 1
          op_sta_id,
          op_sta_code,
          op_sta_name,
          CAST(op_sta_active AS INT) AS op_sta_active
        FROM [dbo].[op_station]
        WHERE op_sta_id = @id
      `);

    const station = result.recordset[0];
    if (!station) return res.status(404).json({ message: "Station not found" });

    return res.json({ station });
  } catch (err) {
    console.error("GET STATION ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

// PUT /api/stations/:id
// ✅ ส่งมาแค่ field ที่อยากแก้ก็พอ: op_sta_code, op_sta_name, op_sta_active (0/1 หรือ true/false)
// ❌ ไม่ให้อัปเดต: op_sta_id
async function updateStation(req, res) {
  const op_sta_id = String(req.params.id || "").trim();
  if (!isValidStationId(op_sta_id)) {
    return res.status(400).json({ message: "Invalid station id" });
  }

  const hasCode = Object.prototype.hasOwnProperty.call(req.body, "op_sta_code");
  const hasName = Object.prototype.hasOwnProperty.call(req.body, "op_sta_name");
  const hasActive = Object.prototype.hasOwnProperty.call(req.body, "op_sta_active");

  if (!hasCode && !hasName && !hasActive) {
    return res.status(400).json({
      message: "ต้องส่งอย่างน้อย 1 ฟิลด์: op_sta_code หรือ op_sta_name หรือ op_sta_active",
    });
  }

  const sets = [];
  const changed = {};

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const r = pool.request().input("op_sta_id", sql.VarChar(20), op_sta_id);

    if (hasCode) {
      const op_sta_code = String(req.body.op_sta_code || "").trim();
      if (!op_sta_code) return res.status(400).json({ message: "op_sta_code ห้ามว่าง" });

      sets.push("op_sta_code = @op_sta_code");
      r.input("op_sta_code", sql.VarChar(20), op_sta_code);
      changed.op_sta_code = op_sta_code;
    }

    if (hasName) {
      const op_sta_name = String(req.body.op_sta_name || "").trim();
      if (!op_sta_name) return res.status(400).json({ message: "op_sta_name ห้ามว่าง" });

      sets.push("op_sta_name = @op_sta_name");
      r.input("op_sta_name", sql.NVarChar(255), op_sta_name);
      changed.op_sta_name = op_sta_name;
    }

    if (hasActive) {
      const rawActive = req.body.op_sta_active;
      const op_sta_active =
        rawActive === true ? 1 :
        rawActive === false ? 0 :
        Number(rawActive);

      if (![0, 1].includes(op_sta_active)) {
        return res.status(400).json({ message: "op_sta_active ต้องเป็น 0 หรือ 1" });
      }

      sets.push("op_sta_active = @op_sta_active");
      r.input("op_sta_active", sql.Int, op_sta_active);
      changed.op_sta_active = op_sta_active;
    }

    const q = `
      UPDATE [dbo].[op_station]
      SET ${sets.join(", ")}
      WHERE op_sta_id = @op_sta_id
    `;

    const result = await r.query(q);
    if ((result.rowsAffected?.[0] || 0) === 0) {
      return res.status(404).json({ message: "Station not found" });
    }

    const after = await pool
      .request()
      .input("id", sql.VarChar(20), op_sta_id)
      .query(`
        SELECT TOP 1
          op_sta_id,
          op_sta_code,
          op_sta_name,
          CAST(op_sta_active AS INT) AS op_sta_active
        FROM [dbo].[op_station]
        WHERE op_sta_id = @id
      `);

    // ✅ log แบบสั้นๆเหมือน login/logout (พอรู้ว่า backend ทำอะไร)
    console.log(
      `[STATION][UPDATE] u_id=${req.user?.u_id ?? "?"} role=${req.user?.role ?? "?"} op_sta_id=${op_sta_id} updated=${JSON.stringify(changed)}`
    );

    return res.json({
      message: "success",
      updated: changed,
      station: after.recordset[0],
    });
  } catch (err) {
    console.error("UPDATE STATION ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

module.exports = { listStations, getStationById, updateStation };
