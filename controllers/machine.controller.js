// controllers/machine.controller.js
const { getPool } = require("../config/db");

function isValidMachineId(id) {
  return /^MC\d{3,}$/i.test(String(id || "").trim());
}

function isValidStationId(id) {
  return /^STA\d+$/i.test(String(id || "").trim());
}

async function validateStationActive(pool, op_sta_id) {
  const [rows] = await pool.query(
    `SELECT op_sta_id, op_sta_name, CAST(op_sta_active AS UNSIGNED) AS op_sta_active
     FROM op_station
     WHERE op_sta_id = ?
     LIMIT 1`,
    [op_sta_id]
  );
  const row = rows[0];
  if (!row) return { ok: false, message: "Station not found" };
  if (Number(row.op_sta_active) !== 1) return { ok: false, message: "Station ถูกปิดใช้งาน" };
  return { ok: true, row };
}

async function listMachinesMyStation(req, res) {
  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const op_sta_id = req.user?.op_sta_id ? String(req.user.op_sta_id).trim() : "";

    if (!op_sta_id) {
      return res.status(400).json({
        message: "Missing station in token (a_op_sta_id is null). Please login again with op_sta_id.",
        hint: "POST /api/auth/login (HH operator) must write access.a_op_sta_id",
      });
    }

    const [staRows] = await pool.query(
      `SELECT op_sta_id, op_sta_name, CAST(op_sta_active AS UNSIGNED) AS op_sta_active
       FROM op_station
       WHERE op_sta_id = ?
       LIMIT 1`,
      [op_sta_id]
    );
    const sta = staRows[0];

    if (!sta) {
      return res.status(400).json({ message: `Station not found: ${op_sta_id}`, op_sta_id, hint: "Check op_station op_sta_id" });
    }
    if (Number(sta.op_sta_active) !== 1) {
      return res.status(403).json({
        message: `Station is inactive: ${sta.op_sta_id} (${sta.op_sta_name})`,
        station: { op_sta_id: sta.op_sta_id, op_sta_name: sta.op_sta_name, op_sta_active: Number(sta.op_sta_active) },
        hint: "Set op_station.op_sta_active = 1",
      });
    }

    const [rows] = await pool.query(
      `SELECT m.MC_id AS mc_id, m.MC_name AS mc_name,
              CAST(m.MC_active AS UNSIGNED) AS mc_active, m.op_sta_id, s.op_sta_name
       FROM \`machine\` m
       LEFT JOIN op_station s ON s.op_sta_id = m.op_sta_id
       WHERE m.op_sta_id = ? AND CAST(m.MC_active AS UNSIGNED) = 1
       ORDER BY m.MC_id ASC`,
      [sta.op_sta_id]
    );

    return res.json({ station: { op_sta_id: sta.op_sta_id, op_sta_name: sta.op_sta_name }, total: rows.length, machines: rows });
  } catch (err) {
    console.error("MY STATION MACHINES ERROR:", err);
    return res.status(500).json({ message: "Server error", error: err.message });
  }
}

// GET /api/machines
async function listMachines(req, res) {
  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const [rows] = await pool.query(
      `SELECT m.MC_id AS mc_id, m.MC_name AS mc_name,
              CAST(m.MC_active AS UNSIGNED) AS mc_active, m.op_sta_id, s.op_sta_name
       FROM \`machine\` m
       LEFT JOIN op_station s ON s.op_sta_id = m.op_sta_id
       ORDER BY m.MC_id ASC`
    );

    return res.json({ total: rows.length, machines: rows });
  } catch (err) {
    console.error("LIST MACHINES ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

// GET /api/machines/:id
async function getMachineById(req, res) {
  const id = String(req.params.id || "").trim();
  if (!isValidMachineId(id)) return res.status(400).json({ message: "Invalid machine id" });

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const [rows] = await pool.query(
      `SELECT m.MC_id AS mc_id, m.MC_name AS mc_name,
              CAST(m.MC_active AS UNSIGNED) AS mc_active, m.op_sta_id, s.op_sta_name
       FROM \`machine\` m
       LEFT JOIN op_station s ON s.op_sta_id = m.op_sta_id
       WHERE m.MC_id = ?
       LIMIT 1`,
      [id]
    );

    const machine = rows[0];
    if (!machine) return res.status(404).json({ message: "Machine not found" });

    return res.json({ machine });
  } catch (err) {
    console.error("GET MACHINE ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

// POST /api/machines
async function createMachine(req, res) {
  const mc_name  = String(req.body.mc_name || "").trim();
  const rawActive = req.body.mc_active;
  const mc_active = rawActive === undefined ? 1 : rawActive === true ? 1 : rawActive === false ? 0 : Number(rawActive);

  let op_sta_id = req.body.op_sta_id;
  if (op_sta_id === null || op_sta_id === undefined || String(op_sta_id).trim() === "") {
    op_sta_id = null;
  } else {
    op_sta_id = String(op_sta_id).trim();
  }

  if (!mc_name) return res.status(400).json({ message: "mc_name ห้ามว่าง" });
  if (![0, 1].includes(mc_active)) return res.status(400).json({ message: "mc_active ต้องเป็น 0 หรือ 1" });
  if (op_sta_id && !isValidStationId(op_sta_id)) return res.status(400).json({ message: "Invalid op_sta_id" });

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    let staName = null;
    if (op_sta_id) {
      const chk = await validateStationActive(pool, op_sta_id);
      if (!chk.ok) return res.status(400).json({ message: chk.message });
      staName = chk.row.op_sta_name;
    }

    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
      const [lastRows] = await conn.query(
        `SELECT MC_id FROM \`machine\`
         WHERE MC_id LIKE 'MC%'
         ORDER BY CAST(SUBSTRING(MC_id, 3) AS UNSIGNED) DESC
         LIMIT 1`
      );

      let running = 1;
      if (lastRows.length > 0) {
        const n = parseInt(String(lastRows[0].MC_id || "").replace(/^MC/i, ""), 10);
        if (!Number.isNaN(n)) running = n + 1;
      }

      const mc_id = `MC${String(running).padStart(3, "0")}`;

      await conn.query(
        `INSERT INTO \`machine\` (MC_id, MC_name, MC_active, op_sta_id) VALUES (?, ?, ?, ?)`,
        [mc_id, mc_name, mc_active, op_sta_id]
      );

      await conn.commit();
      conn.release();

      console.log(`[MACHINE][CREATE] u_name=${req.user?.u_name ?? "?"} role=${req.user?.role ?? "?"} mc_id=${mc_id} created=${JSON.stringify({ mc_name, mc_active, op_sta_id })}`);

      return res.status(201).json({ message: "created", machine: { mc_id, mc_name, mc_active, op_sta_id, op_sta_name: staName } });
    } catch (e) {
      await conn.rollback();
      conn.release();
      throw e;
    }
  } catch (err) {
    console.error("CREATE MACHINE ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

// PUT /api/machines/:id
async function updateMachine(req, res) {
  const mc_id = String(req.params.id || "").trim();
  if (!/^MC\d{3,}$/i.test(mc_id)) return res.status(400).json({ message: "Invalid machine id" });

  const forbiddenKeys = ["mc_id", "MC_id", "MC_No", "MC_no", "mc_no"];
  for (const k of forbiddenKeys) {
    if (Object.prototype.hasOwnProperty.call(req.body, k)) {
      return res.status(400).json({ message: "Do not send machine id/MC_No to update (allowed: mc_name, mc_active, op_sta_id)" });
    }
  }

  const hasName   = Object.prototype.hasOwnProperty.call(req.body, "mc_name")   || Object.prototype.hasOwnProperty.call(req.body, "MC_name");
  const hasActive = Object.prototype.hasOwnProperty.call(req.body, "mc_active") || Object.prototype.hasOwnProperty.call(req.body, "MC_active");
  const hasSta    = Object.prototype.hasOwnProperty.call(req.body, "op_sta_id");

  if (!hasName && !hasActive && !hasSta) {
    return res.status(400).json({ message: "Send at least one field: mc_name/MC_name, mc_active/MC_active, or op_sta_id" });
  }

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const sets    = [];
    const params  = [];
    const changed = {};

    if (hasName) {
      const mc_name = String(req.body.mc_name ?? req.body.MC_name ?? "").trim();
      if (!mc_name) return res.status(400).json({ message: "mc_name cannot be empty" });
      sets.push("MC_name = ?");
      params.push(mc_name);
      changed.mc_name = mc_name;
    }

    if (hasActive) {
      const rawActive = req.body.mc_active ?? req.body.MC_active;
      const mc_active = rawActive === true ? 1 : rawActive === false ? 0 : Number(rawActive);
      if (![0, 1].includes(mc_active)) return res.status(400).json({ message: "mc_active must be 0 or 1" });
      sets.push("MC_active = ?");
      params.push(mc_active);
      changed.mc_active = mc_active;
    }

    if (hasSta) {
      let op_sta_id = req.body.op_sta_id;
      if (op_sta_id === null || op_sta_id === undefined || String(op_sta_id).trim() === "") {
        op_sta_id = null;
      } else {
        op_sta_id = String(op_sta_id).trim();
        if (!/^STA\d+$/i.test(op_sta_id)) return res.status(400).json({ message: "Invalid op_sta_id" });

        const [staRows] = await pool.query(
          `SELECT op_sta_id, op_sta_name, CAST(op_sta_active AS UNSIGNED) AS op_sta_active
           FROM op_station WHERE op_sta_id = ? LIMIT 1`,
          [op_sta_id]
        );
        const staRow = staRows[0];
        if (!staRow) return res.status(400).json({ message: "Station not found" });
        if (Number(staRow.op_sta_active) !== 1) return res.status(400).json({ message: "Station is inactive" });
      }
      sets.push("op_sta_id = ?");
      params.push(op_sta_id);
      changed.op_sta_id = op_sta_id;
    }

    params.push(mc_id);
    const [result] = await pool.query(`UPDATE \`machine\` SET ${sets.join(", ")} WHERE MC_id = ?`, params);

    if ((result.affectedRows || 0) === 0) return res.status(404).json({ message: "Machine not found" });

    const [afterRows] = await pool.query(
      `SELECT m.MC_id AS mc_id, m.MC_name AS mc_name,
              CAST(m.MC_active AS UNSIGNED) AS mc_active, m.op_sta_id, s.op_sta_name
       FROM \`machine\` m
       LEFT JOIN op_station s ON s.op_sta_id = m.op_sta_id
       WHERE m.MC_id = ? LIMIT 1`,
      [mc_id]
    );

    return res.json({ message: "success", updated: changed, machine: afterRows[0] || null });
  } catch (err) {
    console.error("UPDATE MACHINE ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

module.exports = { listMachines, getMachineById, createMachine, updateMachine, listMachinesMyStation };