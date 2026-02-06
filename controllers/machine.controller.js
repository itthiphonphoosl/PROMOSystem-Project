// controllers/machine.controller.js
const { sql, getPool } = require("../config/db");

function isValidMachineId(id) {
  // MC001, MC002, ... (ยอมให้มากกว่า 3 หลักเผื่อโตในอนาคต)
  return /^MC\d{3,}$/i.test(String(id || "").trim());
}

// GET /api/machines  (admin only)
async function listMachines(req, res) {
  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const result = await pool.request().query(`
      SELECT
        MC_id   AS mc_id,
        MC_name AS mc_name,
        CAST(MC_active AS INT) AS mc_active
      FROM [dbo].[machine]
      ORDER BY MC_id ASC
    `);

    const machines = result.recordset || [];
    return res.json({
      total: machines.length,
      machines,
    });
  } catch (err) {
    console.error("LIST MACHINES ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

// GET /api/machines/:id  (admin only)
async function getMachineById(req, res) {
  const id = String(req.params.id || "").trim();
  if (!isValidMachineId(id)) return res.status(400).json({ message: "Invalid machine id" });

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const result = await pool
      .request()
      .input("id", sql.VarChar(20), id)
      .query(`
        SELECT TOP 1
          MC_id   AS mc_id,
          MC_name AS mc_name,
          CAST(MC_active AS INT) AS mc_active
        FROM [dbo].[machine]
        WHERE MC_id = @id
      `);

    const machine = result.recordset[0];
    if (!machine) return res.status(404).json({ message: "Machine not found" });

    return res.json({ machine });
  } catch (err) {
    console.error("GET MACHINE ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

// POST /api/machines  (admin only)
// body: { mc_name, mc_active? }
// - สร้าง MC_id อัตโนมัติ: MC001 -> MC002 -> ...
async function createMachine(req, res) {
  const mc_name = String(req.body.mc_name || "").trim();
  const rawActive = req.body.mc_active;
  const mc_active =
    rawActive === undefined ? 1 :
    rawActive === true ? 1 :
    rawActive === false ? 0 :
    Number(rawActive);

  if (!mc_name) return res.status(400).json({ message: "mc_name ห้ามว่าง" });
  if (![0, 1].includes(mc_active)) return res.status(400).json({ message: "mc_active ต้องเป็น 0 หรือ 1" });

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const tx = new sql.Transaction(pool);
    await tx.begin();

    try {
      const last = await new sql.Request(tx).query(`
        SELECT TOP 1 MC_id
        FROM [dbo].[machine] WITH (UPDLOCK, HOLDLOCK)
        WHERE MC_id LIKE 'MC%'
        ORDER BY TRY_CONVERT(INT, SUBSTRING(MC_id, 3, 20)) DESC
      `);

      let running = 1;
      if (last.recordset.length > 0) {
        const lastId = String(last.recordset[0].MC_id || "");
        const n = parseInt(lastId.replace(/^MC/i, ""), 10);
        if (!Number.isNaN(n)) running = n + 1;
      }

      const mc_id = `MC${String(running).padStart(3, "0")}`;

      await new sql.Request(tx)
        .input("mc_id", sql.VarChar(20), mc_id)
        .input("mc_name", sql.NVarChar(255), mc_name)
        .input("mc_active", sql.Int, mc_active)
        .query(`
          INSERT INTO [dbo].[machine] (MC_id, MC_name, MC_active)
          VALUES (@mc_id, @mc_name, @mc_active)
        `);

      await tx.commit();

      // ✅ LOG (จำเป็นพอ)
      console.log(
        `[MACHINE][CREATE] u_name=${req.user?.u_name ?? "?"} role=${req.user?.role ?? "?"} mc_id=${mc_id} created=${JSON.stringify({ mc_name, mc_active })}`
      );

      return res.status(201).json({
        message: "created",
        machine: { mc_id, mc_name, mc_active },
      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("CREATE MACHINE ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

// PUT /api/machines/:id  (admin only)
// - อัปเดตได้เฉพาะ field ที่ส่งมา: mc_name, mc_active
// - ไม่มี DELETE: ถ้าจะ “ลบ” ให้ PUT mc_active = 0
async function updateMachine(req, res) {
  const mc_id = String(req.params.id || "").trim();
  if (!isValidMachineId(mc_id)) return res.status(400).json({ message: "Invalid machine id" });

  if (
    Object.prototype.hasOwnProperty.call(req.body, "mc_id") ||
    Object.prototype.hasOwnProperty.call(req.body, "MC_id")
  ) {
    return res.status(400).json({ message: "ห้ามส่ง mc_id มาแก้ไข (แก้ได้เฉพาะ mc_name, mc_active)" });
  }

  const hasName = Object.prototype.hasOwnProperty.call(req.body, "mc_name");
  const hasActive = Object.prototype.hasOwnProperty.call(req.body, "mc_active");

  if (!hasName && !hasActive) {
    return res.status(400).json({
      message: "ต้องส่งอย่างน้อย 1 ฟิลด์: mc_name หรือ mc_active",
    });
  }

  const sets = [];
  const changed = {};

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const r = pool.request().input("mc_id", sql.VarChar(20), mc_id);

    if (hasName) {
      const mc_name = String(req.body.mc_name || "").trim();
      if (!mc_name) return res.status(400).json({ message: "mc_name ห้ามว่าง" });

      sets.push("MC_name = @mc_name");
      r.input("mc_name", sql.NVarChar(255), mc_name);
      changed.mc_name = mc_name;
    }

    if (hasActive) {
      const rawActive = req.body.mc_active;
      const mc_active =
        rawActive === true ? 1 :
        rawActive === false ? 0 :
        Number(rawActive);

      if (![0, 1].includes(mc_active)) return res.status(400).json({ message: "mc_active ต้องเป็น 0 หรือ 1" });

      sets.push("MC_active = @mc_active");
      r.input("mc_active", sql.Int, mc_active);
      changed.mc_active = mc_active;
    }

    const result = await r.query(`
      UPDATE [dbo].[machine]
      SET ${sets.join(", ")}
      WHERE MC_id = @mc_id
    `);

    if ((result.rowsAffected?.[0] || 0) === 0) {
      return res.status(404).json({ message: "Machine not found" });
    }

    const after = await pool
      .request()
      .input("id", sql.VarChar(20), mc_id)
      .query(`
        SELECT TOP 1
          MC_id   AS mc_id,
          MC_name AS mc_name,
          CAST(MC_active AS INT) AS mc_active
        FROM [dbo].[machine]
        WHERE MC_id = @id
      `);

    console.log(
      `[MACHINE][UPDATE] u_name=${req.user?.u_name ?? "?"} role=${req.user?.role ?? "?"} mc_id=${mc_id} updated=${JSON.stringify(changed)}`
    );

    return res.json({
      message: "success",
      updated: changed,
      machine: after.recordset[0],
    });
  } catch (err) {
    console.error("UPDATE MACHINE ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

module.exports = {
  listMachines,
  getMachineById,
  createMachine,
  updateMachine,
};
