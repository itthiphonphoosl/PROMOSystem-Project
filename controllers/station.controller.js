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
      stations, // ✅ op_sta_active จะเป็น 0/1 แล้ว
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
          op_sta_active
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


// async function createStation(req, res) {
//   const code = String(req.body.op_sta_code || "").trim();
//   const name = String(req.body.op_sta_name || "").trim();
//   const active = req.body.op_sta_active === undefined ? 1 : Number(req.body.op_sta_active);

//   if (!code || !name) {
//     return res.status(400).json({ message: "กรุณากรอก op_sta_code และ op_sta_name" });
//   }

//   if (![0, 1].includes(active)) {
//     return res.status(400).json({ message: "op_sta_active ต้องเป็น 0 หรือ 1" });
//   }

//   try {
//     const pool = await getPool();
//     if (!pool) return res.status(500).json({ message: "Database connection failed" });

//     const tx = new sql.Transaction(pool);
//     await tx.begin();

//     try {
//       // กัน code ซ้ำ
//       const dup = await new sql.Request(tx)
//         .input("code", sql.VarChar(20), code)
//         .query(`SELECT TOP 1 op_sta_id FROM [dbo].[op_station] WHERE op_sta_code = @code`);

//       if (dup.recordset.length > 0) {
//         await tx.rollback();
//         return res.status(409).json({ message: "op_sta_code นี้มีอยู่แล้ว" });
//       }

//       // gen id: STA001, STA002 ...
//       const last = await new sql.Request(tx).query(`
//         SELECT TOP 1 op_sta_id
//         FROM [dbo].[op_station] WITH (UPDLOCK, HOLDLOCK)
//         WHERE op_sta_id LIKE 'STA%'
//         ORDER BY op_sta_id DESC
//       `);

//       let running = 1;
//       if (last.recordset.length > 0) {
//         const lastId = String(last.recordset[0].op_sta_id);
//         const n = parseInt(lastId.replace(/^STA/i, ""), 10);
//         if (!Number.isNaN(n)) running = n + 1;
//       }

//       const op_sta_id = `STA${String(running).padStart(3, "0")}`;

//       await new sql.Request(tx)
//         .input("id", sql.VarChar(20), op_sta_id)
//         .input("code", sql.VarChar(20), code)
//         .input("name", sql.NVarChar(255), name)
//         .input("active", sql.Int, active)
//         .query(`
//           INSERT INTO [dbo].[op_station] (op_sta_id, op_sta_code, op_sta_name, op_sta_active)
//           VALUES (@id, @code, @name, @active)
//         `);

//       await tx.commit();
//       return res.status(201).json({
//         message: "สร้าง Station สำเร็จ",
//         station: { op_sta_id, op_sta_code: code, op_sta_name: name, op_sta_active: active },
//       });
//     } catch (e) {
//       await tx.rollback();
//       throw e;
//     }
//   } catch (err) {
//     console.error("CREATE STATION ERROR:", err);
//     return res.status(500).json({ message: "Server error" });
//   }
// }


// PUT /api/stations/:id
// ✅ อัปเดตได้เฉพาะฟิลด์ที่ส่งมา: op_sta_code, op_sta_name, op_sta_active
// ❌ ไม่ให้อัปเดต: op_sta_id
async function updateStation(req, res) {
  const op_sta_id = String(req.params.id || "").trim();
  if (!op_sta_id) return res.status(400).json({ message: "Missing station id" });

  // ✅ รับเฉพาะ field ที่ส่งมา (ไม่บังคับครบ)
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
      if (!op_sta_code) {
        return res.status(400).json({ message: "op_sta_code ห้ามว่าง" });
      }
      sets.push("op_sta_code = @op_sta_code");
      r.input("op_sta_code", sql.VarChar(20), op_sta_code);
      changed.op_sta_code = op_sta_code;
    }

    if (hasName) {
      const op_sta_name = String(req.body.op_sta_name || "").trim();
      if (!op_sta_name) {
        return res.status(400).json({ message: "op_sta_name ห้ามว่าง" });
      }
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

    // ✅ ส่งกลับ station ล่าสุด (active เป็น 0/1)
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



// // PATCH /api/stations/:id/active  body: { active: 0|1 }
// async function setStationActive(req, res) {
//   const id = String(req.params.id || "").trim();
//   if (!isValidStationId(id)) return res.status(400).json({ message: "Invalid station id" });

//   const active = Number(req.body.active);
//   if (![0, 1].includes(active)) return res.status(400).json({ message: "active ต้องเป็น 0 หรือ 1" });

//   try {
//     const pool = await getPool();
//     if (!pool) return res.status(500).json({ message: "Database connection failed" });

//     const result = await pool
//       .request()
//       .input("id", sql.VarChar(20), id)
//       .input("active", sql.Int, active)
//       .query(`
//         UPDATE [dbo].[op_station]
//         SET op_sta_active = @active
//         WHERE op_sta_id = @id
//       `);

//     const rows = result.rowsAffected?.[0] || 0;
//     if (rows === 0) return res.status(404).json({ message: "Station not found" });

//     return res.json({ message: "อัปเดตสถานะ Station สำเร็จ", op_sta_id: id, op_sta_active: active });
//   } catch (err) {
//     console.error("SET STATION ACTIVE ERROR:", err);
//     return res.status(500).json({ message: "Server error" });
//   }
// }

module.exports = { listStations, getStationById, updateStation,  };
// setStationActive createStation 