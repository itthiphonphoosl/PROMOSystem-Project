// controllers/user.controller.js
const bcrypt = require("bcryptjs");
const { sql, getPool } = require("../config/db");

// GET /api/user
// Anyone who is logged in (op/ad/ma)
async function getCurrentUser(req, res) {
  // req.user is already set by requireAuth middleware
  return res.json({ user: req.user });
}

// GET /api/users
// (มักจะใช้กับ admin/manager ผ่าน routes แต่ใน controller ไม่บังคับเพื่อไม่เปลี่ยน behavior เดิม)
async function listUsers(req, res) {
  const page = Math.max(1, Number(req.query.page || 1));
  const limit = Math.min(200, Math.max(1, Number(req.query.limit || 50)));
  const q = String(req.query.q || "").trim(); //เสิร์ช u_username, u_name
  const u_type = String(req.query.u_type || "").trim();
  const u_active = req.query.u_active === undefined ? null : Number(req.query.u_active); // 0/1

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    let where = "WHERE 1=1";
    const rCount = pool.request();
    const rData = pool.request();

    if (q) {
      where += " AND (u_username LIKE @q OR u_name LIKE @q)";
      rCount.input("q", sql.NVarChar(255), `%${q}%`);
      rData.input("q", sql.NVarChar(255), `%${q}%`);
    }

    if (u_type) {
      where += " AND u_type = @u_type";
      rCount.input("u_type", sql.VarChar(2), u_type);
      rData.input("u_type", sql.VarChar(2), u_type);
    }

    if (u_active === 0 || u_active === 1) {
      where += " AND u_active = @u_active";
      rCount.input("u_active", sql.Int, u_active);
      rData.input("u_active", sql.Int, u_active);
    }

    const offset = (page - 1) * limit;
    rData.input("offset", sql.Int, offset);
    rData.input("limit", sql.Int, limit);

    const totalRes = await rCount.query(`
      SELECT COUNT(1) AS total
      FROM [user]
      ${where}
    `);

    const total = Number(totalRes.recordset[0]?.total || 0);

    const dataRes = await rData.query(`
      SELECT
        u_id,
        u_username,
        u_name,
        u_type,
        u_active,
        u_created_ts,
        u_updated_ts
      FROM [user]
      ${where}
      ORDER BY u_id ASC
      OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `);

    return res.json({
      page,
      limit,
      total,
      totalPages: Math.ceil(total / limit),
      users: dataRes.recordset,
    });
  } catch (err) {
    console.error("LIST USERS ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

// GET /api/users/:id
async function getUserById(req, res) {
  const id = Number(req.params.id);

  if (!Number.isFinite(id) || id <= 0) {
    return res.status(400).json({ message: "Invalid user id" });
  }

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const result = await pool
      .request()
      .input("u_id", sql.Int, id)
      .query(`
        SELECT TOP 1
          u_id,
          u_username,
          u_name,
          u_type,
          u_active,
          u_created_ts,
          u_updated_ts
        FROM [user]
        WHERE u_id = @u_id
      `);

    const user = result.recordset[0];
    if (!user) return res.status(404).json({ message: "User not found" });

    return res.json({ user });
  } catch (err) {
    console.error("GET USER BY ID ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

/**
 * ✅ [เพิ่มใหม่] บังคับสิทธิ์: ให้เฉพาะ ad/ma เท่านั้น
 * - ไม่พึ่ง routes อย่างเดียว (กันพลาด)
 */
function assertAdminOrManager(req, res) {
  const u_type = String(req.user?.u_type || "").trim(); // ad / ma / op
  if (!["ad", "ma"].includes(u_type)) {
    res.status(403).json({ message: "Forbidden: admin/manager only" });
    return false;
  }
  return true;
}

/**
 * ✅ [เพิ่มใหม่] hash password ตามโหมด
 * - PASSWORD_MODE=plain => เก็บ plain
 * - default => bcrypt hash
 */
async function hashOrPlainPassword(password) {
  const mode = String(process.env.PASSWORD_MODE || "bcrypt").toLowerCase();
  if (mode === "plain") return String(password);
  return bcrypt.hash(String(password), 10);
}

/**
 * ✅ [เพิ่มใหม่] UPDATE ผู้ใช้ (แก้ได้ครบ 5 ฟิลด์)
 * PUT /api/users/:id
 * body: { u_name?, u_username?, u_password?, u_type?, u_active? }
 *
 * - u_active ใช้ soft delete ได้ (1 -> 0)
 * - u_password ถ้าส่งมา จะ hash ให้ตามโหมด
 * - u_username กันซ้ำให้
 * - สิทธิ์: เฉพาะ ad/ma เท่านั้น
 */
async function updateUser(req, res) {
  if (!assertAdminOrManager(req, res)) return;

  const id = Number(req.params.id);
  if (!Number.isFinite(id) || id <= 0) {
    return res.status(400).json({ message: "Invalid user id" });
  }

  // รับค่ามาแบบ optional (ไม่ส่งมาก็ไม่แก้)
  const u_name =
    req.body.u_name !== undefined ? String(req.body.u_name).trim() : undefined;

  const u_username =
    req.body.u_username !== undefined ? String(req.body.u_username).trim() : undefined;

  const u_password =
    req.body.u_password !== undefined ? String(req.body.u_password).trim() : undefined;

  const u_type =
    req.body.u_type !== undefined ? String(req.body.u_type).trim() : undefined;

  const u_active =
    req.body.u_active !== undefined ? Number(req.body.u_active) : undefined;

  // ต้องส่งมาอย่างน้อย 1 ฟิลด์
  const hasAny =
    u_name !== undefined ||
    u_username !== undefined ||
    u_password !== undefined ||
    u_type !== undefined ||
    u_active !== undefined;

  if (!hasAny) {
    return res.status(400).json({
      message: "No fields to update (ต้องส่งอย่างน้อย 1 ฟิลด์ใน u_name/u_username/u_password/u_type/u_active)",
    });
  }

  // validate ทีละตัว (เฉพาะตัวที่ส่งมา)
  if (u_name !== undefined && !u_name) {
    return res.status(400).json({ message: "u_name ห้ามเป็นค่าว่าง" });
  }

  if (u_username !== undefined && !u_username) {
    return res.status(400).json({ message: "u_username ห้ามเป็นค่าว่าง" });
  }

  if (u_password !== undefined && !u_password) {
    return res.status(400).json({ message: "u_password ห้ามเป็นค่าว่าง" });
  }

  if (u_type !== undefined && !["op", "ad", "ma"].includes(u_type)) {
    return res.status(400).json({ message: "u_type ต้องเป็น op, ad, ma เท่านั้น" });
  }

  if (u_active !== undefined && ![0, 1].includes(u_active)) {
    return res.status(400).json({ message: "u_active ต้องเป็น 0 หรือ 1 เท่านั้น" });
  }

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const tx = new sql.Transaction(pool);
    await tx.begin();

    try {
      // 1) เช็ค user มีจริง
      const exists = await new sql.Request(tx)
        .input("u_id", sql.Int, id)
        .query(`SELECT TOP 1 u_id FROM [user] WHERE u_id = @u_id`);

      if (exists.recordset.length === 0) {
        await tx.rollback();
        return res.status(404).json({ message: "User not found" });
      }

      // 2) ถ้าแก้ username ต้องกันซ้ำ
      if (u_username !== undefined) {
        const dup = await new sql.Request(tx)
          .input("u_username", sql.VarChar(50), u_username)
          .input("u_id", sql.Int, id)
          .query(`
            SELECT TOP 1 u_id
            FROM [user]
            WHERE u_username = @u_username AND u_id <> @u_id
          `);

        if (dup.recordset.length > 0) {
          await tx.rollback();
          return res.status(409).json({ message: "u_username นี้มีอยู่แล้ว" });
        }
      }

      // 3) ถ้าแก้ password ให้ hash/plain ตามโหมด
      const storedPassword =
        u_password !== undefined ? await hashOrPlainPassword(u_password) : undefined;

      const now = new Date();

      // 4) UPDATE ด้วย COALESCE (ส่ง null = ไม่แก้ / ส่งค่าจริง = แก้)
      await new sql.Request(tx)
        .input("u_id", sql.Int, id)
        .input("u_name", sql.NVarChar(255), u_name === undefined ? null : u_name)
        .input("u_username", sql.VarChar(50), u_username === undefined ? null : u_username)
        .input("u_password", sql.VarChar(255), storedPassword === undefined ? null : storedPassword)
        .input("u_type", sql.VarChar(2), u_type === undefined ? null : u_type)
        .input("u_active", sql.Int, u_active === undefined ? null : u_active)
        .input("u_updated_ts", sql.DateTime2(3), now)
        .query(`
          UPDATE [user]
          SET
            u_name       = COALESCE(@u_name, u_name),
            u_username   = COALESCE(@u_username, u_username),
            u_password   = COALESCE(@u_password, u_password),
            u_type       = COALESCE(@u_type, u_type),
            u_active     = COALESCE(@u_active, u_active),
            u_updated_ts = @u_updated_ts
          WHERE u_id = @u_id
        `);

      await tx.commit();

      return res.json({
        message: "อัปเดตผู้ใช้สำเร็จ",
        u_id: id,
        updatedFields: {
          u_name: u_name !== undefined,
          u_username: u_username !== undefined,
          u_password: u_password !== undefined,
          u_type: u_type !== undefined,
          u_active: u_active !== undefined,
        },
      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("UPDATE USER ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

module.exports = {
  getCurrentUser,
  listUsers,
  getUserById,
  updateUser, // ✅ [เพิ่มใหม่] ให้แก้ได้ 5 ฟิลด์ + บังคับสิทธิ์ ad/ma
};
