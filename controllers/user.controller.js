// controllers/user.controller.js
const bcrypt = require("bcryptjs");
const { getPool } = require("../config/db");

// GET /api/user
async function getCurrentUser(req, res) {
  return res.json({ user: req.user });
}

// GET /api/users
async function listUsers(req, res) {
  const page     = Math.max(1, Number(req.query.page  || 1));
  const limit    = Math.min(200, Math.max(1, Number(req.query.limit || 50)));
  const q        = String(req.query.q       || "").trim();
  const u_type   = String(req.query.u_type  || "").trim();
  const u_active = req.query.u_active === undefined ? null : Number(req.query.u_active);

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    let where = "WHERE 1=1";
    const params = [];

    if (q) {
      where += " AND (u_username LIKE ? OR u_name LIKE ?)";
      params.push(`%${q}%`, `%${q}%`);
    }
    if (u_type) {
      where += " AND u_type = ?";
      params.push(u_type);
    }
    if (u_active === 0 || u_active === 1) {
      where += " AND u_active = ?";
      params.push(u_active);
    }

    const offset = (page - 1) * limit;

    const [[countRow]] = await pool.query(
      `SELECT COUNT(1) AS total FROM \`user\` ${where}`,
      params
    );
    const total = Number(countRow?.total || 0);

    const [rows] = await pool.query(
      `SELECT u_id, u_username, u_firstname, u_lastname, u_type, u_active, u_created_ts, u_updated_ts
       FROM \`user\`
       ${where}
       ORDER BY u_id ASC
       LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    );

    return res.json({
      page,
      limit,
      total,
      totalPages: Math.ceil(total / limit),
      users: rows,
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
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const [rows] = await pool.query(
      `SELECT u_id, u_username, u_firstname, u_lastname, u_type, u_active, u_created_ts, u_updated_ts
       FROM \`user\`
       WHERE u_id = ?
       LIMIT 1`,
      [id]
    );

    const user = rows[0];
    if (!user) return res.status(404).json({ message: "User not found" });

    return res.json({ user });
  } catch (err) {
    console.error("GET USER BY ID ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

function assertAdmin(req, res) {
  const u_type = String(req.user?.u_type || "").trim();
  if (u_type !== "ad") {
    res.status(403).json({ message: "Forbidden: admin only" });
    return false;
  }
  return true;
}

async function hashOrPlainPassword(password) {
  const mode = String(process.env.PASSWORD_MODE || "bcrypt").toLowerCase();
  if (mode === "plain") return String(password);
  return bcrypt.hash(String(password), 10);
}

// PUT /api/users/:id
async function updateUser(req, res) {
  if (!assertAdmin(req, res)) return;

  const id = Number(req.params.id);
  if (!Number.isFinite(id) || id <= 0) {
    return res.status(400).json({ message: "Invalid user id" });
  }

  const u_firstname = req.body.u_firstname !== undefined ? String(req.body.u_firstname).trim() : undefined;
  const u_lastname  = req.body.u_lastname  !== undefined ? String(req.body.u_lastname).trim()  : undefined;
  const u_username = req.body.u_username !== undefined ? String(req.body.u_username).trim() : undefined;
  const u_password = req.body.u_password !== undefined ? String(req.body.u_password).trim() : undefined;
  const u_type     = req.body.u_type     !== undefined ? String(req.body.u_type).trim()     : undefined;
  const u_active   = req.body.u_active   !== undefined ? Number(req.body.u_active)          : undefined;

  const hasAny = u_firstname !== undefined || u_lastname !== undefined || u_username !== undefined ||
    u_password !== undefined || u_type !== undefined || u_active !== undefined;

  if (!hasAny) {
    return res.status(400).json({
      message: "No fields to update (ต้องส่งอย่างน้อย 1 ฟิลด์ใน u_firstname/u_lastname/u_username/u_password/u_type/u_active)",
    });
  }

  if (u_firstname !== undefined && !u_firstname) return res.status(400).json({ message: "u_firstname ห้ามเป็นค่าว่าง" });
  if (u_lastname  !== undefined && !u_lastname)  return res.status(400).json({ message: "u_lastname ห้ามเป็นค่าว่าง" });
  if (u_username !== undefined && !u_username) return res.status(400).json({ message: "u_username ห้ามเป็นค่าว่าง" });
  if (u_password !== undefined && !u_password) return res.status(400).json({ message: "u_password ห้ามเป็นค่าว่าง" });
  if (u_type     !== undefined && !["op", "ad"].includes(u_type)) return res.status(400).json({ message: "u_type ต้องเป็น op หรือ ad เท่านั้น" });
  if (u_active   !== undefined && ![0, 1].includes(u_active))     return res.status(400).json({ message: "u_active ต้องเป็น 0 หรือ 1 เท่านั้น" });

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
      // 1) เช็ค user มีจริง
      const [existRows] = await conn.query(
        `SELECT u_id FROM \`user\` WHERE u_id = ? LIMIT 1`,
        [id]
      );
      if (existRows.length === 0) {
        await conn.rollback();
        conn.release();
        return res.status(404).json({ message: "User not found" });
      }

      // 2) ถ้าแก้ username ต้องกันซ้ำ
      if (u_username !== undefined) {
        const [dupRows] = await conn.query(
          `SELECT u_id FROM \`user\` WHERE u_username = ? AND u_id <> ? LIMIT 1`,
          [u_username, id]
        );
        if (dupRows.length > 0) {
          await conn.rollback();
          conn.release();
          return res.status(409).json({ message: "u_username นี้มีอยู่แล้ว" });
        }
      }

      const storedPassword = u_password !== undefined ? await hashOrPlainPassword(u_password) : undefined;
      const now = new Date();

      await conn.query(
        `UPDATE \`user\`
         SET
           u_firstname  = COALESCE(?, u_firstname),
           u_lastname   = COALESCE(?, u_lastname),
           u_username   = COALESCE(?, u_username),
           u_password   = COALESCE(?, u_password),
           u_type       = COALESCE(?, u_type),
           u_active     = COALESCE(?, u_active),
           u_updated_ts = ?
         WHERE u_id = ?`,
        [
          u_firstname ?? null,
          u_lastname  ?? null,
          u_username ?? null,
          storedPassword ?? null,
          u_type     ?? null,
          u_active   ?? null,
          now,
          id,
        ]
      );

      await conn.commit();
      conn.release();

      return res.json({
        message: "อัปเดตผู้ใช้สำเร็จ",
        u_id: id,
        updatedFields: {
          u_firstname: u_firstname !== undefined,
          u_lastname:  u_lastname  !== undefined,
          u_username: u_username !== undefined,
          u_password: u_password !== undefined,
          u_type:     u_type     !== undefined,
          u_active:   u_active   !== undefined,
        },
      });
    } catch (e) {
      await conn.rollback();
      conn.release();
      throw e;
    }
  } catch (err) {
    console.error("UPDATE USER ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

module.exports = { getCurrentUser, listUsers, getUserById, updateUser };