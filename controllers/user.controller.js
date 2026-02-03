// controllers/user.controller.js
const { sql, getPool } = require("../config/db");

// GET /api/user
// Anyone who is logged in (op/ad/ma)
async function getCurrentUser(req, res) {
  // req.user is already set by requireAuth middleware
  return res.json({ user: req.user });
}


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
// Admin/Manager only
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

module.exports = { getCurrentUser, listUsers, getUserById };
