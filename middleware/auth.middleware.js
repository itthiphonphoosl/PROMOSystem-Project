const { sql, getPool } = require("../config/db");

function mapUserTypeToRole(u_type) {
  if (u_type === "op") return "operator";
  if (u_type === "ad") return "admin";
  if (u_type === "ma") return "manager";
  return "unknown";
}

// checking token in DB (access.a_token) + not expired (a_expired)
async function requireAuth(req, res, next) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7) : null;

  if (!token) return res.status(401).json({ message: "Missing token" });

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "Database connection failed" });

    const now = new Date();

    const result = await pool
      .request()
      .input("a_token", sql.NVarChar(500), token)
      .input("now", sql.DateTime2(3), now)
      .query(`
        SELECT TOP 1
          a.u_id,
          a.a_created,
          a.a_expired,
          a.a_client_type,
          u.u_username,
          u.u_name,
          u.u_type,
          u.u_active
        FROM access a
        JOIN [user] u ON u.u_id = a.u_id
        WHERE a.a_token = @a_token
          AND a.a_expired > @now
        ORDER BY a.a_created DESC
      `);

    const row = result.recordset[0];

    if (!row) {
      return res.status(401).json({ message: "Token expired or invalid, please login again" });
    }

    if (row.u_active !== 1) {
      return res.status(403).json({ message: "User is inactive" });
    }

    req.user = {
      u_id: String(row.u_id),
      u_username: row.u_username,
      u_name: row.u_name,
      u_type: row.u_type,
      role: mapUserTypeToRole(row.u_type),
      clientType: row.a_client_type,
      tokenExpiresAt: row.a_expired, 
    };

    next();
  } catch (err) {
    console.error("AUTH ERROR:", err);
    return res.status(500).json({ message: "Server error" });
  }
}

function requireRole(allowedRoles = []) {
  return (req, res, next) => {
    const role = req.user?.role;
    if (!role) return res.status(401).json({ message: "Unauthorized" });

    if (!allowedRoles.includes(role)) {
      return res.status(403).json({ message: "Forbidden" });
    }

    next();
  };
}

module.exports = { requireAuth, requireRole };
