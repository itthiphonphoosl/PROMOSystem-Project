const { sql, getPool } = require("../config/db");

// map u_type -> role
function mapUserTypeToRole(u_type) {
  if (u_type === "op") return "operator";
  if (u_type === "ad") return "admin";
  if (u_type === "ma") return "manager";
  return "unknown";
}

/**
 * normalize x-client-type -> PC / HH / UNKNOWN
 */
function normalizeClientType(raw) {
  const v = String(raw || "").trim().toUpperCase();
  if (v === "PC" || v === "REACT") return "PC";
  if (v === "HH" || v === "FLUTTER") return "HH";
  return "UNKNOWN";
}

// checking token in DB (access.a_token) + not expired (a_expired)
async function requireAuth(req, res, next) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : null;

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
          a.a_op_sta_id,

          s.op_sta_code,
          s.op_sta_name,
          s.op_sta_active,

          u.u_username,
          u.u_name,
          u.u_type,
          u.u_active
        FROM access a
        JOIN [user] u ON u.u_id = a.u_id
        LEFT JOIN dbo.op_station s ON s.op_sta_id = a.a_op_sta_id
        WHERE a.a_token = @a_token
          AND a.a_expired > @now
        ORDER BY a.a_created DESC
      `);

    const row = result.recordset?.[0];

    if (!row) {
      return res.status(401).json({ message: "Token expired or invalid, please login again" });
    }

    if (row.u_active !== 1) {
      return res.status(403).json({ message: "User is inactive" });
    }

    // ถ้า token มี station -> ต้องมีใน op_station และ active=1
    const tokenOpStaId = row.a_op_sta_id ? String(row.a_op_sta_id).trim() : null;
    if (tokenOpStaId) {
      // ถ้า LEFT JOIN ไม่เจอ station จะได้ค่า null
      if (!row.op_sta_code && !row.op_sta_name) {
        return res.status(403).json({ message: "Station in token not found" });
      }
      if (row.op_sta_active !== 1) {
        return res.status(403).json({ message: "Station is inactive" });
      }
    }

    const requestClientType = normalizeClientType(
      req.get("x-client-type") || req.headers["x-client-type"]
    );

    req.user = {
      u_id: String(row.u_id),
      u_username: row.u_username,
      u_name: row.u_name,
      u_type: row.u_type,
      role: mapUserTypeToRole(row.u_type),

      clientType: normalizeClientType(row.a_client_type),
      requestClientType,

      tokenExpiresAt: row.a_expired,

      // ✅ station remembered by token
      op_sta_id: tokenOpStaId,
      station: tokenOpStaId
        ? {
            op_sta_id: tokenOpStaId,
            op_sta_code: row.op_sta_code,
            op_sta_name: row.op_sta_name,
          }
        : null,
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

function requireClientType(allowed = ["PC", "HH"]) {
  return (req, res, next) => {
    const reqType = normalizeClientType(req.get("x-client-type") || req.headers["x-client-type"]);
    if (reqType === "UNKNOWN") {
      return res.status(400).json({ message: "Missing or invalid x-client-type" });
    }

    if (!allowed.includes(reqType)) {
      return res.status(403).json({ message: "Client type is not allowed" });
    }

    const tokenType = req.user?.clientType ? normalizeClientType(req.user.clientType) : null;
    if (tokenType && tokenType !== "UNKNOWN" && tokenType !== reqType) {
      return res.status(403).json({ message: "Token client type mismatch" });
    }

    next();
  };
}

module.exports = { requireAuth, requireRole, requireClientType, normalizeClientType };