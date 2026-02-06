const { sql, getPool } = require("../config/db");

// map u_type -> role
function mapUserTypeToRole(u_type) {
  if (u_type === "op") return "operator";
  if (u_type === "ad") return "admin";
  if (u_type === "ma") return "manager";
  return "unknown";
}

/**
 * ✅ [แก้/รวม] normalize x-client-type ให้เหลือแค่ PC / HH
 * - PC = React Web
 * - HH = Flutter Handheld
 * - รองรับค่าเก่า: REACT/FLUTTER -> PC/HH
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

    // ✅ [เพิ่ม] เก็บ client type จาก request header (เอาไว้เช็ค/ใช้งานต่อ)
    const requestClientType = normalizeClientType(
      req.get("x-client-type") || req.headers["x-client-type"]
    );

    req.user = {
      u_id: String(row.u_id),
      u_username: row.u_username,
      u_name: row.u_name,
      u_type: row.u_type,
      role: mapUserTypeToRole(row.u_type),

      // ✅ [แก้] normalize type ที่เก็บใน token ด้วย (กันข้อมูลเก่า REACT/FLUTTER)
      clientType: normalizeClientType(row.a_client_type),

      // ✅ [เพิ่ม] type ที่ส่งมากับ request นี้
      requestClientType,

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

/**
 * ✅ [แก้] requireClientType ไม่พึ่ง req.user แล้ว
 * - อ่าน x-client-type จาก header โดยตรง -> กันกรณี route ไม่ได้เรียก requireAuth ก่อน
 * - default รับได้ทั้ง PC และ HH (ตามที่ฉันสั่ง)
 * - ถ้ามี req.user (ผ่าน requireAuth แล้ว) จะเช็ค tokenType mismatch ให้ด้วย
 */
function requireClientType(allowed = ["PC", "HH"]) {
  return (req, res, next) => {
    const reqType = normalizeClientType(req.get("x-client-type") || req.headers["x-client-type"]);
    if (reqType === "UNKNOWN") {
      return res.status(400).json({ message: "Missing or invalid x-client-type" });
    }

    if (!allowed.includes(reqType)) {
      return res.status(403).json({ message: "Client type is not allowed" });
    }

    // ✅ ถ้าใช้ร่วมกับ requireAuth: กัน token ข้าม device/type
    const tokenType = req.user?.clientType ? normalizeClientType(req.user.clientType) : null;
    if (tokenType && tokenType !== "UNKNOWN" && tokenType !== reqType) {
      return res.status(403).json({ message: "Token client type mismatch" });
    }

    next();
  };
}

module.exports = { requireAuth, requireRole, requireClientType, normalizeClientType };
