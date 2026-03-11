const bcrypt = require("bcryptjs");
const crypto = require("crypto");

const { getPool } = require("../config/db");

// map u_type -> role (no Utils folder)
function mapUserTypeToRole(u_type) {
  if (u_type === "op") return "operator";
  if (u_type === "ad") return "admin";
  if (u_type === "ma") return "manager";
  return "unknown";
}

// convert "10s" / "30m" / "24h" / "7d" -> milliseconds
function durationToMs(text) {
  const s = String(text || "24h").trim().toLowerCase();
  const m = s.match(/^(\d+)\s*([smhd])$/);
  if (!m) return 24 * 60 * 60 * 1000;
  const n = parseInt(m[1], 10);
  const unit = m[2];
  if (unit === "s") return n * 1000;
  if (unit === "m") return n * 60 * 1000;
  if (unit === "h") return n * 60 * 60 * 1000;
  if (unit === "d") return n * 24 * 60 * 60 * 1000;
  return 24 * 60 * 60 * 1000;
}

function normalizeClientType(raw) {
  const v = String(raw || "").trim().toUpperCase();
  if (v === "REACT" || v === "PC") return "PC";
  if (v === "FLUTTER" || v === "HH") return "HH";
  return "UNKNOWN";
}

async function login(req, res) {
  const username   = String(req.body.username   || "").trim();
  const password   = String(req.body.password   || "").trim();
  const op_sta_id  = String(req.body.op_sta_id  || "").trim();

  const clientTypeRaw = req.get("x-client-type") || req.headers["x-client-type"] || "UNKNOWN";
  const safeClientType = normalizeClientType(clientTypeRaw);

  if (!username) {
    return res.status(400).json({ message: "กรุณากรอก username" });
  }

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "เชื่อมต่อฐานข้อมูลไม่สำเร็จ" });

    // 1) find user
    const [userRows] = await pool.query(
      `SELECT u_id, u_username, u_password, u_type, u_active, u_name
       FROM \`user\`
       WHERE u_username = ?
       LIMIT 1`,
      [username]
    );

    const user = userRows[0];
    if (!user) return res.status(404).json({ message: "ไม่มีผู้ใช้นี้อยู่ในระบบ" });
    if (user.u_active !== 1) return res.status(403).json({ message: "บัญชีผู้ใช้ถูกปิดใช้งาน" });

    const role = mapUserTypeToRole(user.u_type);

    // 2) Decide login mode
    const isOperatorHH = safeClientType === "HH" && user.u_type === "op";

    // 3) Validate credentials
    if (!isOperatorHH) {
      if (!password) return res.status(400).json({ message: "กรุณากรอก password" });

      const mode = String(process.env.PASSWORD_MODE || "bcrypt").toLowerCase();
      let passOk = false;

      if (mode === "plain") {
        passOk = password === String(user.u_password || "");
      } else {
        passOk = await bcrypt.compare(password, String(user.u_password || ""));
      }

      if (!passOk) return res.status(401).json({ message: "รหัสผ่านไม่ถูกต้อง" });
    } else {
      if (!op_sta_id) return res.status(400).json({ message: "กรุณาเลือก station (op_sta_id)" });
    }

    // 4) If operator/HH -> lookup station name + active
    let op_sta_name = null;

    if (isOperatorHH) {
      const [staRows] = await pool.query(
        `SELECT op_sta_id, op_sta_name, CAST(op_sta_active AS UNSIGNED) AS op_sta_active
         FROM op_station
         WHERE op_sta_id = ?
         LIMIT 1`,
        [op_sta_id]
      );

      const sta = staRows[0];
      if (!sta) return res.status(400).json({ message: "ไม่พบ station" });
      if (Number(sta.op_sta_active) !== 1) return res.status(400).json({ message: "station ถูกปิดใช้งาน" });

      op_sta_name = sta.op_sta_name || null;
    }

    // 5) Token
    const now = new Date();
    const expiresAt = new Date(now);
    expiresAt.setHours(24, 0, 0, 0);

    const tokenAccess = crypto.randomBytes(48).toString("base64url");
    let tokenId = null;

    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
      const y      = now.getFullYear();
      const m      = String(now.getMonth() + 1).padStart(2, "0");
      const d      = String(now.getDate()).padStart(2, "0");
      const prefix = `AC${y}${m}${d}`;

      const [lastRows] = await conn.query(
        `SELECT a_id FROM \`access\`
         WHERE a_id LIKE ?
         ORDER BY a_id DESC
         LIMIT 1`,
        [`${prefix}%`]
      );

      let running = 1;
      if (lastRows.length > 0) {
        const lastId = String(lastRows[0].a_id || "");
        const tail   = lastId.slice(prefix.length);
        const n      = parseInt(tail, 10);
        if (!Number.isNaN(n)) running = n + 1;
      }

      const suffix = String(running).padStart(4, "0");
      const a_id   = `${prefix}${suffix}`;
      tokenId      = a_id;

      const [existsRows] = await conn.query(
        `SELECT a_id FROM \`access\` WHERE a_token = ? LIMIT 1`,
        [tokenAccess]
      );

      if (existsRows.length > 0) {
        await conn.rollback();
        conn.release();
        return res.status(500).json({ message: "ระบบสร้าง token ซ้ำ กรุณาลองใหม่อีกครั้ง" });
      }

      // ✅ store a_op_sta_id (NULL for non-operator or non-HH)
      await conn.query(
        `INSERT INTO \`access\` (a_id, u_id, a_token, a_created, a_expired, a_client_type, a_op_sta_id)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [a_id, String(user.u_id), tokenAccess, now, expiresAt, safeClientType, isOperatorHH ? op_sta_id : null]
      );

      await conn.commit();
      conn.release();
    } catch (e) {
      await conn.rollback();
      conn.release();
      throw e;
    }

    console.log(
      `[LOGIN] u_id=${user.u_id} u_name=${user.u_name} u_username=${user.u_username} clientType=${safeClientType} tokenId=${tokenId} expiresAt=${expiresAt.toISOString()} op_sta_id=${isOperatorHH ? op_sta_id : "-"}`
    );

    // ✅ response: add op_sta_name
    return res.json({
      tokenAccess,
      clientType:     safeClientType,
      tokenExpiresAt: expiresAt.toISOString(),
      tokenId,
      userInfo: {
        u_id:        String(user.u_id),
        u_name:      user.u_name,
        u_type:      user.u_type,
        role,
        op_sta_id:   isOperatorHH ? op_sta_id   : null,
        op_sta_name: isOperatorHH ? op_sta_name : null,
      },
    });
  } catch (err) {
    console.error("LOGIN ERROR:", err);
    return res.status(500).json({ message: "เกิดข้อผิดพลาดของเซิร์ฟเวอร์" });
  }
}

// POST /api/auth/logout
async function logout(req, res) {
  const auth  = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";

  try {
    const pool = getPool();
    if (!pool) {
      console.log("[LOGOUT] db connection failed");
      return res.status(500).json({ ok: false, message: "เชื่อมต่อฐานข้อมูลไม่สำเร็จ" });
    }

    if (!token) {
      console.log("[LOGOUT] missing token -> ok");
      return res.status(200).json({ ok: true, message: "ออกจากระบบสำเร็จ", expiredRows: 0 });
    }

    const now = new Date();

    const [result] = await pool.query(
      `UPDATE \`access\`
       SET a_expired = ?
       WHERE a_token = ?
         AND a_expired > ?`,
      [now, token, now]
    );

    const expiredRows = result.affectedRows || 0;
    console.log(`[LOGOUT] token=${token.slice(0, 8)}... expiredRows=${expiredRows}`);

    return res.status(200).json({ ok: true, message: "ออกจากระบบสำเร็จ", expiredRows });
  } catch (err) {
    console.log("[LOGOUT] error:", err);
    return res.status(500).json({ ok: false, message: "เกิดข้อผิดพลาดของเซิร์ฟเวอร์" });
  }
}

async function createUser(req, res) {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();
  const name     = String(req.body.name     || "").trim();
  const u_type   = String(req.body.u_type   || "").trim();
  const active   = req.body.active === undefined ? 1 : Number(req.body.active);

  if (!username || !password || !name || !u_type) {
    return res.status(400).json({ message: "กรุณากรอก username, password, name, u_type" });
  }

  if (!["op", "ad", "ma"].includes(u_type)) {
    return res.status(400).json({ message: "u_type ต้องเป็น op, ad, ma เท่านั้น" });
  }

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "เชื่อมต่อฐานข้อมูลไม่สำเร็จ" });

    const [dupRows] = await pool.query(
      `SELECT u_id FROM \`user\` WHERE u_username = ? LIMIT 1`,
      [username]
    );

    if (dupRows.length > 0) {
      return res.status(409).json({ message: "username นี้มีอยู่แล้ว" });
    }

    const hashed = await bcrypt.hash(password, 10);
    const now    = new Date();

    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
      const [maxRows] = await conn.query(
        `SELECT IFNULL(MAX(u_id), 0) + 1 AS nextId FROM \`user\``
      );
      const nextId = maxRows[0].nextId;

      await conn.query(
        `INSERT INTO \`user\` (u_id, u_username, u_password, u_name, u_type, u_active, u_created_ts, u_updated_ts)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [nextId, username, hashed, name, u_type, active, now, now]
      );

      await conn.commit();
      conn.release();

      return res.status(201).json({
        message: "สร้างผู้ใช้สำเร็จ",
        user: {
          u_id:      nextId,
          u_username: username,
          password:  "hash password",
          u_name:    name,
          u_type,
          u_active:  active,
        },
      });
    } catch (e) {
      await conn.rollback();
      conn.release();
      throw e;
    }
  } catch (err) {
    console.error("CREATE USER ERROR:", err);
    return res.status(500).json({ message: "เกิดข้อผิดพลาดของเซิร์ฟเวอร์" });
  }
}

// ✅ [แก้ไข] getAllUsers: รองรับ query alias
// - type   -> u_type   (op/ad/ma)
// - active -> u_active (0/1)
// และยังรองรับ u_type/u_active แบบเดิมด้วย
async function getAllUsers(req, res) {
  const q = String(req.query.q || "").trim();

  // ✅ [เพิ่ม] alias: type/active (fallback ไปหา u_type/u_active)
  const type      = String(req.query.type || req.query.u_type || "").trim();
  const activeRaw = req.query.active !== undefined ? req.query.active : req.query.u_active;
  const active    = activeRaw === undefined ? null : Number(activeRaw);

  // ✅ [เพิ่ม] validate type ถ้ามีส่งมา
  if (type && !["op", "ad", "ma"].includes(type)) {
    return res.status(400).json({ message: "type ต้องเป็น op, ad, ma เท่านั้น" });
  }

  // ✅ [เพิ่ม] validate active ถ้ามีส่งมา
  if (active !== null && ![0, 1].includes(active)) {
    return res.status(400).json({ message: "active ต้องเป็น 0 หรือ 1 เท่านั้น" });
  }

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "เชื่อมต่อฐานข้อมูลไม่สำเร็จ" });

    let where  = "WHERE 1=1";
    const params = [];

    if (q) {
      where += " AND (u_username LIKE ? OR u_name LIKE ?)";
      params.push(`%${q}%`, `%${q}%`);
    }

    if (type) {
      where += " AND u_type = ?";
      params.push(type);
    }

    if (active === 0 || active === 1) {
      where += " AND u_active = ?";
      params.push(active);
    }

    const [rows] = await pool.query(
      `SELECT u_id, u_username, u_name, u_type, u_active, u_created_ts, u_updated_ts
       FROM \`user\`
       ${where}
       ORDER BY u_id ASC`,
      params
    );

    return res.json({ message: "success", users: rows });
  } catch (err) {
    console.error("GET ALL USERS ERROR:", err);
    return res.status(500).json({ message: "เกิดข้อผิดพลาดของเซิร์ฟเวอร์" });
  }
}

async function getUsersByType(req, res) {
  const u_type = String(req.params.u_type || "").trim();

  if (!["op", "ad", "ma"].includes(u_type)) {
    return res.status(400).json({ message: "u_type ต้องเป็น op, ad, ma เท่านั้น" });
  }

  try {
    const pool = getPool();
    if (!pool) return res.status(500).json({ message: "เชื่อมต่อฐานข้อมูลไม่สำเร็จ" });

    const [rows] = await pool.query(
      `SELECT u_id, u_username, u_name, u_type, u_active, u_created_ts, u_updated_ts
       FROM \`user\`
       WHERE u_type = ?
       ORDER BY u_id ASC`,
      [u_type]
    );

    return res.json({ message: "success", u_type, users: rows });
  } catch (err) {
    console.error("GET USERS BY TYPE ERROR:", err);
    return res.status(500).json({ message: "เกิดข้อผิดพลาดของเซิร์ฟเวอร์" });
  }
}

module.exports = { login, logout, createUser, getAllUsers, getUsersByType };