/**
 * controllers/auth.controller.js
 * ✅ เวอร์ชันเต็มไฟล์ (ตามโครงเดิม)
 * ✅ ใส่คอมเมนต์กำกับว่า "เพิ่มอะไร/ตรงไหน" แล้ว
 */

const bcrypt = require("bcryptjs");
const crypto = require("crypto");

const { sql, getPool } = require("../config/db");

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
  const m = s.match(/^(\d+)\s*([smhd])$/); // 10s / 30m / 24h / 7d
  if (!m) return 24 * 60 * 60 * 1000; // fallback 24h
  const n = parseInt(m[1], 10);
  const unit = m[2];
  if (unit === "s") return n * 1000;
  if (unit === "m") return n * 60 * 1000;
  if (unit === "h") return n * 60 * 60 * 1000;
  if (unit === "d") return n * 24 * 60 * 60 * 1000;
  return 24 * 60 * 60 * 1000;
}

/**
 * ✅ [เพิ่มใหม่] normalize client type header (legacy/framework -> device type)
 * - REACT  -> PC
 * - FLUTTER-> HH
 * - also accept new values directly: PC / HH
 */
function normalizeClientType(raw) {
  const v = String(raw || "").trim().toUpperCase();
  if (v === "REACT" || v === "PC") return "PC";
  if (v === "FLUTTER" || v === "HH") return "HH";
  return "UNKNOWN";
}

async function login(req, res) {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();

  const clientTypeRaw =
    req.get("x-client-type") || req.headers["x-client-type"] || "UNKNOWN";

  /**
   * ✅ [แก้/เพิ่ม] store/send เป็น: PC | HH | UNKNOWN
   * เดิมเช็ค ["REACT","FLUTTER"] ตอนนี้เปลี่ยนเป็น normalizeClientType()
   */
  const safeClientType = normalizeClientType(clientTypeRaw);

  if (!username || !password) {
    return res.status(400).json({ message: "กรุณากรอก username และ password" });
  }

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "เชื่อมต่อฐานข้อมูลไม่สำเร็จ" });

    const result = await pool
      .request()
      .input("username", sql.VarChar(50), username)
      .query(`
        SELECT TOP 1
          u_id, u_username, u_password, u_type, u_active, u_name
        FROM [user]
        WHERE u_username = @username
      `);

    const user = result.recordset[0];

    if (!user) {
      return res.status(404).json({ message: "ไม่มีผู้ใช้นี้อยู่ในระบบ" });
    }

    if (user.u_active !== 1) {
      return res.status(403).json({ message: "บัญชีผู้ใช้ถูกปิดใช้งาน" });
    }

    const mode = String(process.env.PASSWORD_MODE || "bcrypt").toLowerCase();
    let passOk = false;

    if (mode === "plain") {
      passOk = password === String(user.u_password || "");
    } else {
      passOk = await bcrypt.compare(password, String(user.u_password || ""));
    }

    if (!passOk) {
      return res.status(401).json({ message: "รหัสผ่านไม่ถูกต้อง" });
    }

    const role = mapUserTypeToRole(user.u_type);

    const now = new Date();

    // expire when day changes (next day 00:00:00.000) based on server time
    const expiresAt = new Date(now);
    expiresAt.setHours(24, 0, 0, 0);

    const tokenAccess = crypto.randomBytes(48).toString("base64url");

    /**
     * ✅ [แก้] tokenId ให้ใช้ a_id (จะ set หลังจาก gen a_id สำเร็จ)
     * เดิม: const tokenId = crypto.randomUUID();
     */
    let tokenId = null;

    const tx = new sql.Transaction(pool);
    await tx.begin();

    try {
      const y = now.getFullYear();
      const m = String(now.getMonth() + 1).padStart(2, "0");
      const d = String(now.getDate()).padStart(2, "0");
      const prefix = `AC${y}${m}${d}`;

      const last = await new sql.Request(tx)
        .input("likePrefix", sql.VarChar(30), `${prefix}%`)
        .query(`
          SELECT TOP 1 a_id
          FROM access WITH (UPDLOCK, HOLDLOCK)
          WHERE a_id LIKE @likePrefix
          ORDER BY a_id DESC
        `);

      let running = 1;
      if (last.recordset.length > 0) {
        const lastId = String(last.recordset[0].a_id);
        const tail = lastId.slice(prefix.length);
        const n = parseInt(tail, 10);
        if (!Number.isNaN(n)) running = n + 1;
      }

      const suffix = String(running).padStart(4, "0");
      const a_id = `${prefix}${suffix}`;

      /**
       * ✅ [เพิ่ม] ให้ tokenId = a_id ตามที่ต้องการ
       */
      tokenId = a_id;

      const exists = await new sql.Request(tx)
        .input("a_token", sql.NVarChar(255), tokenAccess)
        .query(`SELECT TOP 1 a_id FROM access WHERE a_token = @a_token`);

      if (exists.recordset.length > 0) {
        await tx.rollback();
        return res.status(500).json({ message: "ระบบสร้าง token ซ้ำ กรุณาลองใหม่อีกครั้ง" });
      }

      await new sql.Request(tx)
        .input("a_id", sql.VarChar(20), a_id)
        .input("u_id", sql.VarChar(20), String(user.u_id))
        .input("a_token", sql.NVarChar(255), tokenAccess)
        .input("a_created", sql.DateTime2(3), now)
        .input("a_expired", sql.DateTime2(3), expiresAt)
        /**
         * ✅ [แก้/เพิ่ม] เก็บ a_client_type เป็น PC | HH | UNKNOWN
         */
        .input("a_client_type", sql.VarChar(20), safeClientType)
        .query(`
          INSERT INTO access (a_id, u_id, a_token, a_created, a_expired, a_client_type)
          VALUES (@a_id, @u_id, @a_token, @a_created, @a_expired, @a_client_type)
        `);

      await tx.commit();
    } catch (e) {
      await tx.rollback();
      throw e;
    }

    console.log(
      `[LOGIN] u_id=${user.u_id} u_name=${user.u_name} u_username=${user.u_username} clientType=${safeClientType} tokenId=${tokenId} expiresAt=${expiresAt.toISOString()}`
    );

    return res.json({
      tokenAccess,
      /**
       * ✅ [แก้/เพิ่ม] ส่ง clientType เป็น PC | HH | UNKNOWN
       */
      clientType: safeClientType,
      tokenExpiresAt: expiresAt.toISOString(),
      tokenId, // ✅ ตอนนี้ = a_id ในตาราง access แล้ว
      userInfo: {
        u_id: String(user.u_id),
        u_name: user.u_name,
        u_type: user.u_type,
        role,
      },
    });
  } catch (err) {
    console.error("LOGIN ERROR:", err);
    return res.status(500).json({ message: "เกิดข้อผิดพลาดของเซิร์ฟเวอร์" });
  }
}

// POST /api/auth/logout
async function logout(req, res) {
  const auth = req.headers.authorization || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";

  try {
    const pool = await getPool();
    if (!pool) {
      console.log("[LOGOUT] db connection failed");
      return res.status(500).json({ ok: false, message: "เชื่อมต่อฐานข้อมูลไม่สำเร็จ" });
    }

    // ไม่มี token ก็ให้ถือว่า logout สำเร็จ
    if (!token) {
      console.log("[LOGOUT] missing token -> ok");
      return res.status(200).json({ ok: true, message: "ออกจากระบบสำเร็จ", expiredRows: 0 });
    }

    const now = new Date();

    const result = await pool
      .request()
      .input("a_token", sql.NVarChar(255), token)
      .input("now", sql.DateTime2(3), now)
      .query(`
        UPDATE access
        SET a_expired = @now
        WHERE a_token = @a_token
          AND a_expired > @now
      `);

    const expiredRows = result.rowsAffected?.[0] || 0;

    console.log(`[LOGOUT] token=${token.slice(0, 8)}... expiredRows=${expiredRows}`);

    return res.status(200).json({
      ok: true,
      message: "ออกจากระบบสำเร็จ",
      expiredRows,
    });
  } catch (err) {
    console.log("[LOGOUT] error:", err);
    return res.status(500).json({ ok: false, message: "เกิดข้อผิดพลาดของเซิร์ฟเวอร์" });
  }
}

async function createUser(req, res) {
  const username = String(req.body.username || "").trim();
  const password = String(req.body.password || "").trim();
  const name = String(req.body.name || "").trim();
  const u_type = String(req.body.u_type || "").trim();
  const active = req.body.active === undefined ? 1 : Number(req.body.active);

  if (!username || !password || !name || !u_type) {
    return res.status(400).json({ message: "กรุณากรอก username, password, name, u_type" });
  }

  if (!["op", "ad", "ma"].includes(u_type)) {
    return res.status(400).json({ message: "u_type ต้องเป็น op, ad, ma เท่านั้น" });
  }

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "เชื่อมต่อฐานข้อมูลไม่สำเร็จ" });

    const dup = await pool
      .request()
      .input("username", sql.VarChar(50), username)
      .query(`SELECT TOP 1 u_id FROM [user] WHERE u_username = @username`);

    if (dup.recordset.length > 0) {
      return res.status(409).json({ message: "username นี้มีอยู่แล้ว" });
    }

    const hashed = await bcrypt.hash(password, 10);
    const now = new Date();

    const tx = new sql.Transaction(pool);
    await tx.begin();

    try {
      const nextIdResult = await new sql.Request(tx).query(`
        SELECT ISNULL(MAX(u_id), 0) + 1 AS nextId
        FROM [user] WITH (UPDLOCK, HOLDLOCK)
      `);

      const nextId = nextIdResult.recordset[0].nextId;

      await new sql.Request(tx)
        .input("u_id", sql.Int, nextId)
        .input("u_username", sql.VarChar(50), username)
        .input("u_password", sql.VarChar(255), hashed) // hash stored in DB
        .input("u_name", sql.NVarChar(255), name)
        .input("u_type", sql.VarChar(2), u_type)
        .input("u_active", sql.Int, active)
        .input("u_created_ts", sql.DateTime2(3), now)
        .input("u_updated_ts", sql.DateTime2(3), now)
        .query(`
          INSERT INTO [user]
            (u_id, u_username, u_password, u_name, u_type, u_active, u_created_ts, u_updated_ts)
          VALUES
            (@u_id, @u_username, @u_password, @u_name, @u_type, @u_active, @u_created_ts, @u_updated_ts)
        `);

      await tx.commit();

      return res.status(201).json({
        message: "สร้างผู้ใช้สำเร็จ",
        user: {
          u_id: nextId,
          u_username: username,
          password: "hash password",
          u_name: name,
          u_type,
          u_active: active,
        },
      });
    } catch (e) {
      await tx.rollback();
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
  const q = String(req.query.q || "").trim(); // search u_username/u_name

  // ✅ [เพิ่ม] alias: type/active (fallback ไปหา u_type/u_active)
  const type = String(req.query.type || req.query.u_type || "").trim(); // op/ad/ma

  const activeRaw = req.query.active !== undefined ? req.query.active : req.query.u_active;
  const active = activeRaw === undefined ? null : Number(activeRaw); // 0/1

  // ✅ [เพิ่ม] validate type ถ้ามีส่งมา
  if (type && !["op", "ad", "ma"].includes(type)) {
    return res.status(400).json({ message: "type ต้องเป็น op, ad, ma เท่านั้น" });
  }

  // ✅ [เพิ่ม] validate active ถ้ามีส่งมา
  if (active !== null && ![0, 1].includes(active)) {
    return res.status(400).json({ message: "active ต้องเป็น 0 หรือ 1 เท่านั้น" });
  }

  try {
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "เชื่อมต่อฐานข้อมูลไม่สำเร็จ" });

    let where = "WHERE 1=1";
    const r = pool.request();

    if (q) {
      where += " AND (u_username LIKE @q OR u_name LIKE @q)";
      r.input("q", sql.NVarChar(255), `%${q}%`);
    }

    if (type) {
      where += " AND u_type = @u_type";
      r.input("u_type", sql.VarChar(2), type);
    }

    if (active === 0 || active === 1) {
      where += " AND u_active = @u_active";
      r.input("u_active", sql.Int, active);
    }

    const result = await r.query(`
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
    `);

    return res.json({ message: "success", users: result.recordset });
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
    const pool = await getPool();
    if (!pool) return res.status(500).json({ message: "เชื่อมต่อฐานข้อมูลไม่สำเร็จ" });

    const result = await pool
      .request()
      .input("u_type", sql.VarChar(2), u_type)
      .query(`
        SELECT
          u_id,
          u_username,
          u_name,
          u_type,
          u_active,
          u_created_ts,
          u_updated_ts
        FROM [user]
        WHERE u_type = @u_type
        ORDER BY u_id ASC
      `);

    return res.json({
      message: "success",
      u_type,
      users: result.recordset,
    });
  } catch (err) {
    console.error("GET USERS BY TYPE ERROR:", err);
    return res.status(500).json({ message: "เกิดข้อผิดพลาดของเซิร์ฟเวอร์" });
  }
}

module.exports = { login, logout, createUser, getAllUsers, getUsersByType };
