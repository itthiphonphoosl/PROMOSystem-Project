// controllers/color.controller.js
// ──────────────────────────────────────────
// GET  /api/colors            → list ทั้งหมด
// GET  /api/colors/:color_id  → รายการเดียว
// PUT  /api/colors/:color_id  → แก้ไข (ยกเว้น color_id)
// ──────────────────────────────────────────
const sql = require("mssql");
const { getPool } = require("../config/db");

const COLOR_TABLE = process.env.COLOR_TABLE || "dbo.color_painting";

function safeTableName(fullName) {
  const s = String(fullName || "");
  if (!/^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/.test(s))
    throw new Error(`Invalid table name: ${s}`);
  return s;
}

const SAFE_COLOR = safeTableName(COLOR_TABLE);

function actorOf(req) {
  return {
    u_id:   req.user?.u_id   ?? null,
    u_name: req.user?.u_name ?? "unknown",
    role:   req.user?.role   ?? "unknown",
  };
}

// ─────────────────────────────────────────────────────────
// GET /api/colors
// list สีทั้งหมด (default: เฉพาะ active, ?all=true เอาทั้งหมด)
// ─────────────────────────────────────────────────────────
exports.listColors = async (req, res) => {
  const actor   = actorOf(req);
  const showAll = req.query.all === "true"; // ?all=true → รวม inactive ด้วย

  try {
    const pool = await getPool();
    const r = await pool.request().query(`
      SELECT
        color_id,
        color_no,
        color_name,
        color_status,
        CASE color_status
          WHEN 1 THEN 'Active'
          WHEN 0 THEN 'Inactive'
        END AS color_status_label
      FROM ${SAFE_COLOR} WITH (NOLOCK)
      ${showAll ? "" : "WHERE color_status = 1"}
      ORDER BY color_id ASC
    `);

    return res.json({
      actor,
      show_all: showAll,
      count: r.recordset.length,
      items: r.recordset,
    });
  } catch (err) {
    console.error("[COLOR_LIST][ERROR]", err);
    return res.status(500).json({ message: "List colors failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// GET /api/colors/:color_id
// ดูสีรายการเดียวตาม color_id
// ─────────────────────────────────────────────────────────
exports.getColorById = async (req, res) => {
  const actor    = actorOf(req);
  const color_id = Number(req.params.color_id);

  if (!Number.isFinite(color_id) || color_id <= 0) {
    return res.status(400).json({ message: "color_id must be a positive number", actor });
  }

  try {
    const pool = await getPool();
    const r = await pool
      .request()
      .input("color_id", sql.Int, color_id)
      .query(`
        SELECT
          color_id,
          color_no,
          color_name,
          color_status,
          CASE color_status
            WHEN 1 THEN 'Active'
            WHEN 0 THEN 'Inactive'
          END AS color_status_label
        FROM ${SAFE_COLOR} WITH (NOLOCK)
        WHERE color_id = @color_id
      `);

    const row = r.recordset?.[0];
    if (!row) return res.status(404).json({ message: "Color not found", actor, color_id });

    return res.json({ actor, item: row });
  } catch (err) {
    console.error("[COLOR_GET][ERROR]", err);
    return res.status(500).json({ message: "Get color failed", actor, error: err.message });
  }
};

// ─────────────────────────────────────────────────────────
// PUT /api/colors/:color_id
// แก้ไขข้อมูลสี — แก้ได้ทุก field ยกเว้น color_id
// body: { color_no, color_name, color_status }
// ─────────────────────────────────────────────────────────
exports.updateColor = async (req, res) => {
  const actor    = actorOf(req);
  const color_id = Number(req.params.color_id);

  if (!Number.isFinite(color_id) || color_id <= 0) {
    return res.status(400).json({ message: "color_id must be a positive number", actor });
  }

  const { color_no, color_name, color_status } = req.body ?? {};

  // ต้องมีอย่างน้อย 1 field
  if (color_no === undefined && color_name === undefined && color_status === undefined) {
    return res.status(400).json({
      message: "ต้องระบุอย่างน้อย 1 field: color_no, color_name, color_status",
      actor,
    });
  }

  // validate color_status ถ้าส่งมา
  if (color_status !== undefined) {
    const cs = Number(color_status);
    if (cs !== 0 && cs !== 1) {
      return res.status(400).json({ message: "color_status ต้องเป็น 0 หรือ 1 เท่านั้น", actor });
    }
  }

  try {
    const pool = await getPool();

    // ตรวจว่ามี record อยู่จริง
    const checkR = await pool
      .request()
      .input("color_id", sql.Int, color_id)
      .query(`SELECT TOP 1 color_id FROM ${SAFE_COLOR} WHERE color_id = @color_id`);

    if (!checkR.recordset?.[0]) {
      return res.status(404).json({ message: "Color not found", actor, color_id });
    }

    // build SET clause เฉพาะ field ที่ส่งมา
    const setClauses = [];
    const rq = pool.request().input("color_id", sql.Int, color_id);

    if (color_no !== undefined) {
      setClauses.push("color_no = @color_no");
      rq.input("color_no", sql.VarChar(20), String(color_no).trim());
    }
    if (color_name !== undefined) {
      setClauses.push("color_name = @color_name");
      rq.input("color_name", sql.NVarChar(100), String(color_name).trim());
    }
    if (color_status !== undefined) {
      setClauses.push("color_status = @color_status");
      rq.input("color_status", sql.TinyInt, Number(color_status));
    }

    await rq.query(`
      UPDATE ${SAFE_COLOR}
      SET ${setClauses.join(", ")}
      WHERE color_id = @color_id
    `);

    // ดึง record ล่าสุดกลับมา
    const updated = await pool
      .request()
      .input("color_id", sql.Int, color_id)
      .query(`
        SELECT
          color_id, color_no, color_name, color_status,
          CASE color_status WHEN 1 THEN 'Active' WHEN 0 THEN 'Inactive' END AS color_status_label
        FROM ${SAFE_COLOR}
        WHERE color_id = @color_id
      `);

    return res.json({
      actor,
      message: "อัปเดตสีสำเร็จ",
      item: updated.recordset?.[0],
    });
  } catch (err) {
    console.error("[COLOR_UPDATE][ERROR]", err);
    return res.status(500).json({ message: "Update color failed", actor, error: err.message });
  }
};