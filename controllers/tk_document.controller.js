// controllers/tk_document.controller.js
const sql = require("mssql");
const { getPool } = require("../config/db");

const TKHEAD_TABLE = process.env.TKHEAD_TABLE || "dbo.TKHead";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";

function pad(n, len) {
  return String(n).padStart(len, "0");
}
function yymmdd(d) {
  const yy = String(d.getFullYear()).slice(-2);
  const mm = pad(d.getMonth() + 1, 2);
  const dd = pad(d.getDate(), 2);
  return `${yy}${mm}${dd}`;
}
function actorOf(req) {
  return {
    u_id: req.user?.u_id ?? null,
    u_name: req.user?.u_name ?? "unknown",
    role: req.user?.role ?? "unknown",
  };
}

function safeTableName(fullName) {
  const s = String(fullName || "");
  if (!/^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$/.test(s)) {
    throw new Error(`Invalid table name: ${s}`);
  }
  return s;
}

const SAFE_TKHEAD = safeTableName(TKHEAD_TABLE);
const SAFE_TKDETAIL = safeTableName(TKDETAIL_TABLE);

// gen tk_id: TK + YYMMDD + ####
async function genTkId(tx, docDate) {
  const prefix = `TK${yymmdd(docDate)}`;

  const r = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(30), `${prefix}%`)
    .query(`
      SELECT TOP 1 tk_id
      FROM ${SAFE_TKHEAD} WITH (UPDLOCK, HOLDLOCK)
      WHERE tk_id LIKE @likePrefix
      ORDER BY tk_id DESC
    `);

  let running = 1;
  if (r.recordset.length > 0) {
    const lastId = String(r.recordset[0].tk_id || "");
    const tail = lastId.slice(prefix.length); // ####
    const n = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }

  return `${prefix}${pad(running, 4)}`;
}

exports.createTkDoc = async (req, res) => {
  const actor = actorOf(req);

  // route บังคับแล้ว แต่เช็คเพิ่มกันหลุด
  if (actor.role !== "admin") {
    return res.status(403).json({ message: "Forbidden (admin only)", actor });
  }

  try {
    const pool = await getPool();

    const part_no = String(req.body.part_no || "").trim();

    if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });
    if (!part_no) return res.status(400).json({ message: "part_no is required", actor });

    // lookup part
    const partResult = await pool
      .request()
      .input("part_no", sql.VarChar(50), part_no)
      .query(`
        SELECT TOP 1 part_id, part_no, part_name
        FROM dbo.part
        WHERE part_no = @part_no
      `);

    const partRow = partResult.recordset?.[0];
    if (!partRow) {
      return res.status(400).json({
        message: "Create failed: part_no not found",
        actor,
        error: `part_no not found: ${part_no}`,
      });
    }

    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      const now = new Date();

      // 1) gen tk_id
      const tk_id = await genTkId(tx, now);

      // 2) insert TKHead (MC_id = NULL ตาม flow ใหม่)
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("created_by_u_id", sql.Int, Number(actor.u_id))
        .input("tk_status", sql.Int, 0)
        .input("tk_created_at_ts", sql.DateTime2(3), now)
        .input("MC_id", sql.VarChar(10), null)
        .query(`
          INSERT INTO ${SAFE_TKHEAD}
            (tk_id, tk_created_at_ts, created_by_u_id, tk_status, MC_id)
          VALUES
            (@tk_id, @tk_created_at_ts, @created_by_u_id, @tk_status, @MC_id)
        `);

      // 3) DB generates run_no + lot_no and inserts TKRunLog
      const spResult = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("part_id", sql.Int, Number(partRow.part_id))
        .input("created_by_u_id", sql.Int, Number(actor.u_id))
        .output("run_no", sql.Char(14))
        .output("lot_no", sql.NVarChar(300))
        .execute("dbo.usp_TKRunLog_Create");

      const run_no = spResult.output.run_no;
      const lot_no = spResult.output.lot_no;

      if (!run_no || !lot_no) {
        throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");
      }

      // 4) insert TKDetail (MC_id/op_sta_id = NULL ตาม flow ใหม่)
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("MC_id", sql.VarChar(10), null)
        .input("op_sta_id", sql.VarChar(20), null)
        .input("tk_parent_id", sql.VarChar(20), tk_id)
        .input("u_id", sql.Int, Number(actor.u_id))
        .input("part_id", sql.Int, Number(partRow.part_id))
        .input("lot_no", sql.NVarChar(300), lot_no)
        .input("tk_status", sql.Int, 0)
        .input("tk_created_at_ts", sql.DateTime2(3), now)
        .query(`
          INSERT INTO ${SAFE_TKDETAIL}
            (tk_id, MC_id, op_sta_id, tk_parent_id, u_id, part_id, lot_no, tk_status, tk_created_at_ts)
          VALUES
            (@tk_id, @MC_id, @op_sta_id, @tk_parent_id, @u_id, @part_id, @lot_no, @tk_status, @tk_created_at_ts)
        `);

      await tx.commit();

      console.log(
        `[TKDOC_CREATE] tk_id=${tk_id} part_id=${partRow.part_id} part_no=${partRow.part_no} created_by_u_id=${actor.u_id} createdAt=${now.toISOString()}`
      );

      // ✅ response: บังคับให้โชว์เป็น null ตามที่คุณสั่ง
      return res.status(201).json({
        message: "Created TK document",
        id: tk_id,
        run_no,
        lot_no,
        MC_id: null,
        MC_name: null,
        op_sta_id: null,
        op_sta_name: null,
        created_at: now.toISOString(),
      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("[TKDOC_CREATE][ERROR]", err);
    return res.status(500).json({
      message: "Create failed",
      error: err.message,
    });
  }
};

exports.createTkDocument = exports.createTkDoc;

exports.listTkDocs = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool = await getPool();

    // Use TKDetail as the main source (field names match your table)
    // Only take "document root tray" rows (tk_parent_id = tk_id)
    const r = await pool.request().query(`
      SELECT
        d.tk_id,
        d.MC_id,
        d.op_sta_id,
        d.tk_parent_id,
        d.u_id,
        d.part_id,
        p.part_no,
        p.part_name,
        d.lot_no,
        d.tk_status,
        d.tk_created_at_ts
      FROM ${TKDETAIL_TABLE} d
      LEFT JOIN dbo.part p ON p.part_id = d.part_id
      WHERE d.tk_parent_id = d.tk_id
      ORDER BY d.tk_created_at_ts DESC;
    `);

    // Keep shape consistent with get: id + lot_no on top, full info in detail
    const items = (r.recordset || []).map((x) => ({
      id: x.tk_id,
      lot_no: x.lot_no,
      detail: {
        tk_id: x.tk_id,
        MC_id: x.MC_id,
        op_sta_id: x.op_sta_id,
        tk_parent_id: x.tk_parent_id,
        u_id: x.u_id,
        part_id: x.part_id,
        part_no: x.part_no,
        part_name: x.part_name,
        lot_no: x.lot_no,
        tk_status: x.tk_status,
        tk_created_at_ts: x.tk_created_at_ts,
      },
    }));

    return res.json({ actor, items });
  } catch (err) {
    console.error("[TKDOC_LIST][ERROR]", err);
    return res.status(500).json({ message: "List failed", error: err.message });
  }
};



exports.getTkDocById = async (req, res) => {
  const actor = actorOf(req);
  const id = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ message: "id is required", actor });

  try {
    const pool = await getPool();

    // head (ใช้แค่เช็คว่ามีเอกสาร)
    const headR = await pool.request()
      .input("id", sql.VarChar(20), id)
      .query(`
        SELECT TOP 1 tk_id
        FROM ${SAFE_TKHEAD}
        WHERE tk_id = @id
      `);

    const head = headR.recordset?.[0];
    if (!head) return res.status(404).json({ message: "Not found", id, actor });

    // detail (first tray) — field name ให้ตรงกับตาราง TKDetail
    const detailR = await pool.request()
      .input("id", sql.VarChar(20), id)
      .query(`
        SELECT TOP 1
          d.tk_id,
          d.MC_id,
          d.op_sta_id,
          d.tk_parent_id,
          d.u_id,
          d.part_id,
          p.part_no,
          p.part_name,
          d.lot_no,
          d.tk_status,
          d.tk_created_at_ts
        FROM ${SAFE_TKDETAIL} d
        LEFT JOIN dbo.part p ON p.part_id = d.part_id
        WHERE d.tk_id = @id
          AND d.tk_parent_id = @id
        ORDER BY d.tk_created_at_ts ASC
      `);

    const detailRow = detailR.recordset?.[0] || null;

    // runlogs
    const runlogR = await pool.request()
      .input("id", sql.VarChar(20), id)
      .query(`
        SELECT
          run_no,
          lot_no,
          created_at_ts,
          created_by_u_id,
          part_id
        FROM dbo.TKRunLog
        WHERE tk_id = @id
        ORDER BY created_at_ts DESC
      `);

    const runlogs = (runlogR.recordset || []).map((x) => ({
      run_no: x.run_no ? String(x.run_no).trim() : null,
      lot_no: x.lot_no || null,
      created_at_ts: x.created_at_ts ? new Date(x.created_at_ts).toISOString() : null,
      created_by_u_id: x.created_by_u_id ?? null,
      part_id: x.part_id ?? null,
    }));

    const latestRunNo = runlogs[0]?.run_no ?? null;
    const latestLotNo = runlogs[0]?.lot_no ?? (detailRow?.lot_no ?? null);

    console.log(`[TKDOC_GET] tk_id=${id} run_no=${latestRunNo || "-"} actor_u_id=${actor.u_id ?? "-"}`);

    return res.json({
      actor,
      id,
      lot_no: latestLotNo,

      detail: detailRow
        ? {
            // ✅ ตรงกับตาราง TKDetail
            tk_id: detailRow.tk_id,
            MC_id: detailRow.MC_id,
            op_sta_id: detailRow.op_sta_id,
            tk_parent_id: detailRow.tk_parent_id,
            u_id: detailRow.u_id,
            part_id: detailRow.part_id,
            lot_no: detailRow.lot_no,
            tk_status: detailRow.tk_status,
            tk_created_at_ts: detailRow.tk_created_at_ts
              ? new Date(detailRow.tk_created_at_ts).toISOString()
              : null,

            // ✅ เพิ่มไว้โชว์ (ไม่ชนชื่อคอลัมน์)
            part_no: detailRow.part_no,
            part_name: detailRow.part_name,
            run_no: latestRunNo,
          }
        : null,

      runlogs,
    });
  } catch (err) {
    console.error("[TKDOC_GET][ERROR]", err);
    return res.status(500).json({
      message: "Get failed",
      actor,
      error: err.message,
    });
  }
};