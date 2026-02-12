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

/**
 * POST /api/TKDocs
 * body:
 * { "part_no": "382-B42-002D", "MC_id": "MC001" }
 */
exports.createTkDoc = async (req, res) => {
  const actor = actorOf(req);

  // route บังคับแล้ว แต่เช็คเพิ่มกันหลุด
  if (actor.role !== "admin") {
    return res.status(403).json({ message: "Forbidden (admin only)", actor });
  }

  try {
    const pool = await getPool();

    const part_no = String(req.body.part_no || "").trim();
    const MC_id = String(req.body.MC_id || "").trim();

    if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });
    if (!part_no) return res.status(400).json({ message: "part_no is required", actor });
    if (!MC_id) return res.status(400).json({ message: "MC_id is required", actor });

    // lookup part
    const partResult = await pool.request()
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

    // lookup machine -> op_sta_id
    const mcResult = await pool.request()
      .input("MC_id", sql.VarChar(10), MC_id)
      .query(`
        SELECT TOP 1 MC_id, op_sta_id, MC_active, MC_name
        FROM dbo.machine
        WHERE MC_id = @MC_id
      `);

    const mcRow = mcResult.recordset?.[0];
    if (!mcRow) {
      return res.status(400).json({
        message: "Create failed: MC_id not found",
        actor,
        error: `MC_id not found: ${MC_id}`,
      });
    }

    const op_sta_id = mcRow.op_sta_id ? String(mcRow.op_sta_id).trim() : "";
    if (!op_sta_id) {
      return res.status(400).json({
        message: "Create failed: machine has no op_sta_id",
        actor,
        error: `machine ${MC_id} has op_sta_id = NULL`,
      });
    }

    // validate station exists
    const staCheck = await pool.request()
      .input("id", sql.VarChar(20), op_sta_id)
      .query(`SELECT TOP 1 op_sta_id FROM dbo.op_station WHERE op_sta_id = @id`);

    if (!staCheck.recordset?.[0]) {
      return res.status(400).json({
        message: "Create failed: op_sta_id not found in op_station",
        actor,
        error: `op_sta_id not found: ${op_sta_id}`,
      });
    }

    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      const now = new Date();

      // 1) gen tk_id
      const tk_id = await genTkId(tx, now);

      // 2) insert TKHead (include MC_id)
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("created_by_u_id", sql.Int, Number(actor.u_id))
        .input("tk_status", sql.Int, 0)
        .input("tk_created_at_ts", sql.DateTime2(3), now)
        .input("MC_id", sql.VarChar(10), MC_id)
        .query(`
          INSERT INTO ${SAFE_TKHEAD}
            (tk_id, tk_created_at_ts, created_by_u_id, tk_status, MC_id)
          VALUES
            (@tk_id, @tk_created_at_ts, @created_by_u_id, @tk_status, @MC_id)
        `);

      // 3) DB generates run_no + lot_no and inserts TKRunLog
      const spResult = await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("part_id", sql.Int, Number(partRow.part_id)) // ถ้า part_id เป็น BIGINT ค่อยปรับเป็น sql.BigInt
        .input("created_by_u_id", sql.Int, Number(actor.u_id))
        .output("run_no", sql.Char(14))
        .output("lot_no", sql.NVarChar(300))
        .execute("dbo.usp_TKRunLog_Create");

      const run_no = spResult.output.run_no;
      const lot_no = spResult.output.lot_no;

      if (!run_no || !lot_no) {
        throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");
      }

      // 4) insert TKDetail (new schema)
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), tk_id)
        .input("MC_id", sql.VarChar(10), MC_id)
        .input("op_sta_id", sql.VarChar(20), op_sta_id)
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

      // ✅ LOG ฟีลเดียวกับ LOGIN (เฉพาะจำเป็น)
      console.log(
        `[TKDOC_CREATE] tk_id=${tk_id} part_id=${partRow.part_id} part_no=${partRow.part_no} MC_id=${MC_id} op_sta_id=${op_sta_id} run_no=${run_no} created_by_u_id=${actor.u_id} createdAt=${now.toISOString()}`
      );

      // ✅ RESPONSE แบบ minimal
      return res.status(201).json({
        message: "Created TK document",
        id: tk_id,
        run_no,
        lot_no,
        MC_id,
        op_sta_id,
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

// เผื่อ route import ชื่อ createTkDocument
exports.createTkDocument = exports.createTkDoc;
exports.listTkDocs = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool = await getPool();

    const r = await pool.request().query(`
      SELECT
        h.tk_id                              AS id,
        h.tk_created_at_ts                   AS created_at,
        h.created_by_u_id                    AS created_by_u_id,
        h.tk_status                          AS head_status,
        h.MC_id                              AS head_MC_id,

        d.MC_id                              AS detail_MC_id,
        d.op_sta_id                          AS op_sta_id,
        d.part_id                            AS part_id,
        p.part_no                            AS part_no,
        p.part_name                          AS part_name,
        d.lot_no                             AS lot_no,
        d.tk_status                          AS detail_status,
        d.tk_created_at_ts                   AS detail_created_at,

        m.MC_name                            AS MC_name,

        rl.run_no                            AS run_no
      FROM ${SAFE_TKHEAD} h
      LEFT JOIN ${SAFE_TKDETAIL} d
        ON d.tk_id = h.tk_id
       AND d.tk_parent_id = h.tk_id
      LEFT JOIN dbo.part p
        ON p.part_id = d.part_id
      LEFT JOIN dbo.machine m
        ON m.MC_id = COALESCE(d.MC_id, h.MC_id)
      OUTER APPLY (
        SELECT TOP 1 run_no
        FROM dbo.TKRunLog
        WHERE tk_id = h.tk_id
        ORDER BY created_at_ts DESC
      ) rl
      ORDER BY h.tk_created_at_ts DESC;
    `);

    // ทำ response ให้ "minimal/เหมือน create"
    const items = (r.recordset || []).map((x) => ({
      id: x.id,
      run_no: x.run_no ? String(x.run_no).trim() : null,
      lot_no: x.lot_no || null,
      MC_id: x.detail_MC_id || x.head_MC_id || null,
      op_sta_id: x.op_sta_id || null,
      part_no: x.part_no || null,
      part_name: x.part_name || null,
      created_at: x.created_at ? new Date(x.created_at).toISOString() : null,
      tk_status: x.head_status,
    }));

    return res.json({ actor, items });
  } catch (err) {
    console.error("[TKDOC_LIST][ERROR]", err);
    return res.status(500).json({
      message: "List failed",
      actor,
      error: err.message,
    });
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