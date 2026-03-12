// controllers/tk_document.controller.js
const { getPool } = require("../config/db");

const TKHEAD_TABLE   = process.env.TKHEAD_TABLE   || "dbo.TKHead";
const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";
const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";

function pad(n, len) { return String(n).padStart(len, "0"); }
function yymmdd(d) {
  const yy = String(d.getFullYear()).slice(-2);
  const mm = pad(d.getMonth() + 1, 2);
  const dd = pad(d.getDate(), 2);
  return `${yy}${mm}${dd}`;
}
function actorOf(req) {
  return { u_id: req.user?.u_id ?? null, u_firstname: req.user?.u_firstname ?? "", u_lastname: req.user?.u_lastname ?? "", role: req.user?.role ?? "unknown" };
}
function safeTableName(fullName) {
  const s = String(fullName || "").replace(/^[A-Za-z0-9_]+\./, "");
  if (!/^[A-Za-z0-9_]+$/.test(s)) throw new Error(`Invalid table name: ${s}`);
  return `\`${s}\``;
}

const SAFE_TKHEAD   = safeTableName(TKHEAD_TABLE);
const SAFE_TKDETAIL = safeTableName(TKDETAIL_TABLE);
const SAFE_TRANSFER = safeTableName(TRANSFER_TABLE);

// ---------------------------------------------------------------------------
// gen tk_id: TK + YYMMDD + ####
// ---------------------------------------------------------------------------
async function genTkId(conn, docDate) {
  const prefix = `TK${yymmdd(docDate)}`;
  const [rows] = await conn.query(
    `SELECT tk_id FROM ${SAFE_TKHEAD}
     WHERE tk_id LIKE ?
     ORDER BY tk_id DESC LIMIT 1`,
    [`${prefix}%`]
  );
  let running = 1;
  if (rows.length > 0) {
    const tail = String(rows[0].tk_id || "").slice(prefix.length);
    const n    = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }
  return `${prefix}${pad(running, 4)}`;
}

// ---------------------------------------------------------------------------
// assertEditable(pool, tk_id)
//
// Guard สำหรับ PUT (part_no / tk_active) และ DELETE
// เงื่อนไขที่ผ่านได้:
//   1. TKDetail.MC_id IS NULL AND TKDetail.op_sta_id IS NULL  (ยังไม่ start)
//   2. ไม่มี Transfer record เลย (COUNT = 0)
//
// หมายเหตุ: tk_active = 0 ไม่ได้ block การแก้ไข
//           admin ยังต้อง toggle กลับได้เสมอ
// ---------------------------------------------------------------------------
async function assertEditable(pool, tk_id) {
  const [detailRows] = await pool.query(
    `SELECT MC_id, op_sta_id FROM ${SAFE_TKDETAIL} WHERE tk_id = ? LIMIT 1`,
    [tk_id]
  );
  const detail = detailRows[0];

  if (detail) {
    const hasStation = detail.MC_id !== null || detail.op_sta_id !== null;
    if (hasStation) {
      return {
        ok:     false,
        reason: "เอกสารนี้มีการ Start ที่ Machine หรือ Station แล้ว ไม่สามารถแก้ไข/ลบได้",
        detail: { MC_id: detail.MC_id, op_sta_id: detail.op_sta_id },
      };
    }
  }

  const [[{ cnt }]] = await pool.query(
    `SELECT COUNT(1) AS cnt FROM ${SAFE_TRANSFER}
     WHERE from_tk_id = ? OR to_tk_id = ?`,
    [tk_id, tk_id]
  );
  if (Number(cnt) > 0) {
    return {
      ok:     false,
      reason: "เอกสารนี้มี Transfer record แล้ว ไม่สามารถแก้ไข/ลบได้",
      detail: { transfer_count: Number(cnt) },
    };
  }

  return { ok: true };
}

// ---------------------------------------------------------------------------
// POST /api/TKDocs
// ---------------------------------------------------------------------------
exports.createTkDoc = async (req, res) => {
  const actor = actorOf(req);
  if (actor.role !== "admin") {
    return res.status(403).json({ message: "Forbidden (admin only)", actor });
  }

  try {
    const pool    = getPool();
    const part_no = String(req.body.part_no || "").trim();

    if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });
    if (!part_no)    return res.status(400).json({ message: "part_no is required", actor });

    const [partRows] = await pool.query(
      `SELECT part_id, part_no, part_name FROM \`part\` WHERE part_no = ? LIMIT 1`,
      [part_no]
    );
    const partRow = partRows[0];
    if (!partRow) {
      return res.status(400).json({ message: "Create failed: part_no not found", actor, error: `part_no not found: ${part_no}` });
    }

    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
      const now   = new Date();
      const tk_id = await genTkId(conn, now);

      // tk_active = 1 (default เปิดใช้งานทันที)
      await conn.query(
        `INSERT INTO ${SAFE_TKHEAD} (tk_id, tk_created_at_ts, created_by_u_id, tk_status, tk_active)
         VALUES (?, ?, ?, ?, ?)`,
        [tk_id, now, Number(actor.u_id), 0, 1]
      );

      await conn.query(
        `CALL usp_TKRunLog_Create(?, ?, ?, @run_no, @lot_no)`,
        [tk_id, Number(partRow.part_id), Number(actor.u_id)]
      );
      const [[outRow]] = await conn.query(`SELECT @run_no AS run_no, @lot_no AS lot_no`);

      const run_no = outRow?.run_no ?? null;
      const lot_no = outRow?.lot_no ?? null;

      if (!run_no || !lot_no) {
        throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");
      }

      await conn.query(
        `INSERT INTO ${SAFE_TKDETAIL}
           (tk_id, MC_id, op_sta_id, op_sc_id, u_id, part_id, lot_no, tk_status, tk_created_at_ts)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [tk_id, null, null, null, Number(actor.u_id), Number(partRow.part_id), lot_no, 0, now]
      );

      await conn.commit();
      conn.release();

      console.log(`[TKDOC_CREATE] tk_id=${tk_id} part_no=${partRow.part_no} created_by_u_id=${actor.u_id}`);

      return res.status(201).json({
        message:     "Created TK document",
        id:          tk_id,
        tk_active:   1,
        part_id:     Number(partRow.part_id),
        part_no:     partRow.part_no,
        part_name:   partRow.part_name,
        run_no,
        lot_no,
        MC_id:       null,
        MC_name:     null,
        op_sta_id:   null,
        op_sta_name: null,
        created_at:  now.toISOString(),
      });
    } catch (e) {
      await conn.rollback();
      conn.release();
      throw e;
    }
  } catch (err) {
    console.error("[TKDOC_CREATE][ERROR]", err);
    return res.status(500).json({ message: "Create failed", error: err.message });
  }
};

exports.createTkDocument = exports.createTkDoc;

// ---------------------------------------------------------------------------
// GET /api/TKDocs
// ---------------------------------------------------------------------------
exports.listTkDocs = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool = getPool();

    const [rows] = await pool.query(
      `SELECT
         d.tk_id, d.MC_id, d.op_sta_id, d.op_sc_id, d.u_id,
         d.part_id, p.part_no, p.part_name, d.lot_no,
         h.tk_status, h.tk_active, h.tk_created_at_ts
       FROM ${SAFE_TKDETAIL} d
       LEFT JOIN ${SAFE_TKHEAD} h ON h.tk_id   = d.tk_id
       LEFT JOIN \`part\`       p ON p.part_id = d.part_id
       ORDER BY h.tk_created_at_ts DESC`
    );

    const items = rows.map((x) => ({
      id:        x.tk_id,
      lot_no:    x.lot_no,
      tk_active: x.tk_active,
      detail: {
        tk_id: x.tk_id, MC_id: x.MC_id, op_sta_id: x.op_sta_id, op_sc_id: x.op_sc_id,
        u_id: x.u_id, part_id: x.part_id, part_no: x.part_no, part_name: x.part_name,
        lot_no: x.lot_no, tk_status: x.tk_status, tk_active: x.tk_active,
        tk_created_at_ts: x.tk_created_at_ts,
      },
    }));

    return res.json({ actor, total: items.length, items });
  } catch (err) {
    console.error("[TKDOC_LIST][ERROR]", err);
    return res.status(500).json({ message: "List failed", error: err.message });
  }
};

// ---------------------------------------------------------------------------
// GET /api/TKDocs/:id
//
// - admin    : เห็นได้เสมอ ไม่ว่า tk_active จะเป็นอะไร
// - operator : ถ้า tk_active = 0 → 403
// ---------------------------------------------------------------------------
exports.getTkDocById = async (req, res) => {
  const actor = actorOf(req);
  const id    = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ message: "id is required", actor });

  try {
    const pool = getPool();

    const [headRows] = await pool.query(
      `SELECT tk_id, tk_status, tk_active, created_by_u_id, tk_created_at_ts
       FROM ${SAFE_TKHEAD} WHERE tk_id = ? LIMIT 1`,
      [id]
    );
    const head = headRows[0];
    if (!head) return res.status(404).json({ message: "Not found", id, actor });

    // block operator ถ้าเอกสารถูกปิด
    if (Number(head.tk_active) !== 1 && actor.role !== "admin") {
      return res.status(403).json({
        message:   "เอกสารนี้ถูกปิดใช้งานอยู่ กรุณาติดต่อ Admin",
        tk_id:     id,
        tk_active: Number(head.tk_active),
        actor,
      });
    }

    const [detailRows] = await pool.query(
      `SELECT d.tk_id, d.MC_id, d.op_sta_id, d.op_sc_id, d.u_id,
              d.part_id, p.part_no, p.part_name, d.lot_no, d.tk_created_at_ts
       FROM ${SAFE_TKDETAIL} d
       LEFT JOIN \`part\` p ON p.part_id = d.part_id
       WHERE d.tk_id = ?
       ORDER BY d.tk_created_at_ts ASC
       LIMIT 1`,
      [id]
    );
    const detailRow = detailRows[0] || null;

    const [runlogRows] = await pool.query(
      `SELECT run_no, lot_no, created_at_ts, created_by_u_id, part_id
       FROM \`TKRunLog\` WHERE tk_id = ? ORDER BY created_at_ts DESC`,
      [id]
    );

    const runlogs = runlogRows.map((x) => ({
      run_no:          x.run_no ? String(x.run_no).trim() : null,
      lot_no:          x.lot_no || null,
      created_at_ts:   x.created_at_ts ? new Date(x.created_at_ts).toISOString() : null,
      created_by_u_id: x.created_by_u_id ?? null,
      part_id:         x.part_id ?? null,
    }));

    const latestRunNo = runlogs[0]?.run_no ?? null;
    const latestLotNo = runlogs[0]?.lot_no ?? (detailRow?.lot_no ?? null);

    console.log(`[TKDOC_GET] tk_id=${id} tk_status=${head.tk_status} tk_active=${head.tk_active} run_no=${latestRunNo || "-"}`);

    return res.json({
      actor,
      id,
      lot_no:      latestLotNo,
      tk_status:   head.tk_status,
      tk_active:   Number(head.tk_active),
      is_active:   Number(head.tk_active) === 1,
      detail: detailRow ? {
        tk_id:            detailRow.tk_id,
        MC_id:            detailRow.MC_id,
        op_sta_id:        detailRow.op_sta_id,
        op_sc_id:         detailRow.op_sc_id,
        u_id:             detailRow.u_id,
        part_id:          detailRow.part_id,
        lot_no:           detailRow.lot_no,
        tk_status:        head.tk_status,
        tk_active:        Number(head.tk_active),
        tk_created_at_ts: detailRow.tk_created_at_ts
          ? new Date(detailRow.tk_created_at_ts).toISOString()
          : null,
        part_no:   detailRow.part_no,
        part_name: detailRow.part_name,
        run_no:    latestRunNo,
      } : null,
      runlogs,
    });
  } catch (err) {
    console.error("[TKDOC_GET][ERROR]", err);
    return res.status(500).json({ message: "Get failed", actor, error: err.message });
  }
};

// ---------------------------------------------------------------------------
// PUT /api/TKDocs/:id  (admin only)
//
// Fields ที่แก้ได้:
//   - part_no   -> ลบ TKRunLog เดิม + Call SP ใหม่ + UPDATE TKDetail
//   - tk_active -> เปิด(1)/ปิด(0) เอกสาร → UPDATE TKHead.tk_active
//
// Guard (assertEditable) — ต้องผ่านทั้งคู่:
//   1. TKDetail.MC_id IS NULL AND TKDetail.op_sta_id IS NULL  (ยังไม่ start)
//   2. ไม่มี Transfer record เลย (COUNT = 0)
//
// หมายเหตุ: tk_status ไม่อยู่ในนี้ มันเอาไว้ track progress ของระบบเอง
// ---------------------------------------------------------------------------
exports.updateTkDoc = async (req, res) => {
  const actor = actorOf(req);
  const tk_id = String(req.params.id || "").trim();

  if (actor.role !== "admin") {
    return res.status(403).json({ message: "Forbidden (admin only)", actor });
  }
  if (!tk_id) {
    return res.status(400).json({ message: "tk_id is required", actor });
  }

  const part_no   = req.body.part_no   !== undefined ? String(req.body.part_no).trim() : undefined;
  const tk_active = req.body.tk_active !== undefined ? Number(req.body.tk_active)      : undefined;

  if (part_no === undefined && tk_active === undefined) {
    return res.status(400).json({ message: "Required at least 1 field: part_no, tk_active", actor });
  }
  if (part_no !== undefined && !part_no) {
    return res.status(400).json({ message: "part_no cannot be empty", actor });
  }
  if (tk_active !== undefined && ![0, 1].includes(tk_active)) {
    return res.status(400).json({ message: "tk_active must be 0 or 1", actor });
  }

  try {
    const pool = getPool();

    // 1) ตรวจ TKHead มีอยู่จริง
    const [headRows] = await pool.query(
      `SELECT tk_id, tk_status, tk_active FROM ${SAFE_TKHEAD} WHERE tk_id = ? LIMIT 1`,
      [tk_id]
    );
    if (!headRows[0]) {
      return res.status(404).json({ message: "TK Document not found", tk_id, actor });
    }

    // 2) Guard
    const guard = await assertEditable(pool, tk_id);
    if (!guard.ok) {
      return res.status(409).json({ message: guard.reason, detail: guard.detail, actor });
    }

    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
      const changed = {};

      // --- toggle tk_active ---
      if (tk_active !== undefined) {
        await conn.query(
          `UPDATE ${SAFE_TKHEAD} SET tk_active = ? WHERE tk_id = ?`,
          [tk_active, tk_id]
        );
        changed.tk_active = tk_active;
      }

      // --- เปลี่ยน part_no ---
      let partResult = null;
      if (part_no !== undefined) {
        const [partRows] = await conn.query(
          `SELECT part_id, part_no, part_name FROM \`part\` WHERE part_no = ? LIMIT 1`,
          [part_no]
        );
        const partRow = partRows[0];
        if (!partRow) {
          await conn.rollback();
          conn.release();
          return res.status(400).json({ message: `part_no not found: ${part_no}`, actor });
        }

        // ลบ TKRunLog เดิม (run_no/lot_no ผูกกับ part เดิม ใช้ต่อไม่ได้)
        await conn.query(`DELETE FROM \`TKRunLog\` WHERE tk_id = ?`, [tk_id]);

        // Call SP ใหม่สำหรับ part ใหม่
        await conn.query(
          `CALL usp_TKRunLog_Create(?, ?, ?, @run_no, @lot_no)`,
          [tk_id, Number(partRow.part_id), Number(actor.u_id)]
        );
        const [[outRow]] = await conn.query(`SELECT @run_no AS run_no, @lot_no AS lot_no`);

        const new_run_no = outRow?.run_no ?? null;
        const new_lot_no = outRow?.lot_no ?? null;

        if (!new_run_no || !new_lot_no) {
          throw new Error("DB did not return run_no/lot_no from usp_TKRunLog_Create");
        }

        await conn.query(
          `UPDATE ${SAFE_TKDETAIL} SET part_id = ?, lot_no = ? WHERE tk_id = ?`,
          [Number(partRow.part_id), new_lot_no, tk_id]
        );

        changed.part_no  = partRow.part_no;
        changed.part_id  = Number(partRow.part_id);
        changed.run_no   = new_run_no;
        changed.lot_no   = new_lot_no;
        partResult = {
          part_id:   Number(partRow.part_id),
          part_no:   partRow.part_no,
          part_name: partRow.part_name,
          run_no:    new_run_no,
          lot_no:    new_lot_no,
        };
      }

      await conn.commit();
      conn.release();

      console.log(
        `[TKDOC_UPDATE] tk_id=${tk_id} changed=${JSON.stringify(changed)} ` +
        `updated_by=${actor.u_id} (${actor.u_firstname} ${actor.u_lastname})`
      );

      return res.json({
        message: "Updated TK Document",
        id:      tk_id,
        changed,
        ...(partResult ?? {}),
        actor,
      });
    } catch (e) {
      await conn.rollback();
      conn.release();
      throw e;
    }
  } catch (err) {
    console.error("[TKDOC_UPDATE][ERROR]", err);
    return res.status(500).json({ message: "Update failed", actor, error: err.message });
  }
};

// ---------------------------------------------------------------------------
// DELETE /api/TKDocs/:id  (admin only)
//
// Guard (assertEditable):
//   1. TKDetail.MC_id IS NULL AND TKDetail.op_sta_id IS NULL
//   2. ไม่มี Transfer record เลย (COUNT = 0)
//
// ลำดับการลบ: TKDetail -> TKRunLog -> TKHead
// ---------------------------------------------------------------------------
exports.deleteTkDoc = async (req, res) => {
  const actor = actorOf(req);
  const tk_id = String(req.params.id || "").trim();

  if (actor.role !== "admin") {
    return res.status(403).json({ message: "Forbidden (admin only)", actor });
  }
  if (!tk_id) {
    return res.status(400).json({ message: "tk_id is required", actor });
  }

  try {
    const pool = getPool();

    const [headRows] = await pool.query(
      `SELECT tk_id, tk_status, tk_active FROM ${SAFE_TKHEAD} WHERE tk_id = ? LIMIT 1`,
      [tk_id]
    );
    if (!headRows[0]) {
      return res.status(404).json({ message: "TK Document not found", tk_id, actor });
    }

    const guard = await assertEditable(pool, tk_id);
    if (!guard.ok) {
      return res.status(409).json({ message: guard.reason, detail: guard.detail, actor });
    }

    // ดึงข้อมูลเอกสารก่อนลบ เพื่อใส่ใน response
    const [detailRows] = await pool.query(
      `SELECT d.part_id, p.part_no, p.part_name, d.lot_no
       FROM ${SAFE_TKDETAIL} d
       LEFT JOIN \`part\` p ON p.part_id = d.part_id
       WHERE d.tk_id = ? LIMIT 1`,
      [tk_id]
    );
    const detail = detailRows[0] ?? null;

    const [runlogRows] = await pool.query(
      `SELECT run_no FROM \`TKRunLog\` WHERE tk_id = ? ORDER BY created_at_ts ASC LIMIT 1`,
      [tk_id]
    );
    const runlog = runlogRows[0] ?? null;

    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
      await conn.query(`DELETE FROM ${SAFE_TKDETAIL} WHERE tk_id = ?`, [tk_id]);
      await conn.query(`DELETE FROM \`TKRunLog\`     WHERE tk_id = ?`, [tk_id]);
      await conn.query(`DELETE FROM ${SAFE_TKHEAD}   WHERE tk_id = ?`, [tk_id]);

      await conn.commit();
      conn.release();

      console.log(`[TKDOC_DELETE] tk_id=${tk_id} deleted_by=${actor.u_id} (${actor.u_firstname} ${actor.u_lastname})`);

      return res.json({
        message:   "Deleted TK Document",
        tk_id,
        part_id:   detail?.part_id   ?? null,
        part_no:   detail?.part_no   ?? null,
        part_name: detail?.part_name ?? null,
        lot_no:    detail?.lot_no    ?? null,
        run_no:    runlog?.run_no    ? String(runlog.run_no).trim() : null,
        actor,
      });
    } catch (e) {
      await conn.rollback();
      conn.release();
      throw e;
    }
  } catch (err) {
    console.error("[TKDOC_DELETE][ERROR]", err);
    return res.status(500).json({ message: "Delete failed", actor, error: err.message });
  }
};