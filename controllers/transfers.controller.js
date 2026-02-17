// controllers/transfers.controller.js
const sql = require("mssql");
const { getPool } = require("../config/db");

const TKDETAIL_TABLE = process.env.TKDETAIL_TABLE || "dbo.TKDetail";
const TRANSFER_TABLE = process.env.TRANSFER_TABLE || "dbo.t_transfer";

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

// gen child tk_id (ใช้ TKDetail เป็นตัวกันซ้ำ)
async function genChildTkId(tx, docDate) {
  const prefix = `TK${yymmdd(docDate)}`;
  const r = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(30), `${prefix}%`)
    .query(`
      SELECT TOP 1 tk_id
      FROM ${TKDETAIL_TABLE} WITH (UPDLOCK, HOLDLOCK)
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

// gen transfer id (กัน NULL)
async function genTransferId(tx, docDate) {
  const prefix = `TF${yymmdd(docDate)}`;
  const r = await new sql.Request(tx)
    .input("likePrefix", sql.VarChar(30), `${prefix}%`)
    .query(`
      SELECT TOP 1 tk_transfer_id
      FROM ${TRANSFER_TABLE} WITH (UPDLOCK, HOLDLOCK)
      WHERE tk_transfer_id LIKE @likePrefix
      ORDER BY tk_transfer_id DESC
    `);

  let running = 1;
  if (r.recordset.length > 0) {
    const last = String(r.recordset[0].tk_transfer_id || "");
    const tail = last.slice(prefix.length);
    const n = parseInt(tail, 10);
    if (!Number.isNaN(n)) running = n + 1;
  }
  return `${prefix}${pad(running, 4)}`;
}

/**
 * POST /api/transfers/split
 * body:
 * {
 *   "from_tk_id": "TK2602100001",
 *   "split_qty": 20,
 *   "to_op_sta_id": "STA005",     // optional (ไม่ส่ง = ใช้ current เดิม)
 *   "note": "Split1",             // optional (ไม่ส่ง = ใส่ "SPLIT")
 *   "tf_rs_code": "SPLIT"         // optional
 * }
 */
exports.splitTransfer = async (req, res) => {
  const actor = actorOf(req);

  try {
    const pool = await getPool();

    const from_tk_id = String(req.body.from_tk_id || "").trim();
    const split_qty = Number(req.body.split_qty);
    const to_op_sta_id_in = req.body.to_op_sta_id ? String(req.body.to_op_sta_id).trim() : null;
    const tf_rs_code = String(req.body.tf_rs_code || "SPLIT").trim();

    // ✅ กัน NULL note (เพราะ DB ไม่ให้ NULL)
    const note = String(req.body.note || "SPLIT").trim() || "SPLIT";

    if (!from_tk_id) return res.status(400).json({ message: "from_tk_id ห้ามว่าง" });
    if (!Number.isFinite(split_qty) || split_qty <= 0) {
      return res.status(400).json({ message: "split_qty ต้องเป็นตัวเลข > 0" });
    }
    if (!actor.u_id) return res.status(401).json({ message: "Unauthorized", actor });

    const tx = new sql.Transaction(pool);
    await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

    try {
      const now = new Date();

      // 1) lock ตะกร้าต้นทาง
      const fromR = await new sql.Request(tx)
        .input("from_tk_id", sql.VarChar(20), from_tk_id)
        .query(`
          SELECT TOP 1 *
          FROM ${TKDETAIL_TABLE} WITH (UPDLOCK, HOLDLOCK)
          WHERE tk_id = @from_tk_id
        `);

      const fromRow = fromR.recordset?.[0];
      if (!fromRow) {
        await tx.rollback();
        return res.status(404).json({ message: "from_tk_id not found", from_tk_id, actor });
      }

      const fromQty = Number(fromRow.tk_current_qty);
      if (!Number.isFinite(fromQty) || fromQty <= 0) {
        await tx.rollback();
        return res.status(400).json({ message: "from tray qty invalid", from_tk_id, fromQty, actor });
      }

      // แนะนำ: split ต้องน้อยกว่า qty เดิม (ไม่งั้นตะกร้าเดิมเหลือ 0)
      if (split_qty >= fromQty) {
        await tx.rollback();
        return res.status(400).json({
          message: "split_qty ต้องน้อยกว่า qty ของตะกร้าต้นทาง",
          from_tk_id,
          fromQty,
          split_qty,
          actor,
        });
      }

      // 2) gen ตะกร้าใหม่
      const to_tk_id = await genChildTkId(tx, now);

      // root เดิม (ถ้า root คือจากตัวเอง ก็ยังใช้ได้)
      const rootId = String(fromRow.tk_parent_id || fromRow.tk_id);

      // ถ้าไม่ส่ง station ใหม่ -> ใช้ current เดิม
      const to_op_sta_id = to_op_sta_id_in || String(fromRow.current_op_sta_id || fromRow.created_op_sta_id);

      // 3) update qty ต้นทาง
      await new sql.Request(tx)
        .input("from_tk_id", sql.VarChar(20), from_tk_id)
        .input("split_qty", sql.Int, split_qty)
        .query(`
          UPDATE ${TKDETAIL_TABLE}
          SET tk_current_qty = tk_current_qty - @split_qty
          WHERE tk_id = @from_tk_id
        `);

      // 4) insert ตะกร้าใหม่ (copy part_id + lot_no จากต้นทาง)
      await new sql.Request(tx)
        .input("tk_id", sql.VarChar(20), to_tk_id)
        .input("u_id", sql.Int, Number(actor.u_id))
        .input("created_op_sta_id", sql.VarChar(20), String(fromRow.created_op_sta_id))
        .input("tk_parent_id", sql.VarChar(20), rootId)
        .input("current_op_sta_id", sql.VarChar(20), to_op_sta_id)
        .input("tk_current_qty", sql.Int, split_qty)
        .input("tk_status", sql.Int, 0)
        .input("part_id", sql.Int, Number(fromRow.part_id))
        input("lot_no", sql.NVarChar(300), String(fromRow.lot_no))        .input("tk_created_at_ts", sql.DateTime2(3), now)
        .query(`
          INSERT INTO ${TKDETAIL_TABLE}
            (tk_id, u_id, created_op_sta_id, tk_parent_id, current_op_sta_id,
             tk_current_qty, tk_status, part_id, lot_no, tk_created_at_ts)
          VALUES
            (@tk_id, @u_id, @created_op_sta_id, @tk_parent_id, @current_op_sta_id,
             @tk_current_qty, @tk_status, @part_id, @lot_no, @tk_created_at_ts)
        `);

      // 5) insert transfer log (กัน NULL ทั้ง id และ note)
      const tk_transfer_id = await genTransferId(tx, now);

      await new sql.Request(tx)
        .input("tk_transfer_id", sql.VarChar(20), tk_transfer_id)
        .input("from_tk_id", sql.VarChar(20), from_tk_id)
        .input("to_tk_id", sql.VarChar(20), to_tk_id)
        .input("op_sta_id", sql.VarChar(20), to_op_sta_id)
        .input("tk_transfer_ts", sql.DateTime2(3), now)
        .input("u_id", sql.Int, Number(actor.u_id))
        .input("tk_transfer_qty", sql.Int, split_qty)
        .input("tf_rs_code", sql.VarChar(20), tf_rs_code)
        .input("tk_transfer_note", sql.NVarChar(200), note)
        .query(`
          INSERT INTO ${TRANSFER_TABLE}
            (tk_transfer_id, from_tk_id, to_tk_id, op_sta_id, tk_transfer_ts,
             u_id, tk_transfer_qty, tf_rs_code, tk_transfer_note)
          VALUES
            (@tk_transfer_id, @from_tk_id, @to_tk_id, @op_sta_id, @tk_transfer_ts,
             @u_id, @tk_transfer_qty, @tf_rs_code, @tk_transfer_note)
        `);

      await tx.commit();

      return res.status(201).json({
        message: "Split success",
        actor,
        transfer: {
          tk_transfer_id,
          from_tk_id,
          to_tk_id,
          tk_transfer_qty: split_qty,
          op_sta_id: to_op_sta_id,
          tf_rs_code,
          tk_transfer_note: note,
          tk_transfer_ts: now.toISOString(),
        },
        result: {
          from_after_qty: fromQty - split_qty,
          new_tray: {
            tk_id: to_tk_id,
            tk_parent_id: rootId,
            lot_no: String(fromRow.lot_no),
            part_id: Number(fromRow.part_id),
            tk_current_qty: split_qty,
            tk_status: 0,
          },
        },
      });
    } catch (e) {
      await tx.rollback();
      throw e;
    }
  } catch (err) {
    console.error("[TRANSFER][SPLIT][ERROR]", err);
    return res.status(500).json({
      message: "Split failed",
      actor,
      error: err.message,
    });
  }
};
