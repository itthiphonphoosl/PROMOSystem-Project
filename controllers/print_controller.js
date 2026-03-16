// controllers/print_controller.js
// ⚠️  ต้องรัน: npm install qrcode
//
// ── print_log DDL (เพิ่ม lot_no column) ─────────────────────────
// ALTER TABLE print_log ADD COLUMN lot_no VARCHAR(100) NULL AFTER tk_id;
// ALTER TABLE print_log ADD INDEX idx_lot_no (lot_no);
// ────────────────────────────────────────────────────────────────
const QRCode = require("qrcode");
const { exec } = require("child_process");
const path = require("path");
const fs = require("fs");
const os = require("os");
const { getPool } = require("../config/db");

const PRINTER_NAME = process.env.PRINTER_NAME;
const PRINTER_IP   = process.env.PRINTER_IP || null;

// ════════════════════════════════════════════════════════════════
// buildLotLabel  —  สร้าง TSPL 1 label ตาม layout ในรูป
//
// Label: 63×38mm = 504×304 dots (203dpi = 8dots/mm)
//
// ┌─────────────────────────────────────────────────────┐ y=4
// │ Lot No:                                             │ y=8
// │ 251203-45100-K0WX-TF10-M1-XXXXXXXXXXXX             │ y=30
// ├──────────┬──────────────────────────────────────────┤ y=50
// │          │ Part No:      382-C11-184                │ y=58
// │  QR Code │─────────────────────────────────────────│ y=78
// │  (tk_id) │ Part Name:    CAL-RR-MGZA               │ y=82
// │          │─────────────────────────────────────────│ y=100
// │          │ New Part No:  382-C11-200   (split/co)  │ y=104
// │          │─────────────────────────────────────────│ y=122
// │          │ New Part Name: CAL-RR-MGZA  (split/co)  │ y=126
// ├──────────┴──────────────────────────────────────────┤ y=228
// │  [■]Master-ID  [□]Split-ID  [□]Co-ID               │ y=234
// ├─────────────────────────────────────────────────────┤ y=262
// │  Print Time:  Date: DD/MM/YYYY   Time: HH:MM:SS    │ y=268
// └─────────────────────────────────────────────────────┘ y=300
//
// tf_rs_code=1 → ■Master-ID, ไม่แสดง New Part
// tf_rs_code=2 → ■Split-ID,  แสดง New Part
// tf_rs_code=3 → ■Co-ID,     แสดง New Part
// ════════════════════════════════════════════════════════════════
function buildLotLabel(lot, now) {
  const {
    lot_no       = "",
    tk_id        = "",
    part_no      = "",
    part_name    = "",
    new_part_no  = "",
    new_part_name= "",
    tf_rs_code   = 1,
  } = lot;

  const tf       = Number(tf_rs_code);
  const isMaster = tf === 1;
  const isSplit  = tf === 2;
  const isCo     = tf === 3;
  const showNew  = !isMaster && (new_part_no || new_part_name);

  // ── Lot No: "Lot No:" ใหญ่ (font "2"), ค่าเล็ก (font "1") ─
  // font "2" = ~10 dot/char → "Lot No: " (8 chars) = ~80 dots → value เริ่มที่ x=92
  // font "1" = ~8 dot/char, 500-92=408 dots → ~51 chars/line
  const LOT_VAL_X  = 140;  // x ที่ value เริ่ม — font"2" 16dot/char × 8chars = 128 + 12 = 140
  const LOT_LINE1  = 45;   // chars ในบรรทัด 1
  const LOT_LINE2  = 62;   // chars ในบรรทัด 2 (เต็มแถว)
  const lotVal1    = lot_no.slice(0, LOT_LINE1);
  const lotVal2    = lot_no.length > LOT_LINE1
    ? lot_no.slice(LOT_LINE1, LOT_LINE1 + LOT_LINE2)
    : "";

  // ── QR Code ────────────────────────────────────────────────
  const qrText  = tk_id || lot_no;
  const qr      = QRCode.create(qrText, { errorCorrectionLevel: "H" });
  const m       = qr.modules;
  const modSize = m.size;

  // ── QR: fill left panel y=52..228 = 176 dots, quiet=2 ─────
  const quiet   = 2;
  const qrLeft  = 4;
  const qrTop   = 52;
  const qrAreaH = 228 - qrTop;   // 176 dots
  const qrAreaW = 160;
  const cellH   = Math.floor(qrAreaH / (modSize + quiet * 2));
  const cellW   = Math.floor(qrAreaW / (modSize + quiet * 2));
  const cell    = Math.max(3, Math.min(cellH, cellW));
  const qrTotal = (modSize + quiet * 2) * cell;
  const dataX   = qrLeft + quiet * cell;
  const dataY   = qrTop  + quiet * cell;

  // ── Layout ─────────────────────────────────────────────────
  const divX  = qrLeft + qrTotal + 2;
  const infoX = divX + 6;
  const rW    = 500 - divX;

  // ── Date/Time ──────────────────────────────────────────────
  const dd   = String(now.getDate()).padStart(2, "0");
  const mo   = String(now.getMonth() + 1).padStart(2, "0");
  const yyyy = now.getFullYear();
  const hh   = String(now.getHours()).padStart(2, "0");
  const mi   = String(now.getMinutes()).padStart(2, "0");
  const ss   = String(now.getSeconds()).padStart(2, "0");
  const dateStr = `${dd}/${mo}/${yyyy}`;
  const timeStr = `${hh}:${mi}:${ss}`;

  // ── Checkbox ───────────────────────────────────────────────
  const cbY = 234, cbSize = 20, cbTextY = cbY + 4;
  const mX = 16, sX = 168, cX = 316;

  // ── Row Y: label bold font"1" + value font"1", ชิดขึ้น ────
  // เริ่มที่ y=56 (ห่างจากเส้น y=50 แค่ 6 dots)
  // row pitch: label 14 + value 14 + sep 4 = 32 dots
  const R = showNew
    ? { pnL:56,  pnV:70,  pnS:84,
        nmL:88,  nmV:102, nmS:116,
        npL:120, npV:134, npS:148,
        nnL:152, nnV:166 }
    : { pnL:64,  pnV:80,  pnS:100,
        nmL:104, nmV:120 };

  // ── TSPL ───────────────────────────────────────────────────
  const lines = [
    `SIZE 63 mm, 38 mm`,
    `GAP 3 mm, 0`,
    `DIRECTION 1`,
    `SET TEAR ON`,
    `SET PEEL OFF`,
    `REFERENCE 0,0`,
    `SPEED 3`,
    `DENSITY 6`,
    `CLS`,
    `BOX 4,4,500,300,2`,

    // "Lot No:" ใหญ่ (font "2") + value เล็ก (font "1") inline
    `TEXT 12,6,"2",0,1,1,"Lot No:"`,
    `TEXT ${LOT_VAL_X},10,"1",0,1,1,"${lotVal1}"`,
    ...(lotVal2 ? [`TEXT 12,26,"1",0,1,1,"${lotVal2}"`] : []),
    `BAR 4,44,496,2`,

    // Vertical divider
    `BAR ${divX},46,2,182`,
  ];

  // ── QR BAR commands ────────────────────────────────────────
  for (let row = 0; row < modSize; row++) {
    let run = -1;
    for (let col = 0; col <= modSize; col++) {
      const dark = col < modSize && m.get(row, col);
      if (dark && run === -1) { run = col; }
      else if (!dark && run !== -1) {
        lines.push(`BAR ${dataX + run * cell},${dataY + row * cell},${(col - run) * cell},${cell}`);
        run = -1;
      }
    }
  }

  // ── Right panel: label bold (double-print +1px) + value ────
  const boldLabel = (x, y, text) => {
    lines.push(`TEXT ${x},${y},"1",0,1,1,"${text}"`);
    lines.push(`TEXT ${x+1},${y},"1",0,1,1,"${text}"`);
  };

  boldLabel(infoX, R.pnL, "Part No:");
  lines.push(`TEXT ${infoX},${R.pnV},"1",0,1,1,"${part_no}"`);
  lines.push(`BAR ${divX},${R.pnS},${rW},1`);

  boldLabel(infoX, R.nmL, "Part Name:");
  lines.push(`TEXT ${infoX},${R.nmV},"1",0,1,1,"${part_name}"`);

  if (showNew) {
    lines.push(`BAR ${divX},${R.nmS},${rW},1`);
    boldLabel(infoX, R.npL, "New Part No:");
    lines.push(`TEXT ${infoX},${R.npV},"1",0,1,1,"${new_part_no}"`);
    lines.push(`BAR ${divX},${R.npS},${rW},1`);
    boldLabel(infoX, R.nnL, "New Part Name:");
    lines.push(`TEXT ${infoX},${R.nnV},"1",0,1,1,"${new_part_name}"`);
  }

  // ── Checkboxes ─────────────────────────────────────────────
  lines.push(`BAR 4,228,496,2`);

  if (isMaster) lines.push(`BAR ${mX},${cbY},${cbSize},${cbSize}`);
  else          lines.push(`BOX ${mX},${cbY},${mX+cbSize},${cbY+cbSize},2`);
  lines.push(`TEXT ${mX+cbSize+4},${cbTextY},"1",0,1,1,"Master-ID"`);

  if (isSplit)  lines.push(`BAR ${sX},${cbY},${cbSize},${cbSize}`);
  else          lines.push(`BOX ${sX},${cbY},${sX+cbSize},${cbY+cbSize},2`);
  lines.push(`TEXT ${sX+cbSize+4},${cbTextY},"1",0,1,1,"Split-ID"`);

  if (isCo)     lines.push(`BAR ${cX},${cbY},${cbSize},${cbSize}`);
  else          lines.push(`BOX ${cX},${cbY},${cX+cbSize},${cbY+cbSize},2`);
  lines.push(`TEXT ${cX+cbSize+4},${cbTextY},"1",0,1,1,"Co-ID"`);

  // ── Print Time ─────────────────────────────────────────────
  lines.push(`BAR 4,262,496,2`);
  lines.push(`TEXT 12,268,"1",0,1,1,"Print Time:  Date: ${dateStr}    Time: ${timeStr}"`);

  lines.push(`PRINT 1,1`, ``);

  console.log(`[LABEL] QR v${qr.version} ${modSize}mod cell=${cell} bottom=${dataY+modSize*cell}/228 divX=${divX}`);
  return lines.join("\r\n");
}

function sendToPrinter(tsplContent, printerName) {
  return new Promise((resolve, reject) => {
    const ts      = Date.now();
    const tmpDir  = os.tmpdir();
    const prnFile = path.join(tmpDir, `prn_${ts}.prn`);
    const ps1File = path.join(tmpDir, `prn_${ts}.ps1`);

    fs.writeFileSync(prnFile, tsplContent, "binary");

    const prnEsc = prnFile.replace(/\\/g, "\\\\");
    const prtEsc = printerName.replace(/\\/g, "\\\\").replace(/"/g, '\\"');

    const psScript = `
Add-Type -TypeDefinition @"
using System;
using System.Runtime.InteropServices;

[StructLayout(LayoutKind.Sequential, CharSet=CharSet.Ansi)]
public class DOCINFOA {
    [MarshalAs(UnmanagedType.LPStr)] public string pDocName;
    [MarshalAs(UnmanagedType.LPStr)] public string pOutputFile;
    [MarshalAs(UnmanagedType.LPStr)] public string pDataType;
}

public class RawPrint {
    [DllImport("winspool.drv", CharSet=CharSet.Auto, SetLastError=true)]
    public static extern bool OpenPrinter(string n, out IntPtr h, IntPtr d);
    [DllImport("winspool.drv", SetLastError=true)]
    public static extern bool StartDocPrinter(IntPtr h, int lvl, [In][MarshalAs(UnmanagedType.LPStruct)] DOCINFOA di);
    [DllImport("winspool.drv", SetLastError=true)]
    public static extern bool StartPagePrinter(IntPtr h);
    [DllImport("winspool.drv", SetLastError=true)]
    public static extern bool WritePrinter(IntPtr h, IntPtr p, int c, out int w);
    [DllImport("winspool.drv", SetLastError=true)]
    public static extern bool EndPagePrinter(IntPtr h);
    [DllImport("winspool.drv", SetLastError=true)]
    public static extern bool EndDocPrinter(IntPtr h);
    [DllImport("winspool.drv", SetLastError=true)]
    public static extern bool ClosePrinter(IntPtr h);
}
"@ -Language CSharp

\$prn   = "${prtEsc}"
\$file  = "${prnEsc}"
\$bytes = [System.IO.File]::ReadAllBytes(\$file)
\$hPrinter = [IntPtr]::Zero
[RawPrint]::OpenPrinter(\$prn, [ref]\$hPrinter, [IntPtr]::Zero) | Out-Null
if (\$hPrinter -eq [IntPtr]::Zero) { throw "Cannot open printer: \$prn" }
\$di = New-Object DOCINFOA
\$di.pDocName  = "NodeRawPrint"
\$di.pDataType = "RAW"
[RawPrint]::StartDocPrinter(\$hPrinter, 1, \$di)    | Out-Null
[RawPrint]::StartPagePrinter(\$hPrinter)            | Out-Null
\$ptr     = [System.Runtime.InteropServices.Marshal]::AllocHGlobal(\$bytes.Length)
\$written = 0
[System.Runtime.InteropServices.Marshal]::Copy(\$bytes, 0, \$ptr, \$bytes.Length)
[RawPrint]::WritePrinter(\$hPrinter, \$ptr, \$bytes.Length, [ref]\$written) | Out-Null
[System.Runtime.InteropServices.Marshal]::FreeHGlobal(\$ptr)
[RawPrint]::EndPagePrinter(\$hPrinter) | Out-Null
[RawPrint]::EndDocPrinter(\$hPrinter)  | Out-Null
[RawPrint]::ClosePrinter(\$hPrinter)   | Out-Null
Write-Output "OK:\$written"
`;

    fs.writeFileSync(ps1File, psScript, { encoding: "utf8" });
    const cmd = `powershell -NoProfile -ExecutionPolicy Bypass -File "${ps1File}"`;

    exec(cmd, { timeout: 20000 }, (err, stdout, stderr) => {
      try { fs.unlinkSync(prnFile); } catch (_) {}
      try { fs.unlinkSync(ps1File); } catch (_) {}
      if (err) return reject(new Error(stderr?.trim() || err.message));
      const out = stdout.trim();
      if (!out.startsWith("OK:")) return reject(new Error(out || "Unknown print error"));
      resolve(out);
    });
  });
}

// ════════════════════════════════════════════════════════════════
// POST /api/print/barcode
//
// Body:  { "tk_id": "TK2603130006" }
//
// Server ดึง DB เองทั้งหมด:
//   1) TKHead  → ตรวจ tk_active
//   2) t_transfer WHERE to_tk_id = tk_id
//      → แต่ละ to_lot_no = 1 label
//      → tf_rs_code ของ lot นั้น (1=Master 2=Split 3=Co-ID)
//      → part_no/part_name จาก from_lot (ของเดิม)
//      → new_part_no/new_part_name จาก to_lot (ของใหม่)
//   3) ถ้า NO transfers → เอกสารพึ่งสร้าง
//      → ดึง base lot จาก TKRunLog  tf_rs_code=0 (ไม่ทึบช่องไหน)
//   4) เช็ค print_log → ข้าม lot ที่ปริ้นแล้ว
// ════════════════════════════════════════════════════════════════
async function printBarcode(req, res) {
  const { tk_id } = req.body;

  if (!tk_id)        return res.status(400).json({ ok: false, message: "tk_id is required" });
  if (!PRINTER_NAME) return res.status(500).json({ ok: false, message: "PRINTER_NAME not set in .env" });

  const pool      = getPool();
  const printedBy = req.user?.u_username || "system";
  const now       = new Date();

  // ── 1) ตรวจ TKHead ───────────────────────────────────────
  let headRow;
  try {
    const [rows] = await pool.query(
      `SELECT tk_id, tk_active FROM \`TKHead\` WHERE tk_id = ? LIMIT 1`,
      [tk_id]
    );
    headRow = rows[0];
  } catch (e) {
    return res.status(500).json({ ok: false, message: "DB error (TKHead): " + e.message });
  }
  if (!headRow)               return res.status(404).json({ ok: false, message: `ไม่พบเอกสาร tk_id=${tk_id}` });
  if (!Number(headRow.tk_active)) return res.status(403).json({ ok: false, message: "เอกสารนี้ถูกปิดใช้งาน" });

  // ── 2) ดึง lots จาก t_transfer ───────────────────────────
  //    GROUP BY to_lot_no เอา transfer_id ล่าสุด (tf_rs_code อาจถูก update)
  let transferLots = [];
  try {
    // ใช้ subquery หา transfer_id ล่าสุดของแต่ละ to_lot_no ก่อน แล้วค่อย JOIN
    const [rows] = await pool.query(
      `SELECT
         t.to_lot_no          AS lot_no,
         t.tf_rs_code,
         fp.part_no           AS part_no,
         fp.part_name         AS part_name,
         tp.part_no           AS new_part_no,
         tp.part_name         AS new_part_name
       FROM \`t_transfer\` t
       -- เอาเฉพาะ transfer_id ล่าสุดของแต่ละ to_lot_no
       INNER JOIN (
         SELECT to_lot_no, MAX(transfer_id) AS max_id
         FROM \`t_transfer\`
         WHERE to_tk_id = ?
         GROUP BY to_lot_no
       ) latest ON latest.to_lot_no = t.to_lot_no AND latest.max_id = t.transfer_id
       LEFT JOIN \`TKRunLog\` frl ON frl.lot_no = t.from_lot_no
       LEFT JOIN \`part\`     fp  ON fp.part_id = frl.part_id
       LEFT JOIN \`TKRunLog\` trl ON trl.lot_no = t.to_lot_no
       LEFT JOIN \`part\`     tp  ON tp.part_id = trl.part_id
       ORDER BY t.transfer_id ASC`,
      [tk_id, tk_id]
    );
    transferLots = rows;
  } catch (e) {
    return res.status(500).json({ ok: false, message: "DB error (t_transfer): " + e.message });
  }

  // ── 3) ถ้าไม่มี transfer เลย → เอกสารพึ่งสร้าง ──────────
  //       ดึง base lot จาก TKRunLog + part  tf_rs_code=0
  let allLots = transferLots;
  if (allLots.length === 0) {
    try {
      const [rows] = await pool.query(
        `SELECT rl.lot_no, p.part_no, p.part_name
         FROM \`TKRunLog\` rl
         LEFT JOIN \`part\` p ON p.part_id = rl.part_id
         WHERE rl.tk_id = ?
         ORDER BY rl.trl_id ASC LIMIT 1`,
        [tk_id]
      );
      if (rows[0]) {
        allLots = [{
          lot_no:        rows[0].lot_no,
          tf_rs_code:    0,           // พึ่งสร้าง — ไม่ทึบช่องไหน
          part_no:       rows[0].part_no,
          part_name:     rows[0].part_name,
          new_part_no:   null,
          new_part_name: null,
        }];
      }
    } catch (e) {
      return res.status(500).json({ ok: false, message: "DB error (TKRunLog): " + e.message });
    }
  }

  if (allLots.length === 0) {
    return res.status(404).json({ ok: false, message: `ไม่พบข้อมูล Lot สำหรับ tk_id=${tk_id}` });
  }

  // ── 4) กรอง lot ที่ยังไม่เคยปริ้น ───────────────────────
  const toPrint = [];
  const already_printed_lots = [];

  for (const lot of allLots) {
    let printed = false;
    try {
      const [rows] = await pool.query(
        `SELECT pl_id FROM print_log WHERE lot_no = ? AND status = 'success' LIMIT 1`,
        [lot.lot_no]
      );
      printed = rows.length > 0;
    } catch (e) {
      console.warn("[print_log check]", e.message);
    }

    if (printed) {
      already_printed_lots.push({
        lot_no:     lot.lot_no,
        tf_rs_code: lot.tf_rs_code,
        part_no:    lot.part_no,
        part_name:  lot.part_name,
      });
    } else {
      toPrint.push(lot);
    }
  }

  if (toPrint.length === 0) {
    return res.json({
      ok:      true,
      message: "ทุก Lot ปริ้นแล้ว ไม่มีอะไรต้องปริ้นเพิ่ม",
      tk_id,
      total_lots:           allLots.length,
      printed:              0,
      skipped:              already_printed_lots.length,
      printed_lots:         [],
      already_printed_lots,
    });
  }

  // ── 5) build TSPL + ปริ้น ────────────────────────────────
  const tsplAll = toPrint.map(lot => buildLotLabel({ ...lot, tk_id }, now)).join("\r\n");

  try {
    const result = await sendToPrinter(tsplAll, PRINTER_NAME);

    const log_errors = [];
    for (const lot of toPrint) {
      try {
        await pool.query(
          `INSERT INTO print_log (tk_id, lot_no, printed_by, copies, printer_ip, status, created_at)
           VALUES (?, ?, ?, 1, ?, 'success', NOW())`,
          [tk_id, lot.lot_no, printedBy, PRINTER_IP]
        );
        console.log(`[print_log] ✓ ${lot.lot_no}`);
      } catch (e) {
        console.error("[print_log INSERT]", e.message);
        log_errors.push({ lot_no: lot.lot_no, error: e.message });
      }
    }

    return res.json({
      ok:      true,
      message: result,
      tk_id,
      total_lots:  allLots.length,
      printed:     toPrint.length,
      skipped:     already_printed_lots.length,
      printed_lots: toPrint.map(l => ({
        lot_no:        l.lot_no,
        tf_rs_code:    l.tf_rs_code,
        part_no:       l.part_no   || null,
        part_name:     l.part_name || null,
        new_part_no:   l.new_part_no   || null,
        new_part_name: l.new_part_name || null,
      })),
      already_printed_lots,
      // ถ้า log_errors มีข้อมูล → INSERT print_log ล้มเหลว เช็ค DB
      ...(log_errors.length > 0 && { log_errors }),
    });

  } catch (err) {
    console.error("[printBarcode]", err.message);
    for (const lot of toPrint) {
      try {
        await pool.query(
          `INSERT INTO print_log (tk_id, lot_no, printed_by, copies, printer_ip, status, created_at)
           VALUES (?, ?, ?, 1, ?, 'error', NOW())`,
          [tk_id, lot.lot_no, printedBy, PRINTER_IP]
        );
      } catch (_) {}
    }
    return res.status(500).json({ ok: false, message: err.message });
  }
}

// GET /api/print/history/:tk_id
async function getPrintHistory(req, res) {
  const { tk_id } = req.params;
  try {
    const pool = getPool();
    const [rows] = await pool.query(
      `SELECT * FROM print_log WHERE tk_id = ? ORDER BY created_at DESC LIMIT 50`,
      [tk_id]
    );
    return res.json({ ok: true, data: rows });
  } catch (err) {
    return res.status(500).json({ ok: false, message: err.message });
  }
}

module.exports = { printBarcode, getPrintHistory };