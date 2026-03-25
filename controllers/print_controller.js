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
    color_name   = "",
  } = lot;

  const tf         = Number(tf_rs_code);
  const isMaster   = tf === 1;
  const isSplit    = tf === 2;
  const isCo       = tf === 3;
  const showNew    = !isMaster && (new_part_no || new_part_name);
  const showColor  = !!color_name;

  // ── Lot No: "Lot No:" ใหญ่ (font "2"), ค่าเล็ก (font "1") ─
  // font "2" = ~10 dot/char → "Lot No: " (8 chars) = ~80 dots → value เริ่มที่ x=92
  // font "1" = ~8 dot/char, 500-92=408 dots → ~51 chars/line
  const LOT_VAL_X  = 112;  // x ที่ value เริ่ม — ชิด "Lot No:" มากขึ้น
  // actual font"1" ~10 dots/char → line1: (500-4-112)/10 = 38, ใช้ 36 (safe margin)
  const LOT_LINE1  = 34;
  const LOT_LINE2  = 44;   // line2 เริ่มจาก x=12: (476-4-12)/10 = 46 → 44 safe
  // smart wrap: ตัดที่ space หรือ "-" ก่อนตำแหน่ง LOT_LINE1 ไม่ให้ขาดกลางคำ
  let cutAt = LOT_LINE1;
  if (lot_no.length > LOT_LINE1) {
    const sub = lot_no.slice(0, LOT_LINE1);
    const lastBreak = Math.max(sub.lastIndexOf(" "), sub.lastIndexOf("-"));
    if (lastBreak > LOT_LINE1 * 0.7) cutAt = lastBreak + 1;
  }
  const lotVal1 = lot_no.slice(0, cutAt).trimEnd();
  const lotVal2 = lot_no.length > cutAt
    ? lot_no.slice(cutAt, cutAt + LOT_LINE2)
    : "";

  // ── QR Code ────────────────────────────────────────────────
  const qrText  = lot_no;  // encode lot_no เท่านั้น (scan แล้วหา tk_id ผ่าน TKRunLog)
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
  const R_EDGE = 472;             // right content boundary (leave ~4mm margin from paper edge)
  const divX  = Math.min(qrLeft + qrTotal + 2, 165);  // cap left panel ≤165 so right panel ≥325
  const infoX = divX + 6;
  const rW    = R_EDGE - divX;   // separator bar width (never touches right BOX edge)
  // font "1" = 8 dots/char → max chars before right edge
  const MAX_VAL = Math.floor((R_EDGE - infoX) / 8);
  const trunc   = (s) => String(s || '').slice(0, MAX_VAL);

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
  const cbY = 224, cbSize = 18, cbTextY = cbY + 3;
  const mX = 16, sX = 168, cX = 316;

  // ── Row Y: pitch 28 label→label ─────────────────────────────
  // label y, value y+12, separator y+24 → next label y+28
  // 5 rows: 50,78,106,134,162 → last value=174 (fits <228)
  // 4 rows: 50,82,114,146     → last value=158 (fits <228)
  // 3 rows: 50,86,122         → last value=134
  // 2 rows: 50,92             → last value=104
  const R = (showNew && showColor)
    ? { pnL:50,  pnV:62,  pnS:74,
        nmL:78,  nmV:90,  nmS:102,
        npL:106, npV:118, npS:130,
        nnL:134, nnV:146, nnS:158,
        clL:162, clV:174 }
    : showNew
    ? { pnL:50,  pnV:62,  pnS:76,
        nmL:82,  nmV:94,  nmS:108,
        npL:114, npV:126, npS:140,
        nnL:146, nnV:158 }
    : showColor
    ? { pnL:50,  pnV:62,  pnS:76,
        nmL:82,  nmV:94,  nmS:108,
        clL:114, clV:126 }
    : { pnL:50,  pnV:64,  pnS:82,
        nmL:90,  nmV:104 };

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
    `BOX 4,4,476,272,2`,

    // "Lot No:" ใหญ่ (font "2") + value เล็ก (font "1") inline
    `TEXT 12,6,"2",0,1,1,"Lot No:"`,
    `TEXT ${LOT_VAL_X},10,"1",0,1,1,"${lotVal1}"`,
    ...(lotVal2 ? [`TEXT ${LOT_VAL_X},26,"1",0,1,1,"${lotVal2}"`] : []),
    `BAR 4,44,472,2`,

    // Vertical divider
    `BAR ${divX},46,2,170`,
  ];

  // ── QR BAR commands (bounds-checked: max 496x296) ──────────
  const QR_MAX_X = 472, QR_MAX_Y = 226;
  for (let row = 0; row < modSize; row++) {
    const y = dataY + row * cell;
    if (y + cell > QR_MAX_Y) break;          // หยุดถ้าเกิน area
    let run = -1;
    for (let col = 0; col <= modSize; col++) {
      const dark = col < modSize && m.get(row, col);
      if (dark && run === -1) { run = col; }
      else if (!dark && run !== -1) {
        const x = dataX + run * cell;
        const w = (col - run) * cell;
        if (x + w <= QR_MAX_X) {             // skip ถ้าเกิน width
          lines.push(`BAR ${x},${y},${w},${cell}`);
        }
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
  lines.push(`TEXT ${infoX},${R.pnV},"1",0,1,1,"${trunc(part_no)}"`);
  lines.push(`BAR ${divX},${R.pnS},${rW},1`);

  boldLabel(infoX, R.nmL, "Part Name:");
  lines.push(`TEXT ${infoX},${R.nmV},"1",0,1,1,"${trunc(part_name)}"`);

  if (showNew) {
    lines.push(`BAR ${divX},${R.nmS},${rW},1`);
    boldLabel(infoX, R.npL, "New Part No:");
    lines.push(`TEXT ${infoX},${R.npV},"1",0,1,1,"${trunc(new_part_no)}"`);
    lines.push(`BAR ${divX},${R.npS},${rW},1`);
    boldLabel(infoX, R.nnL, "New Part Name:");
    lines.push(`TEXT ${infoX},${R.nnV},"1",0,1,1,"${trunc(new_part_name)}"`);
  }

  if (showColor) {
    lines.push(`BAR ${divX},${showNew ? R.nnS ?? (R.nnV + 14) : R.nmS},${rW},1`);
    boldLabel(infoX, R.clL, "Color Name:");
    lines.push(`TEXT ${infoX},${R.clV},"1",0,1,1,"${trunc(color_name)}"`);
  }

  // ── Checkboxes ─────────────────────────────────────────────
  lines.push(`BAR 4,216,472,2`);

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
  lines.push(`BAR 4,248,472,2`);
  lines.push(`TEXT 12,254,"1",0,1,1,"Print Time:  Date: ${dateStr}   Time: ${timeStr}"`);

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
// Body (เลือกใช้ตาม use case):
//   { tk_id }                          → ปริ้นทุก lot ใหม่ของ TK
//   { tk_id, op_sc_id }                → ปริ้นเฉพาะ scan นั้น (HH)
//   { tk_id, op_sta_id }               → ปริ้นเฉพาะ lot ที่ gen จาก station นั้น (React reprint)
//   { tk_id, lot_no }                  → ปริ้น lot เดี่ยว (ปุ่ม Print per row)
//   { tk_id, reprint: true }           → ปริ้นซ้ำทุก lot
//   { tk_id, op_sta_id, reprint:true } → ปริ้นซ้ำเฉพาะ station นั้น
// ════════════════════════════════════════════════════════════════
async function printBarcode(req, res) {
  const { tk_id, op_sc_id, op_sta_id, lot_no: single_lot_no, reprint } = req.body;
  const isReprint    = reprint === true || reprint === "true";
  const filterBySta  = !!op_sta_id;
  const filterByScan = !!op_sc_id;
  const filterByLot  = !!single_lot_no;

  if (!tk_id)        return res.status(400).json({ ok: false, message: "tk_id is required" });
  if (!PRINTER_NAME) return res.status(500).json({ ok: false, message: "PRINTER_NAME not set in .env" });

  const pool      = getPool();
  const printedBy = req.user?.u_id || null;
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

  // ── 1b) ถ้ามี op_sta_id → ตรวจว่า TK เคยผ่าน station นี้จริงไหม ──
  if (filterBySta) {
    try {
      const [staRows] = await pool.query(
        `SELECT sc.op_sc_id, s.op_sta_name
         FROM \`op_scan\` sc
         LEFT JOIN \`op_station\` s ON s.op_sta_id = sc.op_sta_id
         WHERE sc.tk_id = ? AND sc.op_sta_id = ?
         LIMIT 1`,
        [tk_id, op_sta_id]
      );
      if (staRows.length === 0) {
        const [doneRows] = await pool.query(
          `SELECT DISTINCT sc.op_sta_id, s.op_sta_name
           FROM \`op_scan\` sc
           LEFT JOIN \`op_station\` s ON s.op_sta_id = sc.op_sta_id
           WHERE sc.tk_id = ?
           ORDER BY sc.op_sc_id ASC`,
          [tk_id]
        );
        const doneList = doneRows.length > 0
          ? doneRows.map(r => `${r.op_sta_id} (${r.op_sta_name || '-'})`).join(", ")
          : "ยังไม่มี station ที่ทำงาน";
        return res.status(404).json({
          ok: false,
          message: `${tk_id} ไม่มีการทำงานที่ station "${op_sta_id}" — station ที่ทำงานจริงใน TK นี้: ${doneList}`,
          tk_id,
          op_sta_id_requested: op_sta_id,
          stations_worked: doneRows.map(r => ({ op_sta_id: r.op_sta_id, op_sta_name: r.op_sta_name })),
        });
      }
    } catch (e) {
      return res.status(500).json({ ok: false, message: "DB error (op_scan station check): " + e.message });
    }
  }

  // ── 2) ดึง lots จาก t_transfer ───────────────────────────
  //   op_sta_id → JOIN op_scan เพื่อกรองเฉพาะ lot ที่ gen จาก station นั้น
  //   op_sc_id  → เฉพาะ scan นั้น (HH)
  //   lot_no    → lot เดี่ยว
  //   (ไม่มี)   → ทุก lot ใหม่ของ TK
  let transferLots = [];
  try {
    let subWhere = "WHERE to_tk_id = ?";
    const subParams = [tk_id];

    if (filterByLot) {
      subWhere += " AND to_lot_no = ?";
      subParams.push(single_lot_no);
    } else if (filterByScan) {
      subWhere += " AND op_sc_id = ?";
      subParams.push(op_sc_id);
    }

    const outerWhere = filterByLot ? "t.to_lot_no = ?" : "t.from_lot_no != t.to_lot_no";
    const staJoin    = filterBySta ? "INNER JOIN `op_scan` osc ON osc.op_sc_id = t.op_sc_id AND osc.op_sta_id = ?" : "";
    const staParam   = filterBySta ? [op_sta_id] : [];

    const [rows] = await pool.query(
      `SELECT
         t.to_lot_no           AS lot_no,
         t.from_lot_no,
         t.tf_rs_code,
         t.lot_parked_status,
         fp.part_no            AS part_no,
         fp.part_name          AS part_name,
         tp.part_no            AS new_part_no,
         tp.part_name          AS new_part_name,
         cp.color_name         AS color_name
       FROM \`t_transfer\` t
       ${staJoin}
       INNER JOIN (
         SELECT to_lot_no, MAX(transfer_id) AS max_id
         FROM \`t_transfer\`
         ${subWhere}
         GROUP BY to_lot_no
       ) latest ON latest.to_lot_no = t.to_lot_no AND latest.max_id = t.transfer_id
       LEFT JOIN \`TKRunLog\`       frl ON frl.lot_no  = t.from_lot_no
       LEFT JOIN \`part\`           fp  ON fp.part_id  = frl.part_id
       LEFT JOIN \`TKRunLog\`       trl ON trl.lot_no  = t.to_lot_no
       LEFT JOIN \`part\`           tp  ON tp.part_id  = trl.part_id
       LEFT JOIN \`color_painting\`  cp  ON cp.color_id = t.color_id
       WHERE ${outerWhere}
       ORDER BY t.transfer_id ASC`,
      [...staParam, ...subParams, ...(filterByLot ? [single_lot_no] : [])]
    );
    transferLots = rows;
  } catch (e) {
    return res.status(500).json({ ok: false, message: "DB error (t_transfer): " + e.message });
  }

  // ── 3) ถ้าไม่มี transfer เลย → เอกสารพึ่งสร้าง ──────────
  //       ดึง base lot จาก TKRunLog + part  tf_rs_code=0
  //       ⚠️ ถ้าส่ง lot_no มา → ไม่ fallback เพราะ lot ไม่ตรง tk_id = error ทันที
  let allLots = transferLots;
  if (allLots.length === 0) {
    // ถ้าส่ง lot_no มาแต่หาไม่เจอใน tk_id นี้ → reject ทันที ไม่ fallback
    if (filterByLot) {
      return res.status(404).json({
        ok: false,
        message: `ไม่พบ lot_no="${single_lot_no}" ใน tk_id=${tk_id} — กรุณาตรวจสอบ tk_id ให้ถูกต้อง`,
      });
    }
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
    return res.status(404).json({
      ok: false,
      message: filterBySta
        ? `${tk_id} ไม่มี lot ที่ gen ใหม่จาก station "${op_sta_id}" — อาจยังไม่ได้ทำงานหรือทุก lot เป็น lot เดิม (from=to)`
        : filterByScan
        ? `ไม่พบ Lot active สำหรับ op_sc_id=${op_sc_id} (อาจเป็น lot พักทั้งหมด)`
        : `ไม่พบข้อมูล Lot สำหรับ tk_id=${tk_id}`,
    });
  }

  // ── 4) กรอง lot ที่ยังไม่เคยปริ้น (ถ้า reprint=true → ปริ้นทุก lot) ─
  const toPrint = [];
  const already_printed_lots = [];

  for (const lot of allLots) {
    if (isReprint) {
      toPrint.push(lot);
      continue;
    }
    let printed = false;
    try {
      const [rows] = await pool.query(
        `SELECT pl_id FROM print_log WHERE lot_no = ? AND status = 1 LIMIT 1`,
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

  // ── 5) build TSPL + ปริ้น (ส่งทีละ label เพื่อป้องกัน buffer overflow) ──
  const printResults = [];
  try {
    for (const lot of toPrint) {
      const tspl = buildLotLabel({ ...lot, tk_id }, now);
      const r = await sendToPrinter(tspl, PRINTER_NAME);
      printResults.push(r);
      // รอให้ printer flush buffer ก่อนรับ job ถัดไป (TSC TH240 @ SPEED 3)
      await new Promise(resolve => setTimeout(resolve, 400));
    }
    const result = printResults.join(", ");

    const log_errors = [];
    for (const lot of toPrint) {
      try {
        await pool.query(
          `INSERT INTO print_log (tk_id, lot_no, printed_by, copies, printer_ip, status, created_at)
           VALUES (?, ?, ?, 1, ?, 1, NOW())`,
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
           VALUES (?, ?, ?, 1, ?, 0, NOW())`,
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

// ════════════════════════════════════════════════════════════════
// GET /api/print/test
// ทดสอบเชื่อมต่อเครื่องปริ้น — feed กระดาษ 1 ใบเปล่า ไม่พิมพ์อะไร
// ════════════════════════════════════════════════════════════════
async function testPrinter(req, res) {
  if (!PRINTER_NAME) {
    return res.status(500).json({ ok: false, message: "PRINTER_NAME ยังไม่ได้ตั้งค่าใน .env" });
  }

  const tspl = [
    "SIZE 63 mm, 38 mm",
    "GAP 3 mm, 0",
    "DIRECTION 1",
    "SET TEAR ON",
    "CLS",
    "PRINT 1,1",
    "",
  ].join("\r\n");

  try {
    const result = await sendToPrinter(tspl, PRINTER_NAME);
    return res.json({
      ok:      true,
      message: "เครื่องปริ้นตอบสนองปกติ",
      printer: PRINTER_NAME,
      result,
    });
  } catch (err) {
    return res.status(500).json({
      ok:      false,
      message: "เชื่อมต่อเครื่องปริ้นไม่ได้",
      printer: PRINTER_NAME,
      error:   err.message,
    });
  }
}

module.exports = { printBarcode, getPrintHistory, testPrinter };