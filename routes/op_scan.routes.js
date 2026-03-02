// routes/op_scan.routes.js
const express = require("express");
const router  = express.Router();

const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");

// ── แยก import จาก 2 controller ──
const opScanController      = require("../controllers/op_scan.controller");        // start, finish
const opScanQueryController = require("../controllers/op_scan_query.controller");  // GET ทั้งหมด

// ── GET endpoints ── (จาก op_scan_query.controller.js)
router.get(
  "/op-scan/active",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  opScanQueryController.listAllActiveOpScans
);

router.get(
  "/op-scan/active/:tk_id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  opScanQueryController.getActiveOpScanByTkId
);

router.get(
  "/op-scan/parked",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  opScanQueryController.getParkedLots        // ← API ใหม่ ดึง Lot พักของ Station
);

router.get(
  "/op-scan/summary/:tk_id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  opScanQueryController.getTkSummary
);

router.get(
  "/op-scan/:op_sc_id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  opScanQueryController.getOpScanById
);

// ── POST endpoints ── (จาก op_scan.controller.js)
router.post(
  "/op-scan/start",
  requireAuth,
  requireRole(["operator"]),
  requireClientType(["HH"]),
  opScanController.startOpScan
);

router.post(
  "/op-scan/finish",
  requireAuth,
  requireRole(["operator"]),
  requireClientType(["HH"]),
  opScanController.finishOpScan
);

module.exports = router;
