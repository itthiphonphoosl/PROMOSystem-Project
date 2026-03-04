const express = require("express");
const router = express.Router();

const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const opScanController      = require("../controllers/op_scan.controller");
const opScanQueryController = require("../controllers/op_scan_query.controller");

// ✅ static routes ต้องอยู่ก่อน dynamic (:param) เสมอ
router.get(
  "/op-scan/active",
  requireAuth, requireRole(["admin", "operator"]), requireClientType(["PC", "HH"]),
  opScanQueryController.listAllActiveOpScans
);

router.get(
  "/op-scan/active/:tk_id",
  requireAuth, requireRole(["admin", "operator"]), requireClientType(["PC", "HH"]),
  opScanQueryController.getActiveOpScanByTkId
);

// ✅ /op-scan/parked ต้องอยู่ก่อน /op-scan/:op_sc_id
//    ถ้าสลับกัน Express จะ match "parked" เป็น :op_sc_id แทน
router.get(
  "/op-scan/parked",
  requireAuth, requireRole(["admin", "operator"]), requireClientType(["PC", "HH"]),
  opScanQueryController.getParkedLots
);

router.get(
  "/op-scan/summary/:tk_id",
  requireAuth, requireRole(["admin", "operator"]), requireClientType(["PC", "HH"]),
  opScanQueryController.getTkSummary
);

// ✅ dynamic route อยู่ท้ายสุดของ GET
router.get(
  "/op-scan/:op_sc_id",
  requireAuth, requireRole(["admin", "operator"]), requireClientType(["PC", "HH"]),
  opScanQueryController.getOpScanById
);

router.post(
  "/op-scan/start",
  requireAuth, requireRole(["operator"]), requireClientType(["HH"]),
  opScanController.startOpScan
);

router.post(
  "/op-scan/finish",
  requireAuth, requireRole(["operator"]), requireClientType(["HH"]),
  opScanController.finishOpScan
);

module.exports = router;