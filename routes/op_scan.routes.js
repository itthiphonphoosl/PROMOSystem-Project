const express = require("express");
const router = express.Router();

const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const opScanController      = require("../controllers/op_scan.controller");
const opScanQueryController = require("../controllers/op_scan_query.controller");

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


router.get(
  "/op-scan/parked",
  requireAuth, requireRole(["admin", "operator"]), requireClientType(["PC", "HH"]),
  opScanQueryController.getParkedLots
);


router.get(
  "/op-scans/lookup-by-lot/:lot_no",
  requireAuth, requireRole(["operator"]), requireClientType(["HH"]),
  opScanQueryController.lookupTkByLotNo
);

router.get(
  "/op-scan/summary/:tk_id",
  requireAuth, requireRole(["admin", "operator"]), requireClientType(["PC", "HH"]),
  opScanQueryController.getTkSummary
);

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