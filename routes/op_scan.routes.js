const express = require("express");
const router = express.Router();

const {
  requireAuth,
  requireRole,
  requireClientType,
} = require("../middleware/auth.middleware");

const opScanController = require("../controllers/op_scan.controller");

router.get(
  "/op-scan/active",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  opScanController.listAllActiveOpScans
);
router.get(
  "/op-scan/active/:tk_id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  opScanController.getActiveOpScanByTkId
);



router.get(
  "/op-scan/:op_sc_id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  opScanController.getOpScanById
);

//ก่อน scan → HH สแกนถาด อยากรู้ว่าถาดนี้ผ่านมาจากไหนแล้ว
// finish → ดูผลสรุปว่าตัวเองทำอะไรไปบ้าง
//Admin ดูบน PC → ต้องการ audit trail ย้อนหลัง
router.get(
  "/op-scan/summary/:tk_id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  opScanController.getTkSummary
);

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