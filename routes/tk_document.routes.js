const express = require("express");
const router = express.Router();

const {
  requireAuth,
  requireRole,
  requireClientType,
} = require("../middleware/auth.middleware");

const {
  createTkDoc,     // (จริงๆ คือ create TKHead+first TKDetail)
  getTkDocById,    // get เอกสาร/รายละเอียด
  listTkDocs,      // list เอกสาร
} = require("../controllers/tk_document.controller");

// ✅ ดูรายการเอกสาร (React/PC)
router.get(
  "/TKDocs",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC"]),
  listTkDocs
);

// ✅ ดูเอกสารรายตัว (React/PC + HH scan)
router.get(
  "/TKDocs/:id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  getTkDocById
);

// ✅ สร้างเอกสาร (admin เท่านั้น + PC เท่านั้น)
router.post(
  "/TKDocs",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  createTkDoc
);

module.exports = router;
