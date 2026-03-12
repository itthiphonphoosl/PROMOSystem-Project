// routes/part.routes.js
const express = require("express");
const router = express.Router();
const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const { listParts, getPartById, updatePart, createPart } = require("../controllers/part.controller");

// GET /api/parts — ทั้งหมด (?all=true รวม inactive, ?q=ค้นหา)
router.get(
  "/parts",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  listParts
);

// GET /api/parts/:part_id — รายการเดียว
router.get(
  "/parts/:part_id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  getPartById
);

// PUT /api/parts/:part_id — แก้ไข (admin PC เท่านั้น)
router.put(
  "/parts/:part_id",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  updatePart
);

// POST /api/parts — เพิ่ม part ใหม่ (admin PC เท่านั้น)
router.post(
  "/parts",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  createPart
);

module.exports = router;