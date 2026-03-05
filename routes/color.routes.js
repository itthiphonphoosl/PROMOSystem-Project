// routes/color.routes.js
const express = require("express");
const router  = express.Router();

const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const colorController = require("../controllers/color.controller");

// ─────────────────────────────────────────────────────────
// GET /api/colors
// ดูสีทั้งหมด — admin (PC) และ operator (HH) ดูได้
// ?all=true → รวม inactive (admin เท่านั้น)
// ─────────────────────────────────────────────────────────
router.get(
  "/colors",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  colorController.listColors
);

// ─────────────────────────────────────────────────────────
// GET /api/colors/:color_id
// ดูสีรายการเดียว — admin (PC) และ operator (HH) ดูได้
// ─────────────────────────────────────────────────────────
router.get(
  "/colors/:color_id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  colorController.getColorById
);

// ─────────────────────────────────────────────────────────
// PUT /api/colors/:color_id
// แก้ไขสี — admin (PC) เท่านั้น
// body: { color_no?, color_name?, color_status? }
// ─────────────────────────────────────────────────────────
router.put(
  "/colors/:color_id",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  colorController.updateColor
);

module.exports = router;