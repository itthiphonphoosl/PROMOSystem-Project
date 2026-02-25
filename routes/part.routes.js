// routes/part.routes.js
const express = require("express");
const router = express.Router();
const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const { listParts } = require("../controllers/part.controller");

// ให้ HH/PC ที่ login แล้วเรียกได้
router.get(
  "/parts",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  listParts
);

module.exports = router;