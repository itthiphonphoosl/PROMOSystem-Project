// routes/tray.routes.js
const express = require("express");
const router = express.Router();

const { requireAuth, requireRole } = require("../middleware/auth.middleware");
const { createTray, getTrayByQr } = require("../controllers/tray.controller");

// Create tray (admin/manager)
router.post("/", requireAuth, requireRole(["admin", "manager"]), createTray);

// Get tray by QR (any authenticated)
router.get("/by-qr/:qr", requireAuth, getTrayByQr);

module.exports = router;
