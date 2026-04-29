const express = require("express");
const router = express.Router();

const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const {
  listStations,
  getStationById,
  updateStation,
  listPublicStations,
} = require("../controllers/station.controller");

// ✅ Public — ไม่ต้อง auth (ใช้สำหรับ dropdown ตอน login)
router.get("/stations/public", listPublicStations);

// ✅ Web(PC) + admin/manager เท่านั้น
router.get("/stations",     requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), listStations);
router.get("/stations/:id", requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), getStationById);
router.put("/stations/:id", requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), updateStation);

module.exports = router;