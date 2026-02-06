const express = require("express");
const router = express.Router();

const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const {
  listStations,
  getStationById,
  createStation,
  updateStation,
  setStationActive,
} = require("../controllers/station.controller");

// ✅ Web(PC) + admin/manager เท่านั้น
router.get("/stations", requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), listStations);
router.get("/stations/:id", requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), getStationById);
// router.post("/stations", requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), createStation);
router.put("/stations/:id", requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), updateStation);
// router.patch("/stations/:id/active", requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), setStationActive);

module.exports = router;
