const express = require("express");
const router = express.Router();

const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const {
  listStations,
  getStationById,
  updateStation,
  listPublicStations,
} = require("../controllers/station.controller");

router.get("/stations/public", listPublicStations);

router.get("/stations",     requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), listStations);
router.get("/stations/:id", requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), getStationById);
router.put("/stations/:id", requireAuth, requireRole(["admin", "manager"]), requireClientType(["PC"]), updateStation);

module.exports = router;