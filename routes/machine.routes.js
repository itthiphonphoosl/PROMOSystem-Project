// routes/machine.routes.js
const express = require("express");
const router = express.Router();

const {
  requireAuth,
  requireRole,
  requireClientType, 
} = require("../middleware/auth.middleware");

const {
  listMachines,
  getMachineById,
  createMachine,
  updateMachine,
  listMachinesMyStation, 
} = require("../controllers/machine.controller");

router.get("/machines", requireAuth, requireRole(["admin"]), listMachines);
router.get(
  "/machines/in-station",
  requireAuth,
  requireRole(["operator"]),
  requireClientType(["HH"]),
  listMachinesMyStation
);
router.get("/machines/:id", requireAuth, requireRole(["admin"]), getMachineById);
router.post("/machines", requireAuth, requireRole(["admin"]), createMachine);
router.put("/machines/:id", requireAuth, requireRole(["admin"]), updateMachine);



module.exports = router;