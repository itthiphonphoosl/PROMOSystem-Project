// routes/machine.routes.js
const express = require("express");
const router = express.Router();

const { requireAuth, requireRole } = require("../middleware/auth.middleware");
const {
  listMachines,
  getMachineById,
  createMachine,
  updateMachine,
} = require("../controllers/machine.controller");

router.get("/machines", requireAuth, requireRole(["admin"]), listMachines);
router.get("/machines/:id", requireAuth, requireRole(["admin"]), getMachineById);
router.post("/machines", requireAuth, requireRole(["admin"]), createMachine);
router.put("/machines/:id", requireAuth, requireRole(["admin"]), updateMachine);

module.exports = router;
