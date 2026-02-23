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
// ✅ แก้เพิ่ม requireClientType(["PC"])
router.get("/machines",      requireAuth, requireRole(["admin"]),requireClientType(["PC"]), listMachines);
router.get("/machines/:id",  requireAuth, requireRole(["admin"]),requireClientType(["PC"]), getMachineById);
router.post("/machines",     requireAuth, requireRole(["admin"]),requireClientType(["PC"]), createMachine);
router.put("/machines/:id",  requireAuth, requireRole(["admin"]),requireClientType(["PC"]), updateMachine);

// เส้นนี้ HH+PC ได้ทั้งคู่ถูกต้องแล้ว ✅
router.get("/machines/in-station", requireAuth, requireRole(["admin","operator"]), requireClientType(["HH","PC"]), listMachinesMyStation);


module.exports = router;