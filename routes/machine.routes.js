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

// ✅ ต้องวาง /machines/in-station ก่อน /machines/:id
// เพราะ Express match route ตามลำดับจากบนลงล่าง
// ถ้า :id อยู่ก่อน Express จะคิดว่า "in-station" คือ id แล้วเข้า admin route แทน

router.get(
  "/machines/in-station",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["HH", "PC"]),
  listMachinesMyStation
);

router.get(
  "/machines",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  listMachines
);

router.get(
  "/machines/:id",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  getMachineById
);

router.post(
  "/machines",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  createMachine
);

router.put(
  "/machines/:id",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  updateMachine
);

module.exports = router;