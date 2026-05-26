const express = require("express");
const router = express.Router();
const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const { listParts, getPartById, updatePart, createPart } = require("../controllers/part.controller");

router.get(
  "/parts",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  listParts
);

router.get(
  "/parts/:part_id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  getPartById
);

router.put(
  "/parts/:part_id",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  updatePart
);

router.post(
  "/parts",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  createPart
);

module.exports = router;