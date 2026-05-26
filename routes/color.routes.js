const express = require("express");
const router  = express.Router();

const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const colorController = require("../controllers/color.controller");


router.get(
  "/colors",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  colorController.listColors
);


router.get(
  "/colors/:color_id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  colorController.getColorById
);


router.put(
  "/colors/:color_id",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  colorController.updateColor
);


router.post(
  "/colors",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  colorController.createColor
);

module.exports = router;