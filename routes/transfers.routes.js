const express = require("express");
const router = express.Router();

const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const { splitTransfer } = require("../controllers/transfers.controller");

router.post(
  "/split",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["HH", "PC"]),
  splitTransfer
);

module.exports = router;
