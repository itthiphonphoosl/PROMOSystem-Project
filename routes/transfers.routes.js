// routes/transfers.routes.js
const express = require("express");
const router = express.Router();

const { requireAuth, requireRole, requireClientType } = require("../middleware/auth.middleware");
const transfers = require("../controllers/transfers.controller");

router.get("/",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  transfers.listTransfers
);

router.get("/:transfer_id",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  transfers.getTransferById
);

module.exports = router;