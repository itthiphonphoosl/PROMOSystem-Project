// routes/opScan.routes.js
const express = require("express");
const router = express.Router();

const { requireAuth, requireRole } = require("../middleware/auth.middleware");
const { createOpScan, listPending, approve, reject } = require("../controllers/opScan.controller");

// Operator creates scan (PENDING)
router.post("/", requireAuth, requireRole(["operator"]), createOpScan);

// Supervisor lists pending
router.get("/pending", requireAuth, requireRole(["admin", "manager"]), listPending);

// Supervisor approve/reject
router.post("/:id/approve", requireAuth, requireRole(["admin", "manager"]), approve);
router.post("/:id/reject", requireAuth, requireRole(["admin", "manager"]), reject);

module.exports = router;
