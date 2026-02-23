// routes/user.routes.js
const express = require("express");
const router  = express.Router();

const { requireAuth, requireRole } = require("../middleware/auth.middleware");
const { createUser }               = require("../controllers/auth.controller");
const {
  getCurrentUser,
  listUsers,
  getUserById,
  updateUser,
} = require("../controllers/user.controller");

// POST /api/users — สร้าง user (admin/manager)
router.post("/users",
  requireAuth,
  requireRole(["admin"]),
  createUser
);

// GET /api/user — ดู user ตัวเอง (ทุก role)
router.get("/user",
  requireAuth,
  getCurrentUser
);

// GET /api/users — ดู user ทั้งหมด (admin/manager)
router.get("/users",
  requireAuth,
  requireRole(["admin"]),
  listUsers
);

// GET /api/users/:id — ดู user รายคน (admin/manager)
router.get("/users/:id",
  requireAuth,
  requireRole(["admin"]),
  getUserById
);

// PUT /api/users/:id — แก้ user (admin/manager)
router.put("/users/:id",
  requireAuth,
  requireRole(["admin"]),
  updateUser
);

module.exports = router;