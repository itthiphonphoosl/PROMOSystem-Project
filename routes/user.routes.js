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

router.post("/users",
  requireAuth,
  requireRole(["admin"]),
  createUser
);

router.get("/user",
  requireAuth,
  getCurrentUser
);

router.get("/users",
  requireAuth,
  requireRole(["admin"]),
  listUsers
);

router.get("/users/:id",
  requireAuth,
  requireRole(["admin"]),
  getUserById
);

router.put("/users/:id",
  requireAuth,
  requireRole(["admin"]),
  updateUser
);

module.exports = router;