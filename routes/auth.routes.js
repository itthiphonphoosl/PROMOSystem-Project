const express = require("express");
const router = express.Router();

const { requireAuth } = require("../middleware/auth.middleware");
const {
  login,
  logout,
  createUser,
  getAllUsers,
  getUsersByType,
} = require("../controllers/auth.controller");

// ✅ Auth
router.post("/login", login);
router.post("/logout", requireAuth, logout);

// ✅ Users
router.post("/users", createUser);
router.get("/users", getAllUsers);
router.get("/users/type/:u_type", getUsersByType);

module.exports = router;
