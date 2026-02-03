// routes/auth.routes.js
const express = require("express");
const router = express.Router();

const { requireAuth } = require("../middleware/auth.middleware");
const { login, logout } = require("../controllers/auth.controller");

// ✅ Auth only
router.post("/login", login);
router.post("/logout", requireAuth, logout);

module.exports = router;
