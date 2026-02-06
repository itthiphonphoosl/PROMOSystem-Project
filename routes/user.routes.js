// routes/user.routes.js
const express = require("express");
const router = express.Router();

const { requireAuth, requireRole } = require("../middleware/auth.middleware");
const { getPool, sql } = require("../config/db");
const { createUser } = require("../controllers/auth.controller");

// Reuse existing controller (so behavior stays the same)
const { getAllUsers } = require("../controllers/auth.controller");

// ✅ [เพิ่มใหม่] import controller สำหรับอัปเดต user (แก้ได้ 5 ฟิลด์)
const { updateUser } = require("../controllers/user.controller");

// ✅ Create user (admin/manager only)
router.post("/users", requireAuth, requireRole(["admin", "manager"]), createUser);

// ✅ Current user (op/ad/ma)
router.get("/user", requireAuth, (req, res) => {
  return res.json({ message: "success", user: req.user });
});

// ✅ All users (admin/manager only)
router.get("/users", requireAuth, requireRole(["admin", "manager"]), getAllUsers);

// ✅ User by id (admin/manager only)
router.get(
  "/users/:id",
  requireAuth,
  requireRole(["admin", "manager"]),
  async (req, res) => {
    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ message: "Missing user id" });

    if (!/^\d+$/.test(id)) {
      return res.status(400).json({ message: "Invalid user id" });
    }

    try {
      const pool = await getPool();
      if (!pool) return res.status(500).json({ message: "Database connection failed" });

      const result = await pool
        .request()
        .input("u_id", sql.Int, Number(id))
        .query(`
          SELECT TOP 1
            u_id,
            u_username,
            u_name,
            u_type,
            u_active,
            u_created_ts,
            u_updated_ts
          FROM [user]
          WHERE u_id = @u_id
        `);

      const user = result.recordset[0];
      if (!user) return res.status(404).json({ message: "User not found" });

      return res.json({ message: "success", user });
    } catch (err) {
      console.error("GET USER BY ID ERROR:", err);
      return res.status(500).json({ message: "Server error" });
    }
  }
);

/**
 * UPDATE user (admin/manager only)
 * PUT /api/users/:id
 * body ส่งได้: u_name, u_username, u_password, u_type, u_active
 * - op ไม่มีสิทธิ์ เพราะ requireRole(["admin","manager"]) กันไว้
 */
router.put("/users/:id", requireAuth, requireRole(["admin", "manager"]), updateUser);

module.exports = router;
