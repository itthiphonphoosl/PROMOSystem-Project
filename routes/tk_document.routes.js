const express = require("express");
const router  = express.Router();

const {
  requireAuth,
  requireRole,
  requireClientType,
} = require("../middleware/auth.middleware");

const {
  createTkDoc,
  getTkDocById,
  listTkDocs,
  updateTkDoc,
  deleteTkDoc,
} = require("../controllers/tk_document.controller");

router.get(
  "/TKDocs",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC"]),
  listTkDocs
);

router.get(
  "/TKDocs/:id",
  requireAuth,
  requireRole(["admin", "operator"]),
  requireClientType(["PC", "HH"]),
  getTkDocById
);

router.post(
  "/TKDocs",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  createTkDoc
);

router.put(
  "/TKDocs/:id",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  updateTkDoc
);

router.delete(
  "/TKDocs/:id",
  requireAuth,
  requireRole(["admin"]),
  requireClientType(["PC"]),
  deleteTkDoc
);

module.exports = router;