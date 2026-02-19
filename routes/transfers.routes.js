// routes/transfers.routes.js
const express = require("express");
const router = express.Router();

const transfers = require("../controllers/transfers.controller");

// ✅ Keep GET for viewing logs
router.get("/", transfers.listTransfers);
router.get("/:transfer_id", transfers.getTransferById);

// ⛔ Disable POST split (deprecated)
router.post("/split", (req, res) => {
  return res.status(410).json({
    message:
      "This endpoint is deprecated. Use POST /api/op-scan/finish with tf_rs_code=2 (Split) instead.",
  });
});

module.exports = router;