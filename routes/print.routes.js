// routes/print.routes.js
const express   = require('express');
const router    = express.Router();

const { requireAuth } = require('../middleware/auth.middleware');
const printCtrl = require('../controllers/print_controller');

// ตรวจสอบว่า controller load ได้จริง
if (typeof printCtrl.printBarcode !== 'function') {
  throw new Error('print_controller.js โหลดไม่ได้ — เช็ค syntax ใน controllers/print_controller.js');
}

// POST /api/print/barcode
router.post('/barcode', requireAuth, printCtrl.printBarcode);

// GET /api/print/history/:tk_id
router.get('/history/:tk_id', requireAuth, printCtrl.getPrintHistory);

module.exports = router;