const express   = require('express');
const router    = express.Router();

const { requireAuth } = require('../middleware/auth.middleware');
const printCtrl = require('../controllers/print_controller');

if (typeof printCtrl.printBarcode !== 'function') {
  throw new Error('print_controller.js โหลดไม่ได้ — เช็ค syntax ใน controllers/print_controller.js');
}

router.post('/barcode', requireAuth, printCtrl.printBarcode);

router.get('/history/:tk_id', requireAuth, printCtrl.getPrintHistory);

router.get('/test', requireAuth, printCtrl.testPrinter);

module.exports = router;