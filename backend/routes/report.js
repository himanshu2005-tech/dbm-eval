import express from 'express';
import * as ReportController from '../controllers/ReportController.js';

const router = express.Router();

router.post('/', ReportController.createReport);           // Create a new report
router.get('/:id', ReportController.getReportById);        // Get report by ID
router.put('/:id', ReportController.updateReport);         // Update report by ID
router.delete('/:id', ReportController.deleteReport);      // Delete report by ID

export default router;
