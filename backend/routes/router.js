import express from 'express';

import userRoutes from './user.js';
import datasetRoutes from './datasets.js';
import reportRoutes from './report.js';   // import report routes

const router = express.Router();

router.use('/users', userRoutes);
router.use('/dataset', datasetRoutes);
router.use('/reports', reportRoutes);    // add reports route

export default router;
