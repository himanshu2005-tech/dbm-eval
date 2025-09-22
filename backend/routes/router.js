import express from 'express';

import userRoutes from './user.js';
import datasetRoutes from './datasets.js'
const router = express.Router();

router.use('/users', userRoutes);
router.use('/dataset', datasetRoutes);

export default router;
