import express from 'express';

import userRoutes from './user.js';

const router = express.Router();

router.use('/users', userRoutes);

export default router;
