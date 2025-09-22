import express from 'express';
import multer from 'multer';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';

const router = express.Router();

const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, './uploads/');
  },
  filename: function (req, file, cb) {
    const uniqueName = uuidv4() + path.extname(file.originalname);
    cb(null, uniqueName);
  }
});

const upload = multer({ 
  storage: storage,
  limits: { fileSize: 5 * 1024 * 1024 * 1024 }
});

router.post('/upload', upload.single('dataset'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'Dataset file is required' });
  }
  try {
    const uploadId = uuidv4();

    res.status(201).json({
      message: 'Dataset uploaded successfully',
      uploadId: uploadId,
      filename: req.file.filename,
      path: req.file.path,
      size: req.file.size,
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

export default router;
