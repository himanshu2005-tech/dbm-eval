import mongoose from 'mongoose';

const reportSchema = new mongoose.Schema({
  faster_system: { type: String, required: true },
  execution_time_diff: { type: Number, required: true },
  cpu_diff: { type: Number, required: true },
  memory_diff: { type: Number, required: true },
  disk_read_diff: { type: Number, required: true },
  disk_write_diff: { type: Number, required: true },
}, { timestamps: true });

const Report = mongoose.model('reports', reportSchema);

export default Report;
