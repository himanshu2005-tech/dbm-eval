import mongoose from 'mongoose';
import dotenv from 'dotenv';

dotenv.config();

const DBConnection = async () => {
    const MONGODB_URL = process.env.MONGO_URI;
    try {
        await mongoose.connect(MONGODB_URL, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        console.log("connected successfully");
    } catch (error) {
        console.log("error:", error.message);
    }

};

export default DBConnection;