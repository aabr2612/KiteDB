import express from "express";
import cors from "cors";
import "dotenv/config";
import { connectDB } from "./config/kiteDB.js";
import userRouter from "./routes/userRouter.js";

// App Config
const app = express();
const port = process.env.PORT || 4000;

// Connect to KiteDB
connectDB().catch((err) => {
  console.error("Failed to start server due to KiteDB connection error:", err);
  process.exit(1);
});

// Middlewares
app.use(express.json());
app.use(cors());

// API endpoints
app.use("/api/user", userRouter);

// Default endpoint
app.get("/", (req, res) => {
  try {
    res.send("API Working");
  } catch (error) {
    console.log(error);
    res.status(500).send("Server Error");
  }
});

app.listen(port, () => console.log("Server started on port: " + port));