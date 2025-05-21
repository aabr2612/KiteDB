import express from "express";
import cors from "cors";
import "dotenv/config";
import connectDB from "./config/mongodb.js";
import connectCloudinary from "./config/cloudinary.js";
import userRouter from "./routes/userRouter.js";

// App Config
const app = express();
const port = process.env.PORT || 4000;

connectDB();
connectCloudinary();

// middlewares
app.use(express.json());
app.use(cors());

// api endpoints
app.use("/api/user", userRouter);
app.use("/api/product", productRouter);
app.use("/api/cart", cartRouter);
app.use("/api/order",orderRouter);

// api end points
app.get("/", (req, res) => {
  try {
    res.send("API Working");
  } catch (error) {
    console.log(error);
  }
});

app.listen(port, () => console.log("Server started on port : " + port));
