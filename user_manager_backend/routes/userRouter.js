import express from "express";
import { addUser, listUsers, getUser, updateUser, removeUser } from "../controllers/userController.js";

// Router for user
const userRouter = express.Router();

// Routes
userRouter.post("/add", addUser);
userRouter.get("/list", listUsers);
userRouter.get("/:username", getUser);
userRouter.post("/update", updateUser);
userRouter.post("/remove", removeUser);

export default userRouter;