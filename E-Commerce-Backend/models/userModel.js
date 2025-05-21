import mongoose from "mongoose";

// Create a new schema for our user data
const userSchema = new mongoose.Schema(
  {
    name: { type: String, required: true },
    username: { type: String, required: true, unique: true },
    password: { type: String, required: true },
    address: {
      city: { type: String, required: true },
      zipcode: { type: String, required: true },
      country: { type: String, required: true },
    },
  },
  { timestamps: true }
);

const userModel = mongoose.model.user || mongoose.model("user", userSchema);
export default userModel;