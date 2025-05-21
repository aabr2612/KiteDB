import validator from "validator";
import bcrypt from "bcrypt";
import jwt from "jsonwebtoken";
import userModel from "../models/userModel.js";

// Function to create token
const createToken = (id) => {
  return jwt.sign({ id }, process.env.JWT_SECRET);
};

// Route for user login
const loginUser = async (req, res) => {
  try {
    const { username, password } = req.body;

    // Checking user already existing
    const user = await userModel.findOne({ username });
    if (!user) {
      return res.json({ success: false, message: "User doesn't exist!" });
    }

    // Checking password
    const isMatch = await bcrypt.compare(password, user.password);

    // If password is correct
    if (isMatch) {
      const token = createToken(user._id);
      res.json({ success: true, token });
    } else {
      res.json({ success: false, message: "Invalid credentials" });
    }
  } catch (error) {
    console.log(error);
    res.json({ success: false, message: error.message });
  }
};

// Route for user registration
const registerUser = async (req, res) => {
  try {
    const { name, username, password, address } = req.body;

    // Checking user already existing
    const exists = await userModel.findOne({ username });
    if (exists) {
      return res.json({ success: false, message: "Username already exists" });
    }

    // Validating user data
    if (!validator.isStrongPassword(password)) {
      return res.json({
        success: false,
        message: "Please enter a strong password (min 8 chars, with numbers, symbols)",
      });
    }
    if (!name || !address || !address.city || !address.zipcode || !address.country) {
      return res.json({
        success: false,
        message: "All fields are required",
      });
    }

    // Hashing password
    const salt = await bcrypt.genSalt(10);
    const hashedPassword = await bcrypt.hash(password, salt);
    const newUser = new userModel({
      name,
      username,
      password: hashedPassword,
      address,
    });

    // Saving user to the database
    const user = await newUser.save();
    const token = createToken(user._id);

    return res.json({ success: true, token });
  } catch (error) {
    console.log(error);
    res.json({ success: false, message: error.message });
  }
};

// Add a new user
const addUser = async (req, res) => {
  try {
    const userData = req.body;
    const user = new userModel(userData);
    await user.save();
    res.status(201).json({ success: true });
  } catch (error) {
    if (error.code === 11000) {
      res.status(400).json({ success: false, message: 'Username already exists' });
    } else {
      res.status(500).json({ success: false, message: error.message });
    }
  }
};

// List all users
const listUsers = async (req, res) => {
  try {
    const users = await userModel.find();
    res.status(200).json({ success: true, users });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

// Get a user by username
const getUser = async (req, res) => {
  try {
    const user = await userModel.findOne({ username: req.params.username });
    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }
    res.status(200).json({ success: true, user });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

// Update a user by username
const updateUser = async (req, res) => {
  try {
    const { username, ...updateData } = req.body;
    const user = await userModel.findOneAndUpdate(
      { username },
      { ...updateData, password: await bcrypt.hash(updateData.password, 10) },
      { new: true }
    );
    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }
    res.status(200).json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

// Remove a user by username
const removeUser = async (req, res) => {
  try {
    const { username } = req.body;
    const user = await userModel.findOneAndDelete({ username });
    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }
    res.status(200).json({ success: true });
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
};

export { loginUser, registerUser, adminLogin, addUser, listUsers, getUser, updateUser, removeUser };