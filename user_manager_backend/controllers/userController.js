import { kiteDBClient } from "../config/kiteDB.js";
import { validateUser } from "../models/userModel.js";

// Add a new user
const addUser = async (req, res) => {
  try {
    console.log("Adding user");
    const userData = req.body;
    validateUser(userData);

    // Check if user already exists
    const findResponse = await kiteDBClient.sendCommand(
      `users.find{"username": {"$eq": "${userData.username}"}}`
    );
    if (findResponse.success && findResponse.data && findResponse.data.length > 0) {
      console.log("user already exists");
      return res.status(400).json({ success: false, message: "Username already exists!" });
    }

    // Insert user
    const insertResponse = await kiteDBClient.sendCommand(
      `users.add{${JSON.stringify(userData)}}`
    );
    if (!insertResponse.success) {
      return res.status(500).json({ success: false, message: insertResponse.message });
    }
    res.status(201).json({ success: true });
  } catch (error) {
    console.log(error);
    res.status(500).json({ success: false, message: error.message });
  }
};

// List all users
const listUsers = async (req, res) => {
  try {
    const findResponse = await kiteDBClient.sendCommand(`users.find{}`);
    if (!findResponse.success) {
      return res.status(500).json({ success: false, message: findResponse.message });
    }
    res.status(200).json({ success: true, users: findResponse.data || [] });
  } catch (error) {
    console.log(error);
    res.status(500).json({ success: false, message: error.message });
  }
};

// Get a user by username
const getUser = async (req, res) => {
  try {
    const { username } = req.params;
    const findResponse = await kiteDBClient.sendCommand(
      `users.find{"username": {"$eq": "${username}"}}`
    );
    if (!findResponse.success) {
      return res.status(500).json({ success: false, message: findResponse.message });
    }
    if (!findResponse.data || findResponse.data.length === 0) {
      return res.status(404).json({ success: false, message: "User not found" });
    }
    res.status(200).json({ success: true, user: findResponse.data[0] });
  } catch (error) {
    console.log(error);
    res.status(500).json({ success: false, message: error.message });
  }
};

// Update a user by username
const updateUser = async (req, res) => {
  try {
    const { username, ...updateData } = req.body;
    validateUser({ username, ...updateData });

    // Update user
    const updateResponse = await kiteDBClient.sendCommand(
      `users.update{"username": "${username}", "name":"${updateData.name}", "street":"${updateData.street}", "city":"${updateData.city}", "country":"${updateData.country}"}` // Adjust this line to match your update command
    );
    if (!updateResponse.success) {
      return res.status(500).json({ success: false, message: updateResponse.message });
    }
    if (updateResponse.data === 0) {
      return res.status(404).json({ success: false, message: "User not found" });
    }
    res.status(200).json({ success: true });
  } catch (error) {
    console.log(error);
    res.status(500).json({ success: false, message: error.message });
  }
};

// Remove a user by username
const removeUser = async (req, res) => {
  try {
    const { username } = req.body;
    const deleteResponse = await kiteDBClient.sendCommand(
      `users.delete{"username": {"$eq": "${username}"}}`
    );
    if (!deleteResponse.success) {
      return res.status(500).json({ success: false, message: deleteResponse.message });
    }
    if (deleteResponse.data === 0) {
      return res.status(404).json({ success: false, message: "User not found" });
    }
    res.status(200).json({ success: true });
  } catch (error) {
    console.log(error);
    res.status(500).json({ success: false, message: error.message });
  }
};

export { addUser, listUsers, getUser, updateUser, removeUser };