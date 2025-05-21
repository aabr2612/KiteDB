import net from "net";

// KiteDB client class to manage TCP connection
class KiteDBClient {
  constructor(host = "localhost", port = 5432) {
    this.host = host;
    this.port = port;
    this.client = null;
    this.authenticated = false;
    this.currentDb = null;
  }

  // Connect to KiteDB server and authenticate
  async connect(username = "admin", password = "admin", dbName = "e-commerce") {
    console.log(`Attempting to connect to KiteDB at ${this.host}:${this.port}`);
    this.client = new net.Socket();

    await new Promise((resolve, reject) => {
      this.client.on("connect", () => {
        console.log("Successfully connected to KiteDB server");
        resolve();
      });
      this.client.on("error", (err) => {
        console.error("KiteDB connection error:", err.message);
        reject(new Error(`Connection failed: ${err.message}`));
      });
      this.client.on("close", () => {
        console.log("KiteDB connection closed");
      });
      this.client.connect(this.port, this.host);
    });

    // Execute login, use, and create commands synchronously in the same block
    try {
      // Authenticate with KiteDB
      console.log(`Authenticating with username: ${username}`);
      const loginResponse = await this.sendCommand(`login ${username} ${password}`, 5000);
      console.log("Login response:", loginResponse);
      if (!loginResponse.success) {
        throw new Error(`Authentication failed: ${loginResponse.message}`);
      }
      this.authenticated = true;
      console.log("Authentication successful");

      // Switch to the e-commerce database
      console.log(`Switching to database: ${dbName}`);
      const useResponse = await this.sendCommand(`use ${dbName}`, 5000);
      console.log("Use database response:", useResponse);
      if (!useResponse.success) {
        throw new Error(`Failed to use database: ${useResponse.message}`);
      }
      this.currentDb = dbName;
      console.log(`Successfully switched to database: ${dbName}`);

      const createResponse = await this.sendCommand(
        `create users`,
        5000
      ).catch((err) => {
        console.log("Users collection already exists or creation failed:", err.message);
        return { success: true, message: "Collection creation skipped" };
      });
      console.log("Create collection response:", createResponse);
    } catch (error) {
      console.error("Connection setup failed:", error.message);
      if (this.client) {
        this.client.destroy();
        this.client = null;
      }
      throw error;
    }
  }

  // Send a command to KiteDB and return the parsed response with timeout
  async sendCommand(command, timeout = 5000) {
    if (!this.client) {
      throw new Error("Not connected to KiteDB server");
    }
    console.log(`Sending command: ${command}`);
    return new Promise((resolve, reject) => {
      let buffer = "";
      let timeoutId;

      const onData = (data) => {
        buffer += data.toString();
        console.log(`Received data: ${buffer}`);
        try {
          const response = JSON.parse(buffer);
          console.log("Parsed response:", response);
          clearTimeout(timeoutId);
          this.client.off("data", onData);
          this.client.off("error", onError);
          resolve({
            success: response.status === "success",
            message: response.message,
            data: response.data,
          });
        } catch (err) {
          console.log("Waiting for complete JSON response");
        }
      };

      const onError = (err) => {
        console.error("Error during command:", err.message);
        clearTimeout(timeoutId);
        this.client.off("data", onData);
        this.client.off("error", onError);
        reject(new Error(`Command failed: ${err.message}`));
      };

      timeoutId = setTimeout(() => {
        console.error(`Command timeout: ${command}`);
        this.client.off("data", onData);
        this.client.off("error", onError);
        reject(new Error(`Command '${command}' timed out after ${timeout}ms`));
      }, timeout);

      this.client.on("data", onData);
      this.client.on("error", onError);
      this.client.write(`${command}\n`);
    });
  }

  // Disconnect from KiteDB
  async disconnect() {
    if (this.client) {
      console.log("Disconnecting from KiteDB");
      await this.sendCommand("exit", 5000).catch((err) => {
        console.error("Error sending exit command:", err.message);
      });
      this.client.end();
      this.client = null;
      this.authenticated = false;
      this.currentDb = null;
      console.log("Disconnected from KiteDB");
    }
  }
}

// Singleton instance of KiteDB client
const kiteDBClient = new KiteDBClient();

const connectDB = async () => {
  console.log("Starting KiteDB connection process");
  try {
    await kiteDBClient.connect("admin", "admin", "e-commerce");
    console.log("KiteDB connection established");
  } catch (error) {
    console.error("Failed to connect to KiteDB:", error.message);
    throw error;
  }
};

export { connectDB, kiteDBClient };