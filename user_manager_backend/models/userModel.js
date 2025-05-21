// Define the user schema for KiteDB
const userSchema = {
  fields: {
    name: "str",
    username: "str",
    password: "str",
    street: "str",
    city: "str",
    zipcode: "str",
    country: "str",
  },
};

// Function to validate user data against the schema
const validateUser = (user) => {
  const requiredFields = ["name", "username", "password", "street", "city", "zipcode", "country"];
  for (const field of requiredFields) {
    if (!user[field]) {
      throw new Error(`Missing required field: ${field}`);
    }
    if (typeof user[field] !== "string") {
      throw new Error(`Field ${field} must be a string`);
    }
  }
};

export { userSchema, validateUser };