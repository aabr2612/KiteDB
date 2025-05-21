// Connect to KiteDB
const connectDB = async () => {
  mongoose.connection.on("connected", () => {
    console.log("connected");
  });
  await mongoose.connect(`${process.env.KITEDB_URI}/e-commerce`);
};
export default connectDB;
