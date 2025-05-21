# E-Commerce Backend

A robust backend for an e-commerce panel, built with Node.js and Express.js to manage products, orders, user authentication, and cart functionality. This API supports an admin panel frontend and frontend panel, enabling administrators to handle inventory, process orders, and manage user data securely. It integrates with MongoDB for data storage, Cloudinary for image uploads, and payment gateways like Stripe and Razorpay for seamless transactions.

## Table of Contents
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [API Endpoints](#api-endpoints)
- [Contributing](#contributing)
- [Contact](#contact)

## Features

- **Product Management**:
  - Add products with details (name, description, price, category, subcategory, sizes, bestseller status) and up to four images via Cloudinary.
  - List all products or fetch a single product by ID.
  - Remove products from the inventory with admin authentication.
- **Order Management**:
  - Support for placing orders via Cash on Delivery (COD), Stripe, or Razorpay.
  - View all orders (admin) or user-specific orders.
  - Update order statuses (e.g., Order Placed, Shipped, Delivered) by admins.
  - Verify Stripe payments and clear user cart upon successful transactions.
- **Cart Management**:
  - Add items to a user's cart with size selection and quantity updates.
  - Retrieve and update cart data for authenticated users.
- **User Authentication**:
  - User registration and login with JWT-based authentication and bcrypt password hashing.
  - Admin login with hardcoded credentials for secure access.
  - Middleware for user and admin authorization on protected routes.
- **Integrations**:
  - MongoDB for persistent storage of users, products, and orders.
  - Cloudinary for image uploads and management.
  - Stripe and Razorpay for payment processing.

## Tech Stack

- **Runtime**: Node.js (v18.x or higher)
- **Framework**: Express.js (v4.21.1)
- **Database**: MongoDB with Mongoose (v8.8.3)
- **Authentication**: JSON Web Tokens (jsonwebtoken v9.0.2), bcrypt (v5.1.1)
- **File Uploads**: Multer (v1.4.5-lts.1), Cloudinary (v2.5.1)
- **Payments**: Stripe (v17.4.0), Razorpay (v2.9.5)
- **Environment**: dotenv (v16.4.5)
- **CORS**: cors (v2.8.5)
- **Development**: Nodemon (v3.1.7) for auto-restart
- **Validation**: validator (v13.12.0)

## Prerequisites

- Node.js (v18 or higher)
- npm (v9 or higher)
- MongoDB Atlas account or local MongoDB instance
- Cloudinary account for image storage
- Stripe account with a secret key
- Razorpay account (optional, as implementation is incomplete)
- Environment variables configured in a `.env` file

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/aabr2612/e-commerce-backend.git
   cd e-commerce-backend
   
2. **Install Dependencies**:
   ```bash
   npm install
      
3. **Set Up Environment Variables**:

   Create a `.env` file in the root directory with the following:
   ```bash
   PORT=4000
   MONGODB_URI=your-mongodb-connection-string
   CLOUDINARY_NAME=your-cloudinary-cloud-name
   CLOUDINARY_API_KEY=your-cloudinary-api-key
   CLOUDINARY_SECRET_KEY=your-cloudinary-secret-key
   JWT_SECRET=your-jwt-secret
   ADMIN_EMAIL=your-admin-email
   ADMIN_PASSWORD=your-admin-password
   STRIPE_SECRET_KEY=your-stripe-secret-key
   RAZORPAY_KEY_ID=your-razorpay-key-id
   RAZORPAY_KEY_SECRET=your-razorpay-key-secret

4. **Run Development Server**:
   ```bash
   npm run server
   ```
   The server will start on http://localhost:4000 (or the port specified in .env).
   
## Usage

1. **Start the Server**:
   - Use `npm run server` for development with Nodemon or `npm start` for production.
   - The root endpoint (`/`) returns "API Working" to confirm the server is running.

2. **Admin Operations**:
   - Log in as an admin via `/api/user/admin` to obtain a JWT token.
   - Use the token to add/remove products (`/api/product/add`, `/api/product/remove`) or update order statuses (`/api/order/status`).

3. **User Operations**:
   - Register or log in via `/api/user/register` or `/api/user/login` to get a user token.
   - Manage cart (`/api/cart/add`, `/api/cart/update`, `/api/cart/get`) and place orders (`/api/order/place`, `/api/order/stripe`).

4. **Testing APIs**:
   - Use tools like Postman or Thunder Client to test endpoints.
   - Ensure the `Authorization` header includes the JWT token (`Bearer <token>`) for protected routes.

## Project Structure
```bash
e-commerce-admin-backend/
    ├── config/
    │   ├── cloudinary.js       # Cloudinary configuration
    │   └── mongodb.js          # MongoDB connection setup
    ├── controllers/
    │   ├── cartController.js   # Cart management logic
    │   ├── orderController.js  # Order placement and status updates
    │   ├── productController.js# Product CRUD operations
    │   └── userController.js   # User and admin authentication
    ├── middleware/
    │   ├── adminAuth.js        # Admin authorization middleware
    │   ├── auth.js             # User authorization middleware
    │   └── multer.js           # File upload configuration
    ├── models/
    │   ├── orderModel.js       # Mongoose schema for orders
    │   ├── productModel.js     # Mongoose schema for products
    │   └── userModel.js        # Mongoose schema for users
    ├── routes/
    │   ├── cartRouter.js       # Cart-related API routes
    │   ├── orderRouter.js      # Order-related API routes
    │   ├── productRouter.js    # Product-related API routes
    │   └── userRouter.js       # User and admin authentication routes
    ├── .env                    # Environment variables (git-ignored)
    ├── package.json            # Dependencies and scripts
    ├── server.js               # Main server entry point
    └── README.md               # Project documentation

## API Endpoints

### User Routes (`/api/user`)
- POST /register: Register a new user (email, password, name).
- POST /login: Log in a user and return a JWT token.
- POST /admin: Admin login with hardcoded credentials.

### Product Routes (`/api/product`)
- POST /add (Admin): Add a product with images (multipart/form-data).
- POST /remove (Admin): Delete a product by ID.
- POST /single: Get a single product by ID.
- GET /list: List all products.

### Cart Routes (`/api/cart`)
- POST /add (User): Add an item to the cart (itemId, size).
- POST /update (User): Update cart item quantity.
- POST /get (User): Retrieve user's cart data.

### Order Routes (`/api/order`)
- POST /place (User): Place an order with COD.
- POST /stripe (User): Place an order with Stripe payment.
- POST /verifyStripe (User): Verify Stripe payment and update order.
- POST /userorders (User): Get orders for a specific user.
- POST /list (Admin): List all orders.
- POST /status (Admin): Update an order's status.

## Contributing
  1. Fork the repository.
  2. Create a new branch (`git checkout -b feature/your-feature`).
  3. Make your changes and commit (`git commit -m "Add your feature"`).
  4. Push to your branch (`git push origin feature/your-feature`).
  5. Open a Pull Request.

## Contact

For questions or feedback, feel free to reach out:
- GitHub: [aabr2612](https://github.com/aabr2612)
- Email: aabr2612@gmail.com
