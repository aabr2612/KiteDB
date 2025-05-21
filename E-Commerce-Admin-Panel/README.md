# Admin Panel

A React-based admin panel for managing products and orders in an e-commerce system. This application allows administrators to add products, view and manage product listings, and track order statuses with a clean, responsive user interface. Built with modern tools like Vite, Tailwind CSS, and React Router, it integrates with a backend API for seamless data management.

## Table of Contents
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [Contact](#contact)

## Features

- **Product Management**:
  - Add new products with details like name, description, price, category, sizes, and up to four images.
  - List all products with their images, names, categories, and prices.
  - Remove products from the inventory.
- **Order Management**:
  - View all orders with customer details, items, payment status, and delivery status.
  - Update order status (e.g., Order Placed, Packing, Shipped, Delivered).
- **User Authentication**:
  - Secure login system for admin access using email and password.
  - Token-based authentication with local storage persistence.
- **UI/UX**:
  - Responsive design with Tailwind CSS.
  - Toast notifications for success and error feedback using `react-toastify`.
  - Sidebar navigation for easy access to Add, List, and Orders sections.

## Tech Stack

- **Frontend**: React.js (v18.3.1)
- **Build Tool**: Vite (v6.0.5) for fast development and HMR
- **Routing**: React Router DOM (v7.1.1)
- **Styling**: Tailwind CSS (v3.4.17) with custom fonts (Outfit)
- **HTTP Requests**: Axios (v1.7.9)
- **Notifications**: React Toastify (v11.0.2)
- **Linting**: ESLint (v9.17.0) with React-specific plugins
- **Backend Integration**: Connects to a custom API (URL configured via environment variable)

## Prerequisites

- Node.js (v18 or higher)
- npm (v9 or higher)
- A backend API server (configured via `VITE_BACKEND_URL` in `.env`)

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/aabr2612/e-commerce-admin-panel.git
   cd admin-panel
   ```
2. **Install Dependencies**:
   ```bash
   npm install
   ```
3. **Set up development server**:
   Create a .env file in the root directory and add the backend URL:
   ```bash
   VITE_BACKEND_URL=http://your-backend-api-url
   ```
4. **Run the Development Server**:
   ```bash
   npm run dev
   ```

## Usage

1. **Login**:
   - Open the app and log in using admin credentials (email and password) provided by your backend API.
   - Upon successful login, you'll be redirected to the admin dashboard.

2. **Add Products**:
   - Navigate to the "Add Items" section via the sidebar.
   - Fill in product details, upload images, and submit to add a new product.

3. **Manage Products**:
   - Go to the "List Items" section to view all products.
   - Click the "X" button next to a product to remove it.

4. **Track Orders**:
   - Visit the "Order Items" section to see all orders.
   - Update the status of an order using the dropdown menu.

## Project Structure
    e-commerce-admin-panel/
      ├── public/              # Static assets (e.g., logo.png)
      ├── src/
      │   ├── assets/          # Images and other static resources
      │   ├── components/      # Reusable components (Navbar, Sidebar, Login)
      │   ├── pages/           # Main views (Add, List, Orders)
      │   ├── App.jsx          # Root component with routing
      │   ├── index.css        # Global styles with Tailwind
      │   └── main.jsx         # Entry point with React DOM
      ├── .env                 # Environment variables (not tracked)
      ├── package.json         # Dependencies and scripts
      ├── vite.config.js       # Vite configuration
      ├── tailwind.config.js   # Tailwind CSS configuration
      ├── eslint.config.js     # ESLint configuration
      └── README.md            # Project documentation
      
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
