const Navbar = ({ toggleSidebar }) => {
  return (
    <nav className="fixed top-0 left-0 w-full bg-gradient-to-r from-blue-600 to-indigo-600 text-white py-4 px-4 sm:px-6 lg:px-8 shadow-lg flex justify-between items-center z-30">
      <h1 className="text-2xl font-bold tracking-tight">User Management System</h1>
      <button
        className="lg:hidden p-2 rounded-md hover:bg-blue-700 transition-colors"
        onClick={toggleSidebar}
      >
        <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M4 6h16M4 12h16M4 18h16" />
        </svg>
      </button>
    </nav>
  )
}

export default Navbar