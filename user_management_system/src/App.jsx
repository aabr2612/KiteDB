import { useState } from 'react'
import Navbar from './components/Navbar'
import Sidebar from './components/Sidebar'
import { Route, Routes } from 'react-router-dom'
import Add from './pages/Add'
import List from './pages/List'
import Update from './pages/Update'
import { ToastContainer } from 'react-toastify'
import 'react-toastify/dist/ReactToastify.css'

export const backendUrl = import.meta.env.VITE_BACKEND_URL || 'http://localhost:4000'

const App = () => {
  const [isSidebarOpen, setIsSidebarOpen] = useState(window.innerWidth >= 1024)

  const toggleSidebar = () => {
    setIsSidebarOpen(prev => !prev)
  }

  return (
    <div className="bg-gray-50 min-h-screen">
      <ToastContainer position="top-right" autoClose={3000} />
      <Navbar toggleSidebar={toggleSidebar} />
      <div className="flex">
        <Sidebar isOpen={isSidebarOpen} toggleSidebar={toggleSidebar} />
        <main className="flex-1 w-full pt-16 lg:pl-64 min-h-screen overflow-y-auto">
          <div className="max-w-4xl mx-auto my-8 px-4 text-gray-600 text-base">
            <Routes>
              <Route path="/add" element={<Add />} />
              <Route path="/list" element={<List />} />
              <Route path="/update/:username" element={<Update />} />
            </Routes>
          </div>
        </main>
      </div>
    </div>
  )
}

export default App