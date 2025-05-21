import { useState } from 'react'
import axios from 'axios'
import { backendUrl } from '../App'
import { toast } from 'react-toastify'
import { useNavigate } from 'react-router-dom'

const Add = () => {
  const [user, setUser] = useState({
    name: '',
    username: '',
    password: '',
    street: '',
    city: '',
    zipcode: '',
    country: ''
  })

  const navigate = useNavigate()

  const onSubmitHandler = async (e) => {
    e.preventDefault()
    try {
      const response = await axios.post(`${backendUrl}/api/user/add`, user)
      if (response.data.success) {
        toast.success('User added successfully', { position: 'top-right' })
        setUser({
          name: '',
          username: '',
          password: '',
          street: '',
          city: '',
          zipcode: '',
          country: ''
        })
        navigate('/list')
      } else {
        toast.error(response.data.message, { position: 'top-right' })
      }
    } catch (error) {
      toast.error(error.message, { position: 'top-right' })
    }
  }

  const handleInputChange = (e) => {
    const { name, value } = e.target
    setUser(prev => ({ ...prev, [name]: value }))
  }

  return (
    <div className="bg-white p-6 rounded-lg shadow-md max-w-2xl mx-auto">
      <h2 className="text-xl font-extrabold text-gray-800 mb-4 text-center">Add New User</h2>
      <form onSubmit={onSubmitHandler} className="flex flex-col gap-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Name</label>
          <input
            name="name"
            value={user.name}
            onChange={handleInputChange}
            className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            type="text"
            placeholder="Enter name"
            required
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Username</label>
          <input
            name="username"
            value={user.username}
            onChange={handleInputChange}
            className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            type="text"
            placeholder="Enter username"
            required
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
          <input
            name="password"
            value={user.password}
            onChange={handleInputChange}
            className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            type="password"
            placeholder="Enter password"
            required
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Street</label>
          <input
            name="street"
            value={user.street}
            onChange={handleInputChange}
            className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            type="text"
            placeholder="Enter street"
            required
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">City</label>
          <input
            name="city"
            value={user.city}
            onChange={handleInputChange}
            className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            type="text"
            placeholder="Enter city"
            required
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Zip Code</label>
          <input
            name="zipcode"
            value={user.zipcode}
            onChange={handleInputChange}
            className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            type="text"
            placeholder="Enter zip code"
            required
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Country</label>
          <input
            name="country"
            value={user.country}
            onChange={handleInputChange}
            className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            type="text"
            placeholder="Enter country"
            required
          />
        </div>
        <button
          className="w-32 py-2 mt-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors"
          type="submit"
        >
          Add User
        </button>
      </form>
    </div>
  )
}

export default Add