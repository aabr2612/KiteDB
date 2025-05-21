import { useState, useEffect } from 'react'
import axios from 'axios'
import { backendUrl } from '../App'
import { toast } from 'react-toastify'
import { useNavigate, useParams } from 'react-router-dom'

const Update = () => {
  const { username } = useParams()
  const navigate = useNavigate()
  const [user, setUser] = useState({
    name: '',
    username: '',
    password: '',
    street: '',
    city: '',
    zipcode: '',
    country: ''
  })

  const fetchUser = async () => {
    try {
      const response = await axios.get(`${backendUrl}/api/user/${username}`)
      if (response.data.success) {
        setUser(response.data.user)
      } else {
        const msg =
          error.response?.data?.message || 'Something went wrong!'
        toast.error(msg, { position: 'top-right' })
      }
    } catch (error) {
      const msg =
        error.response?.data?.message || 'Something went wrong!'
      toast.error(msg, { position: 'top-right' })
    }
  }

  const onSubmitHandler = async (e) => {
    e.preventDefault()
    try {
      const response = await axios.post(`${backendUrl}/api/user/update`, { username, ...user })
      if (response.data.success) {
        toast.success('User updated successfully', { position: 'top-right' })
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

  useEffect(() => {
    fetchUser()
  }, [username])

  return (
    <div className="bg-white p-6 rounded-lg shadow-md max-w-2xl mx-auto">
      <h2 className="text-xl font-extrabold text-gray-800 mb-4 text-center">Update User</h2>
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
            className="w-full px-4 py-2 border border-gray-300 rounded-md bg-gray-100 cursor-not-allowed"
            type="text"
            disabled
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
          <label className="block text-sm font-medium text-gray-700 mb-1">street</label>
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
          Update User
        </button>
      </form>
    </div>
  )
}

export default Update