import { useState, useEffect } from 'react'
import axios from 'axios'
import { backendUrl } from '../App'
import { toast } from 'react-toastify'
import { useNavigate } from 'react-router-dom'

const List = () => {
  const [users, setUsers] = useState([])
  const navigate = useNavigate()

  const fetchUsers = async () => {
    try {
      const response = await axios.get(`${backendUrl}/api/user/list`)
      if (response.data.success) {
        setUsers(response.data.users)
      } else {
        toast.error(response.data.message, { position: 'top-right' })
      }
    } catch (error) {
      toast.error(error.message, { position: 'top-right' })
    }
  }

  const removeUser = async (username) => {
    try {
      const response = await axios.post(`${backendUrl}/api/user/remove`, { username })
      if (response.data.success) {
        toast.success('User removed successfully', { position: 'top-right' })
        await fetchUsers()
      } else {
        toast.error(response.data.message, { position: 'top-right' })
      }
    } catch (error) {
      toast.error(error.message, { position: 'top-right' })
    }
  }

  useEffect(() => {
    fetchUsers()
  }, [])

  return (
    <div className="bg-white p-6 rounded-lg shadow-md max-w-4xl mx-auto">
      <h2 className="text-xl font-extrabold text-gray-800 mb-4 text-center">All Users</h2>
      {users.length === 0 ? (
        <p className="text-gray-500">No users found. Add some users to get started!</p>
      ) : (
        <div className="overflow-x-auto">
          <div className="min-w-full border border-gray-200 rounded-md">
            <div className="hidden md:grid grid-cols-[1fr_1fr_1fr_1fr_1fr_100px] gap-2 p-3 bg-gray-50 text-sm font-semibold text-gray-700 border-b">
              <span>Name</span>
              <span>Username</span>
              <span>street</span>
              <span>City</span>
              <span>Country</span>
              <span>Actions</span>
            </div>
            {users.map((user) => (
              <div
                key={user.username}
                className="grid grid-cols-1 md:grid-cols-[1fr_1fr_1fr_1fr_1fr_100px] gap-2 p-3 border-b text-sm hover:bg-gray-50 transition-colors"
              >
                <div className="md:hidden font-semibold">Name:</div>
                <div>{user.name}</div>
                <div className="md:hidden font-semibold">Username:</div>
                <div>{user.username}</div>
                <div className="md:hidden font-semibold">street:</div>
                <div>{user.street}</div>
                <div className="md:hidden font-semibold">City:</div>
                <div>{user.city}</div>
                <div className="md:hidden font-semibold">Country:</div>
                <div>{user.country}</div>
                <div className="md:hidden font-semibold">Actions:</div>
                <div className="flex gap-2">
                  <button
                    onClick={() => navigate(`/update/${user.username}`)}
                    className="text-blue-600 hover:text-blue-800 font-medium"
                  >
                    Edit
                  </button>
                  <button
                    onClick={() => removeUser(user.username)}
                    className="text-red-600 hover:text-red-800 font-medium"
                  >
                    Delete
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

export default List