import socket
import json

class KiteDBClient:
    """Simple client to interact with the KiteDB server."""
    def __init__(self, host: str = "localhost", port: int = 5432):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    def send_command(self, command: str) -> dict:
        """Send a command to the server and return the response."""
        self.sock.sendall(command.encode('utf-8') + b'\n')
        response = self.sock.recv(4096).decode('utf-8')
        return json.loads(response)

    def close(self):
        """Close the client connection."""
        self.sock.close()

if __name__ == "__main__":
    client = KiteDBClient()
    try:
        # Login
        print(client.send_command("login admin admin"))

        # Use database 'b'
        print(client.send_command("use b"))

        # Drop existing users collection to start fresh
        print(client.send_command("delete users"))

        # Create users collection without schema (assuming flexible structure)
        print(client.send_command("create users"))

        # Begin transaction for ADD
        print(client.send_command("begin"))

        # === ADD Commands ===
        print(client.send_command('users.add{"name":"Alice Smith","age":28,"class":1,"address":{"city":"San Francisco","zip":94105}}'))
        print(client.send_command('users.add{[{"name":"Bob Johnson","age":34,"class":2,"hobbies":["reading","hiking"]},{"name":"Clara Lee","age":25,"class":3,"hobbies":["reading","hiking"]}]}'))
        print(client.send_command('users.add{"name":"One","age":20,"class":1,"address":{"city":"New York","zip":20}}}'))

        # Commit the adds
        print(client.send_command("commit"))

        # === FIND Commands ===
        print(client.send_command('users.find{}'))  # Find all
        print(client.send_command('users.find{"name":"Alice Smith"}'))  # Find by single field
        print(client.send_command('users.find{"name":"One","age":20}'))  # Find by multiple fields
        print(client.send_command('users.find{"address.city":"San Francisco"}'))  # Find with nested field
        print(client.send_command('users.find{"age":{"$gt":25}}'))  # Find with comparison
        print(client.send_command('users.find{"$or":[{"name":"Bob Johnson"},{"age":{"$gte":30}}]}'))  # Find with $or
        print(client.send_command('users.find{"$and":[{"class":1},{"address.zip":20}]}'))  # Find with $and
        print(client.send_command('users.find{"$not":{"name":"Clara Lee"}}'))  # Find with $not

        # Begin transaction for UPDATE
        print(client.send_command("begin"))

        # === UPDATE Commands ===
        print(client.send_command('users.update{"name":"Alice Smith"},{"age":29,"status":"active"}'))
        print(client.send_command('users.update{"name":"One"},{"age":21,"class":2,"role":"student"}'))
        print(client.send_command('users.update{"name":"One"},{"address":{"city":"New York","zip":21}}'))
        print(client.send_command('users.update{"age":{"$gte":30}},{"role":"senior","scores":{"math":5}}'))

        # Commit the updates
        print(client.send_command("commit"))

        # Begin transaction for DELETE
        print(client.send_command("begin"))

        # === DELETE Commands ===
        print(client.send_command('users.delete{"name":"Clara Lee"}'))  # Delete by single field
        print(client.send_command('users.delete{"name":"One","age":21}'))  # Delete by multiple fields
        print(client.send_command('users.delete{"address.city":"San Francisco"}'))  # Delete with nested field
        print(client.send_command('users.delete{"$and":[{"class":2},{"status":"inactive"}]}'))  # Delete with logical operator
        print(client.send_command('users.delete{}'))  # Delete all

        # Commit the deletes
        print(client.send_command("commit"))

        # Exit
        print(client.send_command("exit"))
    finally:
        client.close()