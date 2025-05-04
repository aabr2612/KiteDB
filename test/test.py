import unittest
from src.core.database import Database
from src.storage.storage_engine import StorageEngine

class TestDatabase(unittest.TestCase):
    def setUp(self):
        self.storage = StorageEngine("test_db/data")
        self.db = Database("test_db", self.storage)

    def test_create_collection(self):
        self.db.create_collection("test_coll")
        self.assertIn("test_coll", self.db.collections)

    def test_insert_and_find(self):
        coll = self.db.get_collection("test_coll")
        coll.insert({"id": 1, "value": 10})
        self.assertEqual(len(coll.find({"value": 10})), 1)

    def tearDown(self):
        import shutil
        shutil.rmtree("test_db", ignore_errors=True)

if __name__ == "__main__":
    unittest.main()