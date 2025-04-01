"""
Test the persistence functionality in FakeRedis
"""
import os
import tempfile
import shutil
import time
import uuid

import fakeredis
import pytest


class TestPersistence:
    """Test persistence functionality"""

    def setup_method(self):
        """Set up the test environment"""
        self.temp_dir = tempfile.mkdtemp()
        # Configure storage with the test's temp directory
        fakeredis.configure_storage(self.temp_dir)
    
    def teardown_method(self):
        """Clean up the test environment"""
        shutil.rmtree(self.temp_dir)
    
    def test_basic_persistence(self):
        """Test that data persists between Redis instances"""
        # Create a unique key for this test
        test_key = f"test:{uuid.uuid4()}"
        
        # First connection writes data
        conn1 = fakeredis.FakeRedis(persistence_enabled=True, storage_dir=self.temp_dir)
        conn1.set(test_key, "value1")
        conn1.set("counter", "1")
        
        # Second connection should see the data
        conn2 = fakeredis.FakeRedis(persistence_enabled=True, storage_dir=self.temp_dir)
        assert conn2.get(test_key) == b"value1"
        assert conn2.get("counter") == b"1"
        
        # Modify data in second connection
        conn2.set(test_key, "value2")
        conn2.incr("counter")
        
        # First connection should see the changes
        assert conn1.get(test_key) == b"value2"
        assert conn1.get("counter") == b"2"
    
    def test_persistence_between_startups(self):
        """Test that data persists after closing and reopening connection"""
        # Set some data
        conn1 = fakeredis.FakeRedis(persistence_enabled=True, storage_dir=self.temp_dir)
        conn1.set("persistent_key", "persist_value")
        conn1.rpush("list_key", "item1", "item2")
        conn1.hset("hash_key", "field1", "value1")
        
        # Close the connection
        conn1.close()
        
        # Simulate application restart with new connection
        conn2 = fakeredis.FakeRedis(persistence_enabled=True, storage_dir=self.temp_dir)
        
        # Verify data persisted
        assert conn2.get("persistent_key") == b"persist_value"
        assert conn2.lrange("list_key", 0, -1) == [b"item1", b"item2"]
        assert conn2.hget("hash_key", "field1") == b"value1"
    
    def test_persistence_directory_structure(self):
        """Test the creation of persistence directory structure"""
        # Create a connection that will generate files
        conn = fakeredis.FakeRedis(persistence_enabled=True, storage_dir=self.temp_dir)
        # Explicitly create and use db 0
        conn.select(0)
        conn.set("test_key", "test_value")
        conn.close()
        
        # Check if files were created in the storage directory
        server_dirs = os.listdir(self.temp_dir)
        assert len(server_dirs) >= 1
        
        # Check if database files exist
        db_files = os.listdir(os.path.join(self.temp_dir, server_dirs[0]))
        assert "db0.json" in db_files
    
    def test_different_dbs(self):
        """Test persistence across different databases"""
        conn = fakeredis.FakeRedis(persistence_enabled=True, storage_dir=self.temp_dir)
        
        # Set data in DB 0
        conn.select(0)
        conn.set("db0_key", "db0_value")
        
        # Set data in DB 1
        conn.select(1)
        conn.set("db1_key", "db1_value")
        
        # Set data in DB 2
        conn.select(2)
        conn.set("db2_key", "db2_value")
        
        # Force persistence by closing the connection
        conn.close()
        
        # Create a new connection to verify data
        conn = fakeredis.FakeRedis(persistence_enabled=True, storage_dir=self.temp_dir)
        
        # Verify data in each DB
        conn.select(0)
        assert conn.get("db0_key") == b"db0_value"
        assert conn.get("db1_key") is None
        
        conn.select(1)
        assert conn.get("db1_key") == b"db1_value"
        assert conn.get("db0_key") is None
        
        conn.select(2)
        assert conn.get("db2_key") == b"db2_value"
        assert conn.get("db0_key") is None
        
        # Check that files exist for all DBs
        server_dirs = os.listdir(self.temp_dir)
        db_files = os.listdir(os.path.join(self.temp_dir, server_dirs[0]))
        assert "db0.json" in db_files
        assert "db1.json" in db_files
        assert "db2.json" in db_files 