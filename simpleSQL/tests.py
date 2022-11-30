import os
import unittest
import simpleSQL


class SampleTable:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class TestSimpleSQL(unittest.TestCase):

    def tearDown(self) -> None:
        for o in os.listdir():
            if o.endswith(".db"):
                os.remove(o)

    def setUp(self) -> None:
        ...

    def test_create_db(self):
        with simpleSQL.connect(serverless=True, database="test1.db") as db:
            assert "test1.db" in db.local_databases()

    def test_create_table(self):

        with simpleSQL.connect(serverless=True, database="mydb.db") as db:
            data = SampleTable(
                db.types.column(db.types.integer(), auto_increment=True)
                , db.types.column(db.types.varchar(50))
            )
            obj = SampleTable(db.AUTO_INC, "tal")
            db.create_table(SampleTable, data, primary_key="id",auto_increment_value=100,ondelete="")
            db.insert_to(SampleTable, obj)
            obj1 = db.query_filter_by(SampleTable, "id", 1, first=True)
            assert 1 == obj1.id and obj.name == obj1.name
