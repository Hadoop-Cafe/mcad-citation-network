import sqlite3 as db

class DB:

    def __init__(self):
        self.conn = db.connect('scrape.db')
        self.conn.text_factory = str
        self.cursor = self.conn.cursor()

    def insert(self, author_id, name='', country=''):
        q = 'INSERT INTO author (author_id, name, country) VALUES (?, ?, ?)'
        self.cursor.execute(
            q,
            [author_id,
             name,
             country])
        self.conn.commit()

    def exists(self, author_id):
        q = 'SELECT COUNT(*) FROM author WHERE author_id=?'
        self.cursor.execute(q, [author_id])
        num = self.cursor.fetchone()[0]
        if num == 0:
            return False
        else:
            return True

    def count(self, table):
        q = 'SELECT COUNT(*) FROM author'
        self.cursor.execute(q)
        num = self.cursor.fetchone()[0]
        return num
