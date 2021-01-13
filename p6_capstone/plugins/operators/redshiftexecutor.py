import logging

class RedShiftExecutor():
    def __init__(self, conn, queries):
        self.conn = conn
        self.queries = queries
        self.logger = logging.getLogger()

    def execute(self, context=None):
        cur = self.conn.cursor()
        for q in self.queries:
            self.logger.info(q.as_string(self.conn))
            cur.execute(q)
        return None
