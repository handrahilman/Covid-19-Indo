from sqlalchemy import create_engine


class MySQL:
  def __init__(self, cfg):
    self.host = cfg['host']
    self.port = cfg['port']
    self.username = cfg['username']
    self.password = cfg['password']
    self.database = cfg['database']

  def connect(self):
    engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(self.username, self.password, self.host, self.port, self.database))
    engine_conn = engine.connect()
    print("Connect Engine MySQL")
    return engine, engine_conn