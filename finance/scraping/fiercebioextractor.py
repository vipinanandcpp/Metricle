import logging, importio, threading, json

# We define a latch class as python doesn't have a counting latch built in
class _Latch(object):
  def __init__(self, count=1):
    self.count = count
    self.lock = threading.Condition()

  def countDown(self):
    with self.lock:
      self.count -= 1

      if self.count <= 0:
        self.lock.notifyAll()

  def await(self):
    with self.lock:
      while self.count > 0:
        self.lock.wait()

logging.basicConfig(level=logging.INFO)

# Initialise the library
client = importio.ImportIO(host="https://query.import.io", userId="24603aad-24d6-485f-bd50-940fc313ccfe", apiKey="5gKUPEm9dkn0HZgwOCN2/KUeqzf6P4WMAuZLsP0RmfvDCEiLOem64061tMspYwvoPfdLnZj8v4BGztMGXKuhoQ==")
client.connect()

# Use a latch to stop the program from exiting
latch = _Latch(0)

def callback(query, message):
    
  if message["type"] == "MESSAGE": 
    print "Got data!"
    print json.dumps(message["data"],indent = 4)
        
  if query.finished(): latch.countDown()



# Wait until queries complete
latch.await()

client.disconnect()