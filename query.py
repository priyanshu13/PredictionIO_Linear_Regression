# Script to test engine queries
import predictionio
engine_client = predictionio.EngineClient(url="http://localhost:8000")
print engine_client.send_query({"features" : [648600,1, 2011,1,5]})
