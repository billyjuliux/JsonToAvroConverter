import os
import json
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.parse(open("../data/avro.avsc", "rb").read())
writer = DataFileWriter(open("../output/data.avro", "wb"), DatumWriter(), schema)

# convert facebook data
files = os.listdir('../data/facebook_data')
for file in files :
  with open(('../data/facebook_data/' + file), 'r', encoding="utf8") as json_file:
    data = json.load(json_file)
    for elmt in data :
      writer.append({
        "original_id": str(elmt["shares"]["count"]), 
        "content": elmt["message"], 
        "from_id": elmt["from"]["id"], 
        "from_name": elmt["from"]["name"],
        "created_at": elmt["created_time"],
        "social_media": "facebook", 
      })

# convert youtube data
files = os.listdir('../data/youtube_data')
for file in files :
  with open(('../data/youtube_data/' + file), 'r', encoding="utf8") as json_file:
    data = json.load(json_file)
    for elmt in data :
      writer.append({
        "original_id": elmt["id"], 
        "content": elmt["snippet"]["description"], 
        "from_id": elmt["snippet"]["channelId"], 
        "from_name": elmt["snippet"]["channelTitle"],
        "created_at": elmt["snippet"]["publishedAt"],
        "social_media": "youtube", 
      })

writer.close()

# this is to check
reader = DataFileReader(open("../output/data.avro", "rb"), DatumReader())
for datum in reader:
    print(datum)
reader.close()