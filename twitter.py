import sys
import json
from itertools import islice

def inputFileName():
  """
  get json file name
  """
  if len(sys.argv) != 2:
    print ("twitter.py <twitter_file.json>" )
    # what will return here?
  else:
    fileName = sys.argv[1]
    return fileName

def getFileLinePoint(fileName):
  """
  get the line start point, only run in testing
  """
  dict_pos = []
  length = 0

  with open(fileName,'rb') as file:
    for each in file:
        dict_pos.append(length)
        length += len(each)

  with open('conf.json', 'w') as outfile:
    json.dump(dict_pos, outfile)
  return dict_pos

def parseJsonDataWithConf(fileName, startLineNum, size):
  """
  parse json data according to current arranged period
  """
  with open(fileName, 'r') as file:
    file.seek(startLineNum) # jump to the start point
    for i, line in enumerate(file):
      # if larger than current size, then return directly
      if(i >= size): 
        return

      try:
          data = json.loads(line[:-2]) 
          extractInfoFromData(data)
      except json.decoder.JSONDecodeError:
        try:
          data = json.loads(line[:-1]) 
          extractInfoFromData(data)
        except json.decoder.JSONDecodeError:
          print('Error on line', i + 1, ':\n', repr(line))

def extractInfoFromData(data):
  """
  This function is to extract the infomation from
  """
  pass

def getGeoLocation():
  """
  Get the range of x and y for each grid box
  """
  melbGridFile = open('melbGrid.json', 'r')
  data = melbGridFile.read()
  melbGrid = json.loads(data)

  grids = {}
  # grids is a dictionary in which:
  # - key is the id of grid box: A1, A2, ...
  # - value is the x-range and y-range

  for feature in melbGrid["features"]:
    coordinates = {"xmin": feature["properties"]["xmin"], "xmax": feature["properties"]["xmax"], "ymin": feature["properties"]["ymin"], "ymax": feature["properties"]["ymax"]}
    grids[feature["properties"]["id"]] = coordinates
  
  return grids


from mpi4py import MPI
CONST_SIZE = 2500000  # the total line number of data is 2500000

if __name__ == "__main__":
  twitterFile = inputFileName()
  grids = getGeoLocation()   

  #num_lines = sum(1 for line in open('/Users/yicongli/Downloads/twitter-melb.json'))
  #print(num_lines)
  # for grid in grids:
  #   print (grids[grid])

  with open('conf.json') as json_data:
    listConf = json.load(json_data)
  
  #comm = MPI.COMM_WORLD
  #iterRange = CONST_SIZE / comm.size
  # Test code
  parseJsonDataWithConf('/Users/yicongli/Downloads/twitter-melb.json',listConf[0],CONST_SIZE / 4)

