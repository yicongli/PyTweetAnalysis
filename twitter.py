import sys
import json
from mpi4py import MPI

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

# global variable for storing geolocation grids
g_grids = getGeoLocation() 

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
  # store the startpoint of each line into file
  with open('conf.json', 'w') as outfile:
    json.dump(dict_pos, outfile)
  return dict_pos

def initResultDic():
  """
  init the result dic with the grid dic
  This result dic is used to store all the info that collected from json data
  """
  resultDic = {}
  for key, value in g_grids.items():
    resultDic[key] = {'posN':0, 'hashtags':{}}
  
  return resultDic

def parseJsonDataWithConf(fileName, startLinePoint, size):
  """
  parse json data according to current arranged period
  """
  result = initResultDic()

  with open(fileName, 'r') as file:
    file.seek(startLinePoint) # jump to the start point
    for i, line in enumerate(file):
      # if larger than current size, then return directly
      if(i >= size): 
        return result
      # extract key info from data
      try:
          data = json.loads(line[:-2]) # TODO: May use regular formula to extract data
          extractInfoFromData(data, result)
      except json.decoder.JSONDecodeError:
        try:
          data = json.loads(line[:-1]) 
          extractInfoFromData(data, result)
        except json.decoder.JSONDecodeError:
          print('Error on line', i + 1, ':\n', repr(line))
  
  return result

def extractInfoFromData(data, resultDic):
  """
  This function is to extract the infomation from
  """
  try:
    dataCoord = data['doc']['coordinates']['coordinates']
  except TypeError:
    return
  
  if len(dataCoord) < 2:
    print('No coordinate')
    return

  for key, value in g_grids.items():
    # if the value in the specific area, then store the data into the result dic
    if (dataCoord[0] <= value['xmax'] and dataCoord[0] > value['xmin'] 
    and dataCoord[1] <= value['ymax'] and dataCoord[1] > value['ymin']):
      postNum = resultDic[key]['posN']
      resultDic[key]['posN'] = postNum + 1
      hashtags = data['doc']['entities']['hashtags']
      # if has hashtags, then record the hash dic
      if len(hashtags) > 0:
        for hashtag in hashtags:
          hashtagNum = 0
          try:
            hashtagNum = resultDic[key]['hashtags'][hashtag['text']]
          except KeyError:
            resultDic[key]['hashtags'][hashtag['text']] = hashtagNum
          finally:
            resultDic[key]['hashtags'][hashtag['text']] = hashtagNum + 1

def inputFileName():
  """
  get json file name
  """
  if len(sys.argv) != 2:
    print ("twitter.py <twitter_file.json>" )
  else:
    fileName = sys.argv[1]
    return fileName


def removeRedundantHashtags(resultDic):
  """
  remove the hashtags that has really small number in the result,
  only reserve the largest ten tags
  """
  for key, value in resultDic.items():
    hashtagDic = value['hashtags']
    orderedList = [(k, hashtagDic[k]) for k in sorted(hashtagDic, key=hashtagDic.get, reverse=True)]
    value['hashtags'] = dict(orderedList[0:])

def pretty(d, indent=0):
  """
  print pretty, only for testing
  """
  for key, value in d.items():
    print('\t' * indent + str(key))
    if isinstance(value, dict):
        pretty(value, indent+1)
    else:
        print('\t' * (indent+1) + str(value))

CONST_SIZE = 2500000  # the total line number of data is 2500000

if __name__ == "__main__":

  #num_lines = sum(1 for line in open('/Users/yicongli/Downloads/twitter-melb.json'))
  #print(num_lines)
  # for grid in grids:
  #   print (grids[grid])

  with open('conf.json') as json_data:
    listConf = json.load(json_data)
  
  comm = MPI.COMM_WORLD
  iterRange = int(CONST_SIZE / comm.size) # get the range of iteration
  startLineNum = comm.rank * iterRange + 1

  twitterFile = inputFileName()
  result = parseJsonDataWithConf(twitterFile,listConf[startLineNum], iterRange)

  #removeRedundantHashtags(result) # may need to remove the hashtag that have really small number
  #pretty(result) # print pretty, only for testing

