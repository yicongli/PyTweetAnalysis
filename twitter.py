import sys
import json
from mpi4py import MPI
from operator import itemgetter

class CaseInsensitiveDict(dict):
  """
  the dict which is case-insensitive
  """
  def __setitem__(self, key, value):
      super(CaseInsensitiveDict, self).__setitem__(key.lower(), value)

  def __getitem__(self, key):
      return super(CaseInsensitiveDict, self).__getitem__(key.lower())

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
    resultDic[key] = {'posN':0, 'hashtags':CaseInsensitiveDict()}
  
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
    value['hashtags'] = dict(orderedList)#[:20])

def handlingAllData(dataList):
  """
  combine all data in the list together and return data list
  """
  result = initResultDic()
  for data in dataList:
    for key, value in data.items():
      totalNum = result[key]['posN']
      result[key]['posN'] = value['posN'] + totalNum

      hashtags = value['hashtags']
      for hashtag, value in hashtags.items():
          hashtagNum = 0
          try:
            hashtagNum = result[key]['hashtags'][hashtag]
          except KeyError:
            result[key]['hashtags'][hashtag] = hashtagNum
          finally:
            result[key]['hashtags'][hashtag] = hashtagNum + value

  return result

def orderTheResultIntoList(resultDic):
  """
  Sort the Area by post number, sort the hash tags of each area by number 
  """
  for key, value in resultDic.items():
    hashtagDic = value['hashtags']
    hashList = [(k, hashtagDic[k]) for k in sorted(hashtagDic, key=hashtagDic.get, reverse=True)]
    value['hashtags'] = hashList[:5]

  resultList = [(k, resultDic[k]) for k in sorted(resultDic, key=lambda x: resultDic[x]['posN'], reverse=True)]
  return resultList


def prettyPrint(resultList):
  #print pretty
  for value in resultList:
    print('%s: %d posts, (' % (value[0], value[1]['posN']), end='')
    for hashtag in value[1]['hashtags']:
      print('%s ' % (hashtag, ), end='')
    print(')')

CONST_SIZE = 2500000  # the total line number of data is 2500000

if __name__ == "__main__":
  #load configuration of big json data
  with open('conf.json') as json_data:
    listConf = json.load(json_data)
  
  comm = MPI.COMM_WORLD
  iterRange = int(CONST_SIZE / comm.size) # get the range of iteration
  startLineNum = comm.rank * iterRange + 1

  twitterFile = inputFileName()
  result = parseJsonDataWithConf(twitterFile,listConf[startLineNum], iterRange)
  removeRedundantHashtags(result) # may need to remove the hashtag that have really small number

  # gather data all together
  data = comm.gather(result)
  if comm.rank == 0:
    if comm.size != 1:
      # combine all result together into one dict
      result = handlingAllData(data)
    #sort data in the list
    orderedList = orderTheResultIntoList(result)
    #print data
    prettyPrint(orderedList)