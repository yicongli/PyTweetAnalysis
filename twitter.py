import sys
import json
from mpi4py import MPI
import os.path

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

def getStartingPoints(fileName):
  """
  Get the line start point of every line in twitter file
  Run this before run the final application
  """
  dict_pos = []
  length = 0

  with open(fileName,'rb') as file:
    for each in file:
        dict_pos.append(length)
        length += len(each)

  # store the starting point of each line into file
  with open('startingPoints.json', 'w') as outfile:
    json.dump(dict_pos, outfile)
  return dict_pos

def initResultDic():
  """
  Initialise the result dictionary: Store the post number and hashtags of each grid box
  """
  resultDic = {}
  for key, value in g_grids.items():
    resultDic[key] = {'postNum':0, 'hashtags': {}}
  
  return resultDic

def parseJsonDataWithConf(fileName, startLinePoint, maxRange):
  """
  Parse json data according to the starting point and the max range from the twitter file
  """
  result = initResultDic()

  with open(fileName, 'r') as file:
    file.seek(startLinePoint) # jump to the start point
    for i, line in enumerate(file):
      if(i >= maxRange): # if larger than current maxRange, then return directly
        return result
      try:
          data = json.loads(line[:-2]) # extract key info from data
          extractInfoFromData(data, result)
      except:
        try:
          data = json.loads(line[:-1]) 
          extractInfoFromData(data, result)
        except:
          return 

  return result

def extractInfoFromData(data, resultDic):
  """
  Extract the information from tweets
  """
  try:
    dataCoord = data['doc']['coordinates']['coordinates']
  except TypeError:
    return
  
  if len(dataCoord) < 2:
    return

  for key, value in g_grids.items():
    # if the value in the specific area, then store the data into the result dic
    if (dataCoord[0] <= value['xmax'] and dataCoord[0] > value['xmin'] 
    and dataCoord[1] < value['ymax'] and dataCoord[1] >= value['ymin']):
      postNum = resultDic[key]['postNum']
      resultDic[key]['postNum'] = postNum + 1
      hashtags = data['doc']['entities']['hashtags']
      # if has hashtags, then record the hash dic
      if len(hashtags) > 0:
        for hashtag in hashtags:
          hashtagText = hashtag['text'].lower() # case insensitive
          if hashtagText in resultDic[key]['hashtags']:
            hashtagNum = resultDic[key]['hashtags'][hashtagText]
            resultDic[key]['hashtags'][hashtagText] = hashtagNum + 1
          else:
            resultDic[key]['hashtags'][hashtagText] = 1

def inputFileName():
  """
  get twitter file name from command line
  """
  if len(sys.argv) != 2:
    print ("twitter.py <twitter_file.json>" )
  else:
    fileName = sys.argv[1]
    return fileName

def handlingAllData(dataList):
  """
  combine all data in the list together and return data list
  """
  result = initResultDic()
  for data in dataList:
    for key, value in data.items():
      totalNum = result[key]['postNum']
      result[key]['postNum'] = value['postNum'] + totalNum

      hashtags = value['hashtags']
      for hashtag, frequency in hashtags.items():
        if hashtag in result[key]['hashtags']:
          hashtagNum = result[key]['hashtags'][hashtag]
          result[key]['hashtags'][hashtag] = hashtagNum + frequency
        else:
          result[key]['hashtags'][hashtag] = frequency
  return result

def orderTheResultIntoList(resultDic):
  """
  Sort the grid by post number, sort the hash tags of each area by frequency
  """
  for key, value in resultDic.items():
    hashtagDic = value['hashtags']
    hashList = [(tag, hashtagDic[tag]) for tag in sorted(hashtagDic, key=hashtagDic.get, reverse=True)]
    topFives = hashList[:5]
    if len(hashList) > 5:
      fifthRanking = hashList[4][1]
      for nextTag in hashList[5:]:
        if nextTag[1] == fifthRanking:
          topFives.append(nextTag)
        else:
          break
    value['hashtags'] = topFives 
  resultList = [(k, resultDic[k]) for k in sorted(resultDic, key=lambda x: resultDic[x]['postNum'], reverse=True)]
  return resultList

def printOut(resultList):
  """
  Print out the results
  """
  print('The rank of Grid boxes:')
  for value in resultList:
    print('%s: %d posts,' % (value[0], value[1]['postNum']))
  
  print('-'*30)
  print('The rank of the top 5 hashtags in each Grid boxes:')
  for value in resultList:
    print('%s: (' % (value[0]), end='')
    for hashtag in value[1]['hashtags']:
      print('(#%s, %d), ' % (hashtag[0], hashtag[1]), end='')
    print(')')

if __name__ == "__main__":
  twitterFile = inputFileName()

  #load configuration of big json data
  if os.path.exists('startingPoints.json'):
    with open('startingPoints.json') as json_data:
      startingPoints = json.load(json_data)
  else:
    startingPoints = getStartingPoints(twitterFile)

  comm = MPI.COMM_WORLD
  size = comm.size
  rank = comm.rank 

  totalLines = len(startingPoints) - 2
  iterRange = int(totalLines / size) # get the range of iteration
  startLineNum = rank * iterRange + 1

  result = parseJsonDataWithConf(twitterFile, startingPoints[startLineNum], iterRange)
  
  # gather data all together
  data = comm.gather(result)
  if rank == 0:
    if size != 1:
      result = handlingAllData(data) # combine all result together into one dict
    orderedList = orderTheResultIntoList(result) #sort data in the list
    printOut(orderedList) #print data