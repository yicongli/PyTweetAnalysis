import sys
import json
from mpi4py import MPI
import numpy as np

MELBOURNE_GRID_FILE = "melbGrid.json"

def getTwitterFile():
  """
  get twitter file name from command argument
  """
  if len(sys.argv) != 2:
    print ("twitter.py <twitter_file.json>" )
  else:
    fileName = sys.argv[1]
    return fileName

def getMelbourneGrids():
  """
  Read Melbourne grid file, then return the information of each grid
  """
  melbGridFile = open(MELBOURNE_GRID_FILE, 'r')
  data = melbGridFile.read()
  melbGrid = json.loads(data)

  grids = {}
  """
  grids is a dictionary in which:
    - key is the id of grid box: A1, A2, ...
    - value is a dictionary contains:
      + coordinates
      + numPosts
      + hashtags: {hashtag: frequency}
  """
  for feature in melbGrid["features"]:
    coordinates = {"xmin": feature["properties"]["xmin"], "xmax": feature["properties"]["xmax"], "ymin": feature["properties"]["ymin"], "ymax": feature["properties"]["ymax"]}
    grids[feature["properties"]["id"]] = {"coordinates": coordinates, "numPosts": 0, "hashtags": {}}
  
  return grids 

def removeRedundantHashtags(resultDic):
  """
  remove the hashtags that have really small number in the result,
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
    # if sixth is equal to fifth, then get the last index that is equal to the fifth hashtag
    topFiveIndex = 5
    lastPostNumber = hashList[4][1]
    if lastPostNumber == hashList[5][1]:
      for i, hashtag in enumerate(hashList[5:]):
        if hashtag[1] < lastPostNumber:
          topFiveIndex += i
          break

    value['hashtags'] = hashList[:topFiveIndex]

  resultList = [(k, resultDic[k]) for k in sorted(resultDic, key=lambda x: resultDic[x]['posN'], reverse=True)]
  return resultList

def prettyPrint(resultList):
  #print pretty
  print('The rank of Grid boxes:')
  for value in resultList:
    print('%s: %d posts,' % (value[0], value[1]['posN']))
  
  print('-'*30)
  print('The rank of the top 5 hashtags in each Grid boxes:')
  for value in resultList:
    print('%s: (' % (value[0]), end='')
    for hashtag in value[1]['hashtags']:
      print('(#%s, %d), ' % (hashtag[0], hashtag[1]), end='')
    print(')')

def splitData(twitterFile, rank, size):
  """
  read all twitter data in master
    if the number of processors is more than 1, split the data into small parts. The total number of parts is equal to the number of processors
    else do not need to split
  """
  # Master 
  if rank == 0:
    tweetsData = []
    with open(twitterFile, 'r') as f:
      for i, line in enumerate(f):
        try:
          tweet = json.loads(line[:-2])
          tweetsData.append(tweet)
        except:
          try:
            tweet = json.loads(line[:-1])
            tweetsData.append(tweet)
          except:
            print ("cannot read line ", i)
            continue
    if size > 1:
      tweetParts = np.array_split(tweetsData, size)
    else:
      tweetParts = tweetsData
  else: # Slaves 
    tweetParts = None 
  
  return tweetParts

def extractInfoFromTweet(tweetData, grids):
  for tweet in tweetData:
    try:        
      coord = tweet["doc"]["coordinates"]["coordinates"]
      if len(coord) < 2:
          continue
      for gridId in grids:
        if (coord[0] <= grids[gridId]["coordinates"]["xmax"] and coord[0] > grids[gridId]["coordinates"]["xmin"] \
          and coord[1] < grids[gridId]["coordinates"]["ymax"] and coord[1] >= grids[gridId]["coordinates"]["ymin"]):
          postNum = grids[gridId]["numPosts"]
          grids[gridId]["numPosts"] = postNum + 1
          hashtags = tweet["doc"]["entities"]["hashtags"]
          if len(hashtags) > 0:
            for hashtag in hashtags:
              hashtag = hashtag.lower() # Case insensitive
              if hashtag in grids[gridId]["hashtags"]:
                oldFrequency = grids[gridId]["hashtags"][hashtag]
                grids[gridId]["hashtags"][hashtag] = oldFrequency + 1
              else:
                grids[gridId]["hashtags"][hashtag] = 1
    except:
      continue

if __name__ == "__main__":
  twitterFile = getTwitterFile()
  grids = getMelbourneGrids()

  comm = MPI.COMM_WORLD
  size = comm.size
  rank = comm.rank

  tweetParts = splitData(twitterFile, rank, size)

  #Scatter the data into slaves, then gather into master
  if rank == 0 and size < 2:
    extractInfoFromTweet(tweetParts, grids)
    result = grids
  else:
    part = comm.scatter(tweetParts, root = 0)
    extractInfoFromTweet(part, grids)
    result = comm.gather(grids, root = 0)
    print (rank, result)

  # removeRedundantHashtags(result) # may need to remove the hashtag that have really small number

  # gather data all together
  # data = comm.gather(result)
  # if comm.rank == 0:
  #   if comm.size != 1:
  #     # combine all result together into one dict
  #     result = handlingAllData(data)
  #   #sort data in the list
  #   orderedList = orderTheResultIntoList(result)
  #   #print data
  #   prettyPrint(orderedList)