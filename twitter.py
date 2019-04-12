import sys
import json
from mpi4py import MPI
import numpy as np 
import os.path

MELBOURNE_GRID_FILE = "melbGrid.json"

def getTwitterFile():
  """
  Get twitter file name from command argument
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

def printOut(resultList):
  """
  Print out the results in terminal
  """
  print('The rank of Grid boxes:')
  for post in resultList:
    print('%s: %d posts,' % (post[0], post[1]))
  
  print('-'*30)
  print('The rank of the top 5 hashtags in each Grid boxes:')
  for post in resultList:
    print('%s: (' % (post[0]), end='')
    for hashtag in post[2]:
      print('(#%s, %d), ' % (hashtag[0], hashtag[1]), end='')
    print(')')

def getStartingPoints(twitterFile):
  """
  Get all the starting points of every line in twitter file
  """
  startingPoints = []
  length = 0
  with open(twitterFile, 'rb') as f:
    for line in f:
      startingPoints.append(length)
      length += len(line)
  with open('startingPoints.json', 'w') as output:
    json.dump(startingPoints, output)
  return startingPoints

def readTweetsFromStartPoints(twitterFile, startingPoints, grids):
  """
  From the starting point, seek the line in twitter file and read that line
  """
  for startingPoint in startingPoints:
    with open(twitterFile, 'r') as f:
      f.seek(startingPoint)
      try:
        line = f.readline()
        tweet = json.loads(line[:-2])
        extractInfoFromTweet(tweet, grids)
      except:
        try:
          tweet = json.loads(line[:-1])
          extractInfoFromTweet(tweet, grids)
        except:
          continue

def extractInfoFromTweet(tweet, grids):
  """
  Count the number of posts and the frequencies of hashtags
  """
  try:        
    coord = tweet["doc"]["coordinates"]["coordinates"]
    if len(coord) < 2:
      return None
    for gridId in grids:
      if (coord[0] <= grids[gridId]["coordinates"]["xmax"] and coord[0] > grids[gridId]["coordinates"]["xmin"] \
        and coord[1] < grids[gridId]["coordinates"]["ymax"] and coord[1] >= grids[gridId]["coordinates"]["ymin"]):
        postNum = grids[gridId]["numPosts"]
        grids[gridId]["numPosts"] = postNum + 1
        hashtags = tweet["doc"]["entities"]["hashtags"]
        for hashtag in hashtags:
          hashtagText = hashtag["text"].lower() # Case insensitive
          if hashtagText in grids[gridId]["hashtags"]:
            oldFrequency = grids[gridId]["hashtags"][hashtagText]
            grids[gridId]["hashtags"][hashtagText] = oldFrequency + 1
          else:
            grids[gridId]["hashtags"][hashtagText] = 1
  except:
    return None

if __name__ == "__main__":
  # Get the twitter file name and read the Melbourne grids
  twitterFile = getTwitterFile()
  grids = getMelbourneGrids()

  # Initialise mpi
  comm = MPI.COMM_WORLD
  size = comm.size
  rank = comm.rank
  
  # Get the starting points of every line in twitter file
  if os.path.isfile("startingPoints.json"):
    jsonData = open("startingPoints.json")
    allStartingPoints = json.load(jsonData)
  else:
    allStartingPoints = getStartingPoints(twitterFile)

  # Split the starting points array in the master node if the number of processors is more than 1
  if rank == 0:
    allStartingPoints = allStartingPoints[1:-1]
    if size > 1:
      tweetStartingPoints = np.array_split(allStartingPoints, size)
    else:
      tweetStartingPoints = allStartingPoints
  else:
    tweetStartingPoints = None 
  
  #Scatter the array of all starting points into slaves, then gather into master
  if rank == 0 and size < 2:
    result = readTweetsFromStartPoints(twitterFile, tweetStartingPoints, grids)
  else:
    startingPointsChunk = comm.scatter(tweetStartingPoints, root = 0)
    readTweetsFromStartPoints(twitterFile, startingPointsChunk, grids)
    result = comm.gather(grids, root = 0)

  # In the master node, gather all the results and sort them in order 
  if rank == 0:
    # Combine the results
    if size > 1:
      resultGrids = getMelbourneGrids()
      for tweetPart in result:
        for gridId in tweetPart:
          currentNumPosts = resultGrids[gridId]["numPosts"]
          resultGrids[gridId]["numPosts"] = currentNumPosts + tweetPart[gridId]["numPosts"]

          for hashtag in tweetPart[gridId]["hashtags"]:
            if hashtag in resultGrids[gridId]["hashtags"]:
              resultGrids[gridId]["hashtags"][hashtag] += tweetPart[gridId]["hashtags"][hashtag]
            else:
              resultGrids[gridId]["hashtags"][hashtag] = tweetPart[gridId]["hashtags"][hashtag]
    else:
      resultGrids = grids

    # Sort result grids in order of number of posts
    sortedGridIds = sorted(resultGrids, key = lambda gridId: resultGrids[gridId]["numPosts"], reverse = True)
    sortedByPosts = [[id, resultGrids[id]["numPosts"], resultGrids[id]["hashtags"]] for id in sortedGridIds]
    
    # In each grid, sort the hashtags in order
    for grid in sortedByPosts:
      hashtags = grid[2]
      sortedHashtags = [(tag, hashtags[tag]) for tag in sorted(hashtags, key = lambda tag : hashtags[tag], reverse = True)]
      topFives = sortedHashtags[:5]
      if len(sortedHashtags) >= 4:
        fifthRanking = sortedHashtags[4]
        for nextTag in sortedHashtags[5:]:
          if nextTag[1] == fifthRanking[1]:
            topFives.append(nextTag)
          else:
            break
      grid[2] = topFives

    # Print out the result
    printOut(sortedByPosts)
    
    