# cluster-and-cloud-1
Assignment 1

the process of handling the big json file:
1. Preprocessing the big file to get the start position of each line
2. Save all start position in to a list and store in the json file named 'conf.json'
3. Before starting processing the big file, read the config from configuration
4. Every process has its own starting and endinng point, so get the position of these two point from config
5. Then processing the file from its own start point.
