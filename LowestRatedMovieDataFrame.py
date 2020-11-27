from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# Please find the data set used in this program from https://grouplens.org/datasets/movielens/100k/ ml-100k.zip 

def loadMovieNames():
    movieNames = {}
    # Open the u.item file from the local system to create movies dictionary with movieID and names 
    # which run on spark driver
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get the raw data
    # To read the data from hdfs you can give complete hdfs location Ex: hdfs:///user/user_name/ml-100k/u.data
    # The below u.data file will look at the user hadoop home location
    lines = spark.sparkContext.textFile("u.data")
    # Convert it to a RDD of Row objects with (movieID, rating)
    movies = lines.map(parseInput)
    # Convert that to a DataFrame
    movieDataset = spark.createDataFrame(movies)

    # Compute average rating for each movieID
    averageRatings = movieDataset.groupBy("movieID").avg("rating")

    # Compute count of ratings for each movieID
    counts = movieDataset.groupBy("movieID").count()

    # Join the two together (We now have movieID, avg(rating), and count columns)
    averagesAndCounts = counts.join(averageRatings, "movieID")

    # Pull the top 10 results
    topTen = averagesAndCounts.orderBy("avg(rating)").take(10)

    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print (movieNames[movie[0]], movie[1], movie[2])

    # Stop the session
    spark.stop()
