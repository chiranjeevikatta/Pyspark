from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

# Please find the data set used in this program from https://grouplens.org/datasets/movielens/100k/ ml-100k.zip 

def parseInput(line):
    fields = line.split()
    return Row(movieID = int(fields[1]), rating = float(fields[2]))

def parseInput_1(line):
    fields = line.split("|")
    return Row(movieID = int(fields[0]), movie_name = fields[1])    

if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up our movie ID -> name dictionary
    lines = spark.sparkContext.textFile("u.item")
    moviesIdNames = lines.map(parseInput_1)
    moviesIdNamesDataset = spark.createDataFrame(moviesIdNames)

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

    result = averagesAndCounts.join(moviesIdNamesDataset,"movieID")
    
    #Saving top10 low rated movies
    top10 =  result.select("movie_name","avg(rating)","count").orderBy("avg(rating)").take(10)
    
    top10.write.format("csv").save("top10LowRatedMovies")

    # Stop the session
    spark.stop()
