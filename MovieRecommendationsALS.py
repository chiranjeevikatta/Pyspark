from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit

# Please find the data set used in this program from https://grouplens.org/datasets/movielens/100k/ ml-100k.zip

# Load up movie ID -> movie name dictionary
def loadMovieNames():
    movieNames = {}
    # Open the u.item file from the local system to create movies dictionary with movieID and names 
    # which run on spark driver
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

# Convert u.data lines into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.value.split()
    return Row(userID = int(fields[0]), movieID = int(fields[1]), rating = float(fields[2]))


if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Get the raw data
    # To read the data from hdfs you can give complete hdfs location Ex: hdfs:///user/user_name/ml-100k/u_modified.data
    # The below u.data file will look at the user hadoop home location
    lines = spark.read.text("u_modified.data").rdd

    # Convert it to a RDD of Row objects with (userID, movieID, rating)
    ratingsRDD = lines.map(parseInput)

    # Convert to a DataFrame and cache it
    ratings = spark.createDataFrame(ratingsRDD).cache()

    # Create an ALS collaborative filtering model from the complete data set
    als = ALS(maxIter=5, regParam=0.01, userCol="userID", itemCol="movieID", ratingCol="rating")
    model = als.fit(ratings)

    # Print out ratings from user 0:
    print("\nRatings for user ID 0:")
    userRatings = ratings.filter("userID = 0")
    for rating in userRatings.collect():
        print movieNames[rating['movieID']], rating['rating']

    print("\nTop 20 recommendations:")
    # Find movies rated more than 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    # Construct a "test" dataframe for user 0 with every movie rated more than 100 times
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))

    # Run our model on that list of popular movies for user ID 0
    recommendations = model.transform(popularMovies)

    # Get the top 20 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(20)

    for recommendation in topRecommendations:
        print (movieNames[recommendation['movieID']], recommendation['prediction'])

    spark.stop()
