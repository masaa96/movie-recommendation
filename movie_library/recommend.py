from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import lit


def get_recommendations(ratings_list, user_id):
    """
    Getting list of ratings from database and current user id.
    Return list of 10 movie ids recommended for this user.
    """
    user_recommendations = []

    # Start a Spark session
    spark = SparkSession.builder.appName("MovieRecommendation").getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    # Convert to a DataFrame and cache it
    ratings = spark.createDataFrame(ratings_list).cache()

    # Create the StringIndexer for user_id
    user_indexer = StringIndexer(inputCol="user_id", outputCol="userIndex")

    # Fit the indexer on the ratings data
    user_indexer_model = user_indexer.fit(ratings)

    # Get the userIndex for the provided user_id
    userIndex = user_indexer_model.transform(spark.createDataFrame([(user_id,)], ["user_id"])) \
        .select("userIndex") \
        .collect()[0][0]

    # Transform the ratings data with the userIndex column
    ratings = user_indexer_model.transform(ratings)

    # Drop the original user_id column
    ratings = ratings.drop("user_id")

    # Rename the userIndex column to user_id
    ratings = ratings.withColumnRenamed("userIndex", "user_id")

    # Repeat the same steps for movie_id
    movie_indexer = StringIndexer(inputCol="movie_id", outputCol="movieIndex")
    movie_indexer_model = movie_indexer.fit(ratings)
    ratings = movie_indexer_model.transform(ratings)
    ratings = ratings.drop("movie_id")
    ratings = ratings.withColumnRenamed("movieIndex", "movie_id")

    # Create an ALS collaborative filtering model from the complete data set
    als = ALS(maxIter=10, regParam=0.01, userCol="user_id", itemCol="movie_id", ratingCol="rating")
    model = als.fit(ratings)

    userRatings = ratings.filter(f"user_id = {userIndex}")
    for rating in userRatings.collect():
        print(rating['movie_id'], " : ", rating['rating'])

    # Find movies rated at least 2 times
    ratingCounts = ratings.groupBy("movie_id").count().filter("count > 1")

    # Construct a "test" dataframe for current user with every movie rated more than once
    popularMovies = ratingCounts.select("movie_id").withColumn('user_id', lit(userIndex))

    # Run our model on that list of popular movies for current user
    recommendations = model.transform(popularMovies)

    # Get the top 10 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(10)

    # Get the original string values for the movie ids
    original_movie_ids = movie_indexer_model.labels

    for recommendation in topRecommendations:
        movie_id = recommendation['movie_id']
        prediction = recommendation['prediction']
        # Get the original string value for the movie id
        original_movie_id = original_movie_ids[int(movie_id)]
        user_recommendations.append(
            {"movie_id": original_movie_id, "prediction": prediction}
        )

    spark.stop()
    return user_recommendations
