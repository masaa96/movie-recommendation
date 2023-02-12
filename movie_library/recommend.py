from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import lit


def get_recommendations(ratings_list, user_id):
    """
    Getting list of ratings from database and current user id.
    Return list of 5 movie ids recommended for this user.
    """
    user_recommendations = []

    # Start a Spark session
    spark = SparkSession.builder.appName("MovieRecommendation").getOrCreate()
    spark.conf.set("spark.sql.crossJoin.enabled", "true")

    # Convert to a DataFrame and cache it
    ratings = spark.createDataFrame(ratings_list).cache()

    # Create an ALS collaborative filtering model from the complete data set
    als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="movie_id", ratingCol="rating")
    model = als.fit(ratings)

    userRatings = ratings.filter(f"userID = {user_id}")
    for rating in userRatings.collect():
        print(rating['movieID'], " : ", rating['rating'])

    # Find movies rated at least 2 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 1")

    # Construct a "test" dataframe for current user with every movie rated more than once
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(user_id))

    # Run our model on that list of popular movies for current user
    recommendations = model.transform(popularMovies)

    # Get the top 5 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc()).take(5)

    for recommendation in topRecommendations:
        user_recommendations.append(
            {"movie_id": recommendation['movieID'], "prediction": recommendation['prediction']}
        )

    spark.stop()
    return user_recommendations
