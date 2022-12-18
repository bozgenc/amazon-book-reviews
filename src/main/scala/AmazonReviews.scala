package com.bigdata.amazon

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object AmazonReviews extends App {
  val spark = SparkSession
    .builder()
    .appName("Amazon Book Reviews")
    .config("spark.master", "local[*]")
    .config("spark.driver.memory", "12g")
    .getOrCreate()

  import spark.implicits._

  def getProductURL(asin: String): String = {
    "https://www.amazon.com/dp/" + asin
  }

  val rawBookReviewData = spark
    .read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/Books.json")

  val lightweightDF = rawBookReviewData.drop("style")
    .drop("reviewText")
    .drop("summary")
    .drop("verified")
    .cache()


  /*
    printing how many book review made between 1996-2018
   */
  val totalReviewNumber = lightweightDF.count();
  println("All times total review number is: " + totalReviewNumber)


  /*
     finding the user who has most review count between 1996-2018
   */
  val userWhoReviewedMost = lightweightDF.groupBy(col("reviewerId"))
    .count()
    .withColumnRenamed("count", "users_review_number")
    .orderBy(desc("users_review_number"))
  userWhoReviewedMost.show()

  /*
    printing top ten users who made most reviews
   */
  val topTenUsersReviewedMost = userWhoReviewedMost.takeAsList(10)
  val topTenUsersDF = userWhoReviewedMost.limit(10)
  println("All times top ten users who made most reviews:")
  for (i <- 0 until topTenUsersReviewedMost.size()) {
    val reviewerId = topTenUsersReviewedMost.get(i).get(0)
    val reviewCount = topTenUsersReviewedMost.get(i).get(1)
    println("Reviewer id: ", reviewerId, " Review Count: " + reviewCount)
  }

  for (i <- 0 until 1) {
    lightweightDF.filter(col("reviewerId") === topTenUsersReviewedMost.get(i).get(0)).select("reviewerName").show(1)
  }


  /*
  finding the books which has been reviewed most between 1996-2018
  */
  val topBooksReviewed = lightweightDF.groupBy((col("asin")))
    .count()
    .withColumnRenamed("count", "products_review_count")
    .orderBy(desc("products_review_count"))
  topBooksReviewed.show()

  /*
  printing top ten books in terms of review count
  */
  val topTenBooksReviewed = topBooksReviewed.takeAsList(10)
  println("All times most reviewed ten books: ")
  for (i <- 0 until topTenBooksReviewed.size()) {
    val asin: String = topTenBooksReviewed.get(i).get(0).toString
    println("Review Count: " + topTenBooksReviewed.get(i).get(1) + " Book Link: " + getProductURL(asin))
  }


  /*
  finding the top 10 books which has most five star reviews
  */
  val bookWhoHasMostFiveStar = lightweightDF.filter(col("overall") === 5d)
    .groupBy(col("asin"))
    .count()
    .withColumnRenamed("count", "number_of_five_stars")
    .orderBy(desc("number_of_five_stars"))
  bookWhoHasMostFiveStar.show(10)

  /*
  printing the top 10 books which has most five star reviews
  */
  val fiveStarBook = bookWhoHasMostFiveStar.takeAsList(10)
  println("All times top 10 books which reviewed 5 stars")
  for (i <- 0 until fiveStarBook.size()) {
    val asinOfFiveStarBook = fiveStarBook.get(i).get(0) + ", Count:" + fiveStarBook.get(i).get(1)
    println(getProductURL(asinOfFiveStarBook))
  }


  /*
  finding the top 10 books which has most one star reviews
  */
  val bookWhoHasMostOneStar = lightweightDF.filter(col("overall") === 1d)
    .groupBy(col("asin"))
    .count()
    .withColumnRenamed("count", "number_of_one_stars")
    .orderBy(desc("number_of_one_stars"))
  bookWhoHasMostOneStar.show(10)

  /*
  printing the top 10 books which has most one star reviews
  */
  val oneStarBook = bookWhoHasMostOneStar.takeAsList(10)
  println("All times top 10 books which received 1 star")
  for (i <- 0 until oneStarBook.size()) {
    val asinOfOneStarBook = oneStarBook.get(i).get(0) + ", Count:" + oneStarBook.get(i).get(1)
    println(getProductURL(asinOfOneStarBook))
  }


  /*
  finding top rated ten books
  */
  val booksAndTotalPoints = lightweightDF.groupBy(col("asin"))
    .agg(sum("overall").as("total_points"))
    .orderBy(desc("total_points"))
    .limit(100)

  val booksAndReviewCounts = lightweightDF.groupBy(col("asin"))
    .count()
    .withColumnRenamed("count", "review_numbers")
    .orderBy(desc("review_numbers"))
    .limit(100)

  val pointsAndCountsJoined = booksAndTotalPoints
    .join(booksAndReviewCounts, booksAndTotalPoints("asin") === booksAndReviewCounts("asin"), "inner")

  val bookRatings = pointsAndCountsJoined
    .withColumn("ratings", $"total_points" / $"review_numbers")
    .orderBy(desc("ratings"))
  bookRatings.show()

  /*
  printing top rated ten books
  */
  val bookRatingsList = bookRatings.takeAsList(10)
  println("All times top ten books which has the highest scores: ")
  for (i <- 0 until 10) {
    val asinBook = bookRatingsList.get(i).get(0)
    val rating = bookRatingsList.get(i).get(4)
    println("Book Link: " + getProductURL(asinBook.toString) + " , Rating: " + rating)
  }


  /*
  finding and printing top 10 controversial books of all time
  */
  val five = lightweightDF.filter(col("overall") === 5d).
    groupBy(col("asin"))
    .count()
    .withColumnRenamed("count", "five_stars")

  val one = lightweightDF.filter(col("overall") === 1d)
    .groupBy(col("asin"))
    .count()
    .withColumnRenamed("count", "one_stars")

  val others = lightweightDF.filter(col("overall") =!= 5d)
    .filter(col("overall") =!= 1d)
    .groupBy(col("asin"))
    .count()
    .withColumnRenamed("count", "other_points")

  val tempDf = five.alias("five").join(one.alias("one"), five("asin") === one("asin"), "inner").drop(one("asin"))
  val joined = tempDf.join(others.alias("others"), tempDf("asin") === others("asin"), "inner").drop(others("asin"))

  val expr = ($"five_stars" + $"one_stars") / $"other_points"
  val controversials = joined.filter(col("five_stars") > col("other_points"))
    .filter(col("one_stars") > col("other_points"))
    .withColumn("ratio", expr)
    .orderBy(desc("ratio"))

  val temp = controversials.takeAsList(10);
  for (i <- 0 until temp.size()) {
    val str = "Book Link: " + getProductURL(temp.get(i).get(1).toString) + ", " + " Controversy Ratio : " + temp.get(i).get(5)
    println(str)
  }



  // --------------------- YEARLY STATISTICS BELOW HERE --------------------- //

  // finding yearly review counts
  val yearlyCountList = new ListBuffer[Long]
  def findYearlyReviewCounts(index: Int): Unit = {
    val year = index.toString
    val yearSpecificDF = lightweightDF.filter(col("reviewTime").contains(year))

    val yearSpecificReviewCount = yearSpecificDF.count()
    yearlyCountList.append(yearSpecificReviewCount)

  }


  // finding yearly reviewers count
  val yearlyReviewerList = new ListBuffer[String]
  def findYearlyUserWhoHasMostReviews(index: Int): Unit = {
    val year = index.toString
    val yearSpecificDF = lightweightDF.filter(col("reviewTime").contains(year))

    val userWhoReviewedMost = yearSpecificDF.groupBy(col("reviewerId"))
      .count()
      .withColumnRenamed("count", "users_review_number")
      .orderBy(desc("users_review_number"))

    val temp = userWhoReviewedMost.takeAsList(5);
    for (i <- 0 until temp.size()) {
      val str = "Reviewer Id: " + temp.get(i).get(0) + ", " + " Review Count: " + temp.get(i).get(1)
      yearlyReviewerList.append(str)
    }
  }


  // finding top 5 books reviewed most per year
  val yearlyBooksReviewList = new ListBuffer[String]
  def findYearlyBooksWhichReviewedMost(index: Int): Unit = {
    val year = index.toString
    val yearSpecificDF = lightweightDF.filter(col("reviewTime").contains(year))

    val topBooksReviewed = yearSpecificDF.groupBy((col("asin")))
      .count()
      .withColumnRenamed("count", "products_review_count")
      .orderBy(desc("products_review_count"))

    val temp = topBooksReviewed.takeAsList(5);
    for (i <- 0 until temp.size()) {
      val str = "Book Link: " + getProductURL(temp.get(i).get(0).toString) + ", " + " Review Count: " + temp.get(i).get(1)
      yearlyReviewerList.append(str)
    }
  }


  // finding top 5 books has most 5 stars per year
  val yearlyBooksFiveStar = new ListBuffer[String]
  def findYearlyBooksWhichHasMostFiveStars(index: Int): Unit = {
    val year = index.toString
    val yearSpecificDF = lightweightDF.filter(col("reviewTime").contains(year))

    val bookWhoHasMostFiveStar = yearSpecificDF.filter(col("overall") === 5d)
      .groupBy(col("asin"))
      .count()
      .withColumnRenamed("count", "number_of_five_stars")
      .orderBy(desc("number_of_five_stars"))

    val temp = bookWhoHasMostFiveStar.takeAsList(5);
    for (i <- 0 until temp.size()) {
      val str = "Book Link: " + getProductURL(temp.get(i).get(0).toString) + ", " + " Number of Five Stars: " + temp.get(i).get(1)
      yearlyBooksFiveStar.append(str)
    }
  }

  // finding top 5 books has most 1 star per year
  val yearlyBooksOneStar = new ListBuffer[String]
  def findYearlyBooksWhichHasMostOneStar(index: Int): Unit = {
    val year = index.toString
    val yearSpecificDF = lightweightDF.filter(col("reviewTime").contains(year))

    val bookWhoHasMostOneStar = yearSpecificDF.filter(col("overall") === 1d)
      .groupBy(col("asin"))
      .count()
      .withColumnRenamed("count", "number_of_one_star")
      .orderBy(desc("number_of_one_star"))

    val temp = bookWhoHasMostOneStar.takeAsList(5);
    for (i <- 0 until temp.size()) {
      val str = "Book Link: " + getProductURL(temp.get(i).get(0).toString) + ", " + " Number of One Star: " + temp.get(i).get(1)
      yearlyBooksOneStar.append(str)
    }
  }


  // top rated 5 books of each year
  val yearlyTopRatedBooks = new ListBuffer[String]
  def findYearlyBooksWhichTopRated(index: Int): Unit = {
    val year = index.toString
    val yearSpecificDF = lightweightDF.filter(col("reviewTime").contains(year))

    val booksAndTotalPoints = yearSpecificDF.groupBy(col("asin"))
      .agg(sum("overall").as("total_points"))
      .orderBy(desc("total_points"))
      .limit(100)

    val booksAndReviewCounts = yearSpecificDF.groupBy(col("asin"))
      .count()
      .withColumnRenamed("count", "review_numbers")
      .orderBy(desc("review_numbers"))
      .limit(100)

    val pointsAndCountsJoined = booksAndTotalPoints
      .join(booksAndReviewCounts, booksAndTotalPoints("asin") === booksAndReviewCounts("asin"), "inner")

    val bookRatings = pointsAndCountsJoined
      .withColumn("ratings", $"total_points" / $"review_numbers")
      .orderBy(desc("ratings"))

    val temp = bookRatings.takeAsList(5);
    for (i <- 0 until temp.size()) {
      val str = "Book Link: " + getProductURL(temp.get(i).get(0).toString) + ", " + " Rating: " + temp.get(i).get(4)
      yearlyTopRatedBooks.append(str)
    }
  }


  // most controversial 5 books of each year
  val yearlyControversialBooks = new ListBuffer[String]
  def findYearlyMostControversialBooks(index: Int): Unit = {
    val year = index.toString
    val yearSpecificDF = lightweightDF.filter(col("reviewTime").contains(year))

    val five = yearSpecificDF.filter(col("overall") === 5d).
      groupBy(col("asin"))
      .count()
      .withColumnRenamed("count", "five_stars")

    val one = yearSpecificDF.filter(col("overall") === 1d)
      .groupBy(col("asin"))
      .count()
      .withColumnRenamed("count", "one_stars")

    val others = yearSpecificDF.filter(col("overall") =!= 5d)
      .filter(col("overall") =!= 1d)
      .groupBy(col("asin"))
      .count()
      .withColumnRenamed("count", "other_points")

    val tempDf = five.alias("five").join(one.alias("one"), five("asin") === one("asin"), "inner").drop(one("asin"))
    val joined = tempDf.join(others.alias("others"), tempDf("asin") === others("asin"), "inner").drop(others("asin"))

    val expr = ($"five_stars" + $"one_stars") / $"other_points"
    val controversials = joined.filter(col("five_stars") > col("other_points"))
      .filter(col("one_stars") > col("other_points"))
      .withColumn("ratio", expr)
      .orderBy(desc("ratio"))

    controversials.show()
    val temp = controversials.takeAsList(5);
    for (i <- 0 until temp.size()) {
      val str = "Book Link: " + getProductURL(temp.get(i).get(1).toString) + ", " + " Controversy Ratio : " + temp.get(i).get(5)
      yearlyControversialBooks.append(str)
    }
  }


  // finding yearly statistics
  for (i <- 1996 until 2019) {
    findYearlyReviewCounts(i)
    findYearlyUserWhoHasMostReviews(i)
    findYearlyBooksWhichReviewedMost(i)
    findYearlyBooksWhichHasMostFiveStars(i)
    findYearlyBooksWhichHasMostOneStar(i)
    findYearlyBooksWhichTopRated(i)
    findYearlyMostControversialBooks(i)
  }

  // printing yearly review counts
  for (i <- yearlyCountList.indices) {
    println("Year: " + (i + 1996) + ", Review Count: " + yearlyCountList(i))
  }

  // printing yearly top 5 reviewers in terms of review counts
  var j = 0;
  for (i <- yearlyReviewerList.indices) {
    if (i == 0 || i % 5 == 0) {
      println("--------------------------------")
      println("Year: " + (j + 1996))
      j = j + 1;
    }
    println(yearlyReviewerList(i))
  }

  // printing yearly top 5 books reviewed most
  var k = 0;
  for (i <- yearlyBooksReviewList.indices) {
    if (i == 0 || i % 5 == 0) {
      println("--------------------------------")
      println("Year: " + (k + 1996))
      k = k + 1;
    }
    println(yearlyBooksReviewList(i))
  }

  // printing yearly top 5 books which has most 5 stars
  var p = 0;
  for (i <- yearlyBooksFiveStar.indices) {
    if (i == 0 || i % 5 == 0) {
      println("--------------------------------")
      println("Year: " + (p + 1996))
      p = p + 1;
    }
    println(yearlyBooksFiveStar(i))
  }

  // printing yearly top 5 books which has most 1 star
  var r = 0;
  for (i <- yearlyBooksOneStar.indices) {
    if (i == 0 || i % 5 == 0) {
      println("--------------------------------")
      println("Year: " + (r + 1996))
      r = r + 1;
    }
    println(yearlyBooksOneStar(i))
  }

  // printing yearly top 5 books which has rated highest
  var s = 0
  for (i <- yearlyTopRatedBooks.indices) {
    if (i == 0 || i % 5 == 0) {
      println("--------------------------------")
      println("Year: " + (s + 1996))
      s = s + 1;
    }
    println(yearlyTopRatedBooks(i))
  }

  // printing yearly top 5 books which are most controversial
  var x = 0
  for (i <- yearlyControversialBooks.indices) {
    if (i == 0 || i % 5 == 0) {
      println("--------------------------------")
      println("Year: " + (x + 1996))
      x = x + 1;
    }
    println(yearlyControversialBooks(i))
  }


}
