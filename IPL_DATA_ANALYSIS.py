# # Databricks notebook source
# spark

# # COMMAND ----------

# from pyspark.sql.types import*
# from pyspark.sql.functions import*
# from pyspark.sql.window import Window 

# # COMMAND ----------

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("IPL Data Analysis").getOrCreate()

# # COMMAND ----------

# sparks

# # COMMAND ----------

# ball_by_ball_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/FileStore/tables/Ball_By_Ball.csv")

# # COMMAND ----------

# ball_by_ball_df.display()

# # COMMAND ----------

# ball_by_ball_schema = StructType([
#     StructField("match_id", IntegerType(), True),
#     StructField("over_id", IntegerType(), True),
#     StructField("ball_id", IntegerType(), True),
#     StructField("innings_no", IntegerType(), True),
#     StructField("team_batting", StringType(), True),
#     StructField("team_bowling", StringType(), True),
#     StructField("striker_batting_position", IntegerType(), True),
#     StructField("extra_type", StringType(), True),
#     StructField("runs_scored", IntegerType(), True),
#     StructField("extra_runs", IntegerType(), True),
#     StructField("wides", IntegerType(), True),
#     StructField("legbyes", IntegerType(), True),
#     StructField("byes", IntegerType(), True),
#     StructField("noballs", IntegerType(), True),
#     StructField("penalty", IntegerType(), True),
#     StructField("bowler_extras", IntegerType(), True),
#     StructField("out_type", StringType(), True),
#     StructField("caught", BooleanType(), True),
#     StructField("bowled", BooleanType(), True),
#     StructField("run_out", BooleanType(), True),
#     StructField("lbw", BooleanType(), True),
#     StructField("retired_hurt", BooleanType(), True),
#     StructField("stumped", BooleanType(), True),
#     StructField("caught_and_bowled", BooleanType(), True),
#     StructField("hit_wicket", BooleanType(), True),
#     StructField("obstructingfeild", BooleanType(), True),
#     StructField("bowler_wicket", BooleanType(), True),
#     StructField("match_date", DateType(), True),
#     StructField("season", IntegerType(), True),
#     StructField("striker", IntegerType(), True),
#     StructField("non_striker", IntegerType(), True),
#     StructField("bowler", IntegerType(), True),
#     StructField("player_out", IntegerType(), True),
#     StructField("fielders", IntegerType(), True),
#     StructField("striker_match_sk", IntegerType(), True),
#     StructField("strikersk", IntegerType(), True),
#     StructField("nonstriker_match_sk", IntegerType(), True),
#     StructField("nonstriker_sk", IntegerType(), True),
#     StructField("fielder_match_sk", IntegerType(), True),
#     StructField("fielder_sk", IntegerType(), True),
#     StructField("bowler_match_sk", IntegerType(), True),
#     StructField("bowler_sk", IntegerType(), True),
#     StructField("playerout_match_sk", IntegerType(), True),
#     StructField("battingteam_sk", IntegerType(), True),
#     StructField("bowlingteam_sk", IntegerType(), True),
#     StructField("keeper_catch", BooleanType(), True),
#     StructField("player_out_sk", IntegerType(), True),
#     StructField("matchdatesk", DateType(), True)
# ])

# # COMMAND ----------

# ball_by_ball_df = spark.read.schema(ball_by_ball_schema).format("csv").option("header","true").load("dbfs:/FileStore/tables/Ball_By_Ball.csv")

# # COMMAND ----------

# match_schema = StructType([
#     StructField("match_sk", IntegerType(), True),
#     StructField("match_id", IntegerType(), True),
#     StructField("team1", StringType(), True),
#     StructField("team2", StringType(), True),
#     StructField("match_date", DateType(), True),
#     StructField("season_year", IntegerType(), True),  # PySpark doesn't have a dedicated YearType; using IntegerType.
#     StructField("venue_name", StringType(), True),
#     StructField("city_name", StringType(), True),
#     StructField("country_name", StringType(), True),
#     StructField("toss_winner", StringType(), True),
#     StructField("match_winner", StringType(), True),
#     StructField("toss_name", StringType(), True),
#     StructField("win_type", StringType(), True),
#     StructField("outcome_type", StringType(), True),
#     StructField("manofmach", StringType(), True),
#     StructField("win_margin", IntegerType(), True),
#     StructField("country_id", IntegerType(), True)
# ])

# match_df = spark.read.schema(match_schema).format("csv").option("header","true").load("dbfs:/FileStore/tables/Match.csv")



# # COMMAND ----------

# player_schema = StructType([
#     StructField("player_sk", IntegerType(), True),
#     StructField("player_id", IntegerType(), True),
#     StructField("player_name", StringType(), True),
#     StructField("dob", DateType(), True),
#     StructField("batting_hand", StringType(), True),
#     StructField("bowling_skill", StringType(), True),
#     StructField("country_name", StringType(), True)
# ]) 


# player_df = spark.read.schema(player_schema).format("csv").option("header","true").load("dbfs:/FileStore/tables/Player.csv")

# # COMMAND ----------

# player_df.show()

# # COMMAND ----------

# player_match_schema = StructType([
#     StructField("player_match_sk", IntegerType(), True),
#     StructField("playermatch_key", DecimalType(10, 2), True),  # Adjust precision and scale as required
#     StructField("match_id", IntegerType(), True),
#     StructField("player_id", IntegerType(), True),
#     StructField("player_name", StringType(), True),
#     StructField("dob", DateType(), True),
#     StructField("batting_hand", StringType(), True),
#     StructField("bowling_skill", StringType(), True),
#     StructField("country_name", StringType(), True),
#     StructField("role_desc", StringType(), True),
#     StructField("player_team", StringType(), True),
#     StructField("opposit_team", StringType(), True),
#     StructField("season_year", IntegerType(), True),  # PySpark does not have a YearType; IntegerType is used
#     StructField("is_manofthematch", BooleanType(), True),
#     StructField("age_as_on_match", IntegerType(), True),
#     StructField("isplayers_team_won", BooleanType(), True),
#     StructField("batting_status", StringType(), True),
#     StructField("bowling_status", StringType(), True),
#     StructField("player_captain", StringType(), True),
#     StructField("opposit_captain", StringType(), True),
#     StructField("player_keeper", StringType(), True),
#     StructField("opposit_keeper", StringType(), True)
# ])


# player_match_df = spark.read.schema(player_match_schema).format("csv").option("header","true").load("dbfs:/FileStore/tables/Player_match.csv")


# # COMMAND ----------


# team_schema = StructType([
#     StructField("team_sk", IntegerType(), True),
#     StructField("team_id", IntegerType(), True),
#     StructField("team_name", StringType(), True)
# ])
# team_df = spark.read.schema(team_schema).format("csv").option("header","true").load("dbfs:/FileStore/tables/Team.csv")


# # COMMAND ----------

# team_df.show()

# # COMMAND ----------

# ball_by_ball_df = ball_by_ball_df.filter((col("wides")==0)& (col("noballs")==0))

# #Aggregation:Calculate the total and average runs scored in each match and ining
# total_and_avg_runs = ball_by_ball_df.groupBy("match_id","innings_no").agg(sum("runs_scored").alias("total_runs"),avg("runs_scored").alias("average_runs"))

# # COMMAND ----------


# total_and_avg_runs.display()

# # COMMAND ----------

# #Window Function: Calculate runnning total of runs in each match for each over

# windowSpec = Window.partitionBy("match_id","innings_no").orderBy("over_id")

# ball_by_ball_df = ball_by_ball_df.withColumn("runninng_total_runs",sum("runs_scored").over(windowSpec))

# # COMMAND ----------

#  #Conditional column:Flag for high impact balls (either a wicket or more than 6 runs including extras)

# ball_by_ball_df = ball_by_ball_df.withColumn("high_impact",when((col("runs_scored")+col("extra_runs")>6) | (col("bowler_wicket")==True),True).otherwise(False))

# # COMMAND ----------

# from pyspark.sql.functions import year, month, dayofmonth, when, col

# # Extracting year, month, and day from the match date for more detailed time-based analysis
# match_df = match_df.withColumn("year", year("match_date"))
# match_df = match_df.withColumn("month", month("match_date"))
# match_df = match_df.withColumn("day", dayofmonth("match_date"))

# # High margin win: categorizing win margins into "High", "Medium", and "Low"
# match_df = match_df.withColumn(
#     "win_margin_category",
#     when(col("win_margin") >= 100, "High")
#     .when((col("win_margin") >= 50) & (col("win_margin") < 100), "Medium")
#     .otherwise("Low")
# )

# # Adding a column to check if the toss winner is also the match winner
# match_df = match_df.withColumn(
#     "toss_match_winner",
#     when(col("toss_winner") == col("match_winner"), "Yes").otherwise("No")
# )

# match_df.show(2)


# # COMMAND ----------

# from pyspark.sql.functions import lower , regexp_replace

# #Normalize and clean player names
# player_df = player_df.withColumn("player_name",lower(regexp_replace("player_name", "[^a-zA-Z0-9]", "")))

# #Handle missing values in "batting_hand"and"bowling_skill" with a default "unknown"
# players_df = player_df.na.fill({"batting_hand":"nuknown","bowling_skill":"unknown"})

# #Categorizing players based on batting hand
# player_df = player_df.withColumn("batting_style",
#                                  when(col("batting_hand").contains("left"), "Left-Handed").otherwise("Right-Handed")
# )
# player_df.show(2)

# # COMMAND ----------

# from pyspark.sql.functions import col,when,current_date,expr

# #Add a 'veteran_status'column based on player age
# player_match_df = player_match_df.withColumn("veteran_status",
#                                             when(col("age_as_on_match") >=35,"Veteran").otherwise("Non-Veteran") 
# )

# #Dynamic column to calculate years since debut
# player_match_df = player_match_df.withColumn("years_since_debut",(year(current_date())- col("season_year"))
# )

# player_match_df.display()

# # COMMAND ----------

# ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
# match_df.createOrReplaceTempView("match")
# player_df.createOrReplaceTempView("player")
# player_match_df.createOrReplaceTempView("player_match")
# team_df.createOrReplaceTempView("team")

# # COMMAND ----------

# top_scoring_batsmen_per_season = spark.sql("""
# Select
# p.player_name,
# m.season_year ,
# Sum(b.runs_scored)As total_runs
# from ball_by_ball b
# join match m on b.match_id = m.match_id
# join player_match pm on m.match_id = pm.match_id and b.striker = pm.player_id
# join player p on p.player_id  = pm.player_id
# GROUP BY p.player_name,m.season_year
# ORDER BY m.season_year,total_runs desc
# """)

# # COMMAND ----------

# top_scoring_batsmen_per_season.show(5)

# # COMMAND ----------

# economical_bowlers_powerplay = spark.sql("""
# select p.player_name,
# avg(b.runs_scored) as avg_runs_per_ball,
# count(b.bowler_wicket) as total_wickets
# from ball_by_ball  b
# join player_match pm on b.match_id = pm.match_id and b.bowler  = pm.player_id
# join player p on pm.player_id = p.player_id
# where b.over_id <=6
# group by p.player_name
# having count(*) > 1
# order by avg_runs_per_ball,total_wickets desc
# """)

# economical_bowlers_powerplay.show()
                                       

# # COMMAND ----------

# toss_impact_individual_matches = spark.sql("""
# select m.match_id, m.toss_winner, m.toss_name, m.match_winner,    
#         Case when m.toss_winner  = m.match_winner then 'Won' Else 'Lose' End as match_outcome
# from match m
# where m.toss_name Is Not null
# Order by m.match_id
# """)                                

# toss_impact_individual_matches.show()

# # COMMAND ----------

# average_runs_in_wins = spark.sql("""
# select p.player_name, avg(b.runs_scored) as avg_runs_in_wins, count(*) as innings_played
# from ball_by_ball b  
# join player_match pm on b.match_id = pm.match_id and b.striker = pm.player_id
# join player p on pm.player_id = p.player_id
# join match m on pm.match_id = m.match_id
# where m.match_winner = pm.player_team  
# group by p.player_name
# order by avg_runs_in_wins desc
# """)                            

# average_runs_in_wins.show()

# # COMMAND ----------

# import matplotlib.pyplot as plt

# # COMMAND ----------

# economical_bowlers_pd = economical_bowlers_powerplay.toPandas()

# plt.figure(figsize=(12,8))
# top_economical_bowlers = economical_bowlers_pd.nsmallest(10, 'avg_runs_per_ball') 
# plt.bar(top_economical_bowlers['player_name'],top_economical_bowlers['avg_runs_per_ball'], color = 'skyblue')
# plt.xlabel('Bowler Name')
# plt.ylabel('Average Runs per Ball')
# plt.title('Most Economical Bowlers in Powerplay Overs (Top 10)')
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.show()

# # COMMAND ----------

# import seaborn as sns

# # COMMAND ----------

# toss_impact_pd = toss_impact_individual_matches.toPandas()

# #Creating a countplot to show win/loss after winning toss
# plt.figure(figsize=(10,6))
# sns.countplot(x='toss_winner', hue='match_outcome',data=toss_impact_pd)
# plt.title('Impact of Winning Toss on Match Outcomes')
# plt.xlabel('Toss Winner')
# plt.ylabel('Number of Matches')
# plt.legend(title='Match Outcome')
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.show()

# # COMMAND ----------

# scores_by_venue = spark.sql("""
# select venue_name,avg(total_runs) as average_score, Max(total_runs) as hights_score
# from (
#     select ball_by_ball.match_id ,match.venue_name,sum(runs_scored) as total_runs
#     from ball_by_ball
#     join match on ball_by_ball.match_id = match.match_id
#     group by ball_by_ball.match_id,match.venue_name
# )
# group by venue_name
# order by average_score desc
# """)

# scores_by_venue.show()



# # COMMAND ----------

# #Convert to Pandas DataFrame
# scores_by_venue_pd = scores_by_venue.toPandas()

# plt.figure(figsize=(14,8))
# sns.barplot(x='average_score', y='venue_name',data=scores_by_venue_pd)
# plt.title('Distribution of Scores by Venue')
# plt.xlabel('Average Score')
# plt.ylabel('Venue')
# plt.show()

# # COMMAND ----------

# team_toss_win_performance = spark.sql("""
# select team1,count(*) as matches_played,sum(CASE WHEN toss_winner = match_winner Then 1 ELSE 0 END)  as wins_after_toss 
# from match
# where toss_winner = team1   
# group by team1         
# order by wins_after_toss desc
# """)                       
