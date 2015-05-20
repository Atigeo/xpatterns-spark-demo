CREATE DATABASE IF NOT EXISTS demo_summit;

USE demo_summit;

drop table if exists USER;
drop table if exists RATING;
drop table if exists MOVIES;
drop table if exists similarity;


CREATE TABLE USER(userID INT, age INT, gender STRING,occupation STRING, zip_code STRING ) ROW FORMAT
              DELIMITED FIELDS TERMINATED BY '|'
              LINES TERMINATED BY '\n'            
              STORED AS TEXTFILE;

CREATE TABLE RATING(userID INT, movieID INT, rating Double,time INT) ROW FORMAT
              DELIMITED FIELDS TERMINATED BY '\t'
              LINES TERMINATED BY '\n'        
              STORED AS TEXTFILE;

CREATE TABLE MOVIES(movieID INT, movieName STRING, release_date TIMESTAMP, video_release_date TIMESTAMP, imdb_link STRING,type_unknown BOOLEAN, type_Action BOOLEAN, type_Adventure BOOLEAN, type_Animation BOOLEAN, type_Children BOOLEAN, type_Crime BOOLEAN,type_Documentary BOOLEAN,type_Drama BOOLEAN,type_Fantasy BOOLEAN, type_FilmNoir BOOLEAN, type_Horror BOOLEAN, type_Musical BOOLEAN,type_Mystery BOOLEAN, type_Romance BOOLEAN,type_SciFi BOOLEAN,type_War BOOLEAN,type_Western BOOLEAN ) ROW FORMAT       
 DELIMITED FIELDS TERMINATED BY '|'
              LINES TERMINATED BY '\n' 
              STORED AS TEXTFILE;
            
 CREATE EXTERNAL TABLE similarity(movieID1 INT, movieID2 INT, correlation_factor FLOAT) ROW FORMAT
               DELIMITED FIELDS TERMINATED BY ','
               LINES TERMINATED BY '\n' 
               STORED AS TEXTFILE
               LOCATION '/user/ubuntu/datasets/demoUser/movielens_100k_s3/result.data';


LOAD DATA INPATH '/user/ubuntu/datasets/demoUser/movielens_100k_s3/clean/u.item' OVERWRITE INTO TABLE movies;
LOAD DATA INPATH '/user/ubuntu/datasets/demoUser/movielens_100k_s3/clean/u.data' OVERWRITE INTO TABLE rating;
LOAD DATA INPATH '/user/ubuntu/datasets/demoUser/movielens_100k_s3/clean/u.user' OVERWRITE INTO TABLE user;

drop table if exists similarity_peruser_intermed;

create table similarity_peruser_intermed as 
	select users.userid, similarity.movieid1, similarity.movieid2, similarity.correlation_factor
	from
	  ( select r.userid, r.movieid from rating r where r.rating > 4 ) users 
	     JOIN similarity ON ( users.movieid = similarity.movieid1 );

drop table if exists similarity_peruser;

create table similarity_peruser as 
select * from similarity_peruser_intermed s
WHERE s.movieid1 <> s.movieid2;

drop table if exists similarity_peruser_notseen_intermed;

create table similarity_peruser_notseen_intermed as
	select similarity_peruser.userid, similarity_peruser.movieid1, similarity_peruser.movieid2, similarity_peruser.correlation_factor, rating.rating
	from similarity_peruser left outer join rating
	on similarity_peruser.userid = rating.userid and similarity_peruser.movieid2 = rating.movieid;

drop table if exists similarity_peruser_notseen;

create table similarity_peruser_notseen as select * from similarity_peruser_notseen_intermed where rating is not Null;

drop table if exists similarity_per_user_not_seen_movies_max_correlation;

create table similarity_per_user_not_seen_movies_max_correlation (userid Int, movieid1 Int, movieid2 Int, correlation Double) stored as sequencefile

insert into table similarity_per_user_not_seen_movies_max_correlation select maxim.userid, maxim.movieid1, similarity_peruser_notseen.movieid2, maxim.correlation
  from
   (select c.userid, c.movieid1, max(c.correlation_factor) as correlation
   from similarity_peruser_notseen c group by c.userid, c.movieid1) maxim 
   join similarity_peruser_notseen on (maxim.userid = similarity_peruser_notseen.userid and
   maxim.movieid1 = similarity_peruser_notseen.movieid1 and maxim.correlation = similarity_peruser_notseen.correlation_factor );

drop table if exists user_predictions;
create table user_predictions (userid Int, moviename String) stored as sequencefile;

insert into table user_predictions
	select distinct t.userid, t.moviename from 	
	(SELECT sim.userid, B.moviename
		FROM similarity_per_user_not_seen_movies_max_correlation sim JOIN movies B on
			sim.movieid2 = B.movieid) t;

drop table if exists similarity_peruser_notseen_intermed;
drop table if exists similarity_peruser_intermed;
drop table if exists similarity_peruser;
drop table if exists similarity_peruser_notseen;

