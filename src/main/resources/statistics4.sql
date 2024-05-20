CREATE EXTENSION btree_gin;
CREATE STATISTICS movie_stats ON mov_id, mov_title  FROM movie;
CREATE STATISTICS director_stats ON dir_id, dir_fname, dir_lname  FROM director;
CREATE STATISTICS genres_stats ON gen_id, gen_title  FROM genres;
CREATE STATISTICS movie_cast_stats ON act_id, mov_id  FROM movie_cast;
CREATE STATISTICS movie_direction_stats ON dir_id, mov_id  FROM movie_direction;