import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Schema4 {
    static private final String[] languages = {"EN", "AR", "FR", "DE", "ES"};
    static private final String[] countries = {"US", "EG", "FR", "DE", "GB"};
    static private final String[] genres = {"Action", "Comedy", "Drama", "Fantasy", "Horror", "Mystery", "Romance",
            "Thriller", "Western"};
    static private final String[] genders = {"M", "F"};

    private static final int INSERT_MOVIE = 0;
    private static final int INSERT_REVIEWER = 1;
    private static final int INSERT_GENRES = 2;
    private static final int INSERT_ACTOR = 3;
    private static final int INSERT_DIRECTOR = 4;
    private static final int INSERT_MOVIE_DIRECTION = 5;
    private static final int INSERT_MOVIE_CAST = 6;
    private static final int INSERT_MOVIE_GENRES = 7;
    private static final int INSERT_RATING = 8;

    private final PreparedStatement[] preparedStatements = new PreparedStatement[9];

    public Schema4(Connection connection) throws SQLException {
        preparedStatements[INSERT_MOVIE] = connection.prepareStatement("INSERT INTO Movie(mov_id,mov_title,mov_year,mov_time,mov_lang,mov_dt_rel,mov_rel_country) VALUES(?,?,?,?,?,?,?);");
        preparedStatements[INSERT_REVIEWER] = connection.prepareStatement("INSERT INTO reviewer(rev_id,rev_name) VALUES(?,?);");
        preparedStatements[INSERT_GENRES] = connection.prepareStatement("INSERT INTO Genres(gen_id,gen_title) VALUES(?,?);");
        preparedStatements[INSERT_ACTOR] = connection.prepareStatement("INSERT INTO Actor(act_id,act_fname,act_lname,act_gender) VALUES(?,?,?,?);");
        preparedStatements[INSERT_DIRECTOR] = connection.prepareStatement("INSERT INTO Director(dir_id,dir_fname,dir_lname) VALUES(?,?,?);");
        preparedStatements[INSERT_MOVIE_DIRECTION] = connection.prepareStatement("INSERT INTO movie_direction(dir_id,mov_id) VALUES(?,?);");
        preparedStatements[INSERT_MOVIE_CAST] = connection.prepareStatement("INSERT INTO movie_cast(act_id,mov_id,role) VALUES(?,?,?);");
        preparedStatements[INSERT_MOVIE_GENRES] = connection.prepareStatement("INSERT INTO movie_genres(mov_id,gen_id) VALUES(?,?);");
        preparedStatements[INSERT_RATING] = connection.prepareStatement("INSERT INTO rating(mov_id,rev_id,rev_stars,num_o_ratings) VALUES(?,?,?,?);");
    }

    // CREATE TABLE Movie(mov_id INT PRIMARY KEY, mov_title CHAR(50), mov_year INT,
    // mov_time INT, mov_lang CHAR(50), mov_dt_rel date, mov_rel_country CHAR(5));
    private void insertMovie(int ID, String title, int year, int time, String lang, Date releaseDate,
                             String movieCountry) throws SQLException {


        PreparedStatement pstmt = preparedStatements[INSERT_MOVIE];

        pstmt.setInt(1, ID);
        pstmt.setString(2, title);
        pstmt.setInt(3, year);
        pstmt.setInt(4, time);
        pstmt.setString(5, lang);
        pstmt.setDate(6, releaseDate);
        pstmt.setString(7, movieCountry);
        pstmt.executeUpdate();
    }

    // CREATE TABLE Reviewer(rev_id INT PRIMARY KEY, rev_name CHAR(30));
    private void insertReviewer(int ID, String name) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_REVIEWER];
        pstmt.setInt(1, ID);
        pstmt.setString(2, name);
        pstmt.executeUpdate();
    }

    // CREATE TABLE Genres(gen_id INT PRIMARY KEY, gen_title CHAR(20));
    private void insertGenres(int ID, String title) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_GENRES];
        pstmt.setInt(1, ID);
        pstmt.setString(2, title);
        pstmt.executeUpdate();
    }

    // CREATE TABLE Actor(act_id INT PRIMARY KEY, act_fname CHAR(20), act_lname
    // CHAR(20), act_gender CHAR(1));
    public void insertActor(int ID, String fName, String lName, String gender)
            throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_ACTOR];

        pstmt.setInt(1, ID);
        pstmt.setString(2, fName);
        pstmt.setString(3, lName);
        pstmt.setString(4, gender);
        pstmt.executeUpdate();
    }

    // CREATE TABLE Director(dir_id INT PRIMARY KEY, dir_fname CHAR(20), dir_lname
    // CHAR(20));
    public void insertDirector(int ID, String fName, String lName) throws SQLException {

        PreparedStatement pstmt = preparedStatements[INSERT_DIRECTOR];

        pstmt.setInt(1, ID);
        pstmt.setString(2, fName);
        pstmt.setString(3, lName);
        pstmt.executeUpdate();
    }

    // CREATE TABLE movie_direction(dir_id INT REFERENCES Director, mov_id INT
    // REFERENCES Movie, PRIMARY KEY(dir_id,mov_id));
    private void insertMovieDirection(int ID, int movieID) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_MOVIE_DIRECTION];

        pstmt.setInt(1, ID);
        pstmt.setInt(2, movieID);
        pstmt.executeUpdate();
    }
    // CREATE TABLE movie_cast(act_id INT REFERENCES Actor, mov_id INT REFERENCES
    // Movie, role CHAR(30), PRIMARY KEY(act_id, mov_id));

    private void insertMovieCast(int actorID, int movieID, String role) throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_MOVIE_CAST];

        pstmt.setInt(1, actorID);
        pstmt.setInt(2, movieID);
        pstmt.setString(3, role);

        try {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            if (!e.getSQLState().equals("23505")) {
                throw e;
            }
        }
    }

    // CREATE TABLE movie_genres(mov_id INT REFERENCES Movie, gen_id INT REFERENCES
    // genres, PRIMARY KEY(mov_id,gen_id));
    private void insertMovieGenres(int movieID, int genreID) throws SQLException{
        PreparedStatement pstmt = preparedStatements[INSERT_MOVIE_GENRES];

        pstmt.setInt(1, movieID);
        pstmt.setInt(2, genreID);

        pstmt.executeUpdate();
    }

    // CREATE TABLE Rating(mov_id INT REFERENCES Movie, rev_id INT REFERENCES
    // Reviewer, rev_stars INT, num_o_ratings INT, PRIMARY KEY(mov_id,rev_id));
    private void insertRating(int movieID, int reviewID, int stars, int rating)
            throws SQLException {
        PreparedStatement pstmt = preparedStatements[INSERT_RATING];

        pstmt.setInt(1, movieID);
        pstmt.setInt(2, reviewID);
        pstmt.setInt(3, stars);
        pstmt.setInt(4, rating);

        pstmt.executeUpdate();
    }

    ////////////////////////////////////////////////////////// Data Population
    ////////////////////////////////////////////////////////// Methods
    ////////////////////////////////////////////////////////// //////////////////////////////////////////////////////////
    private void populateMovie() throws SQLException {
        System.out.println("Populating Movie");
        for (int i = 1; i <= 20000; i++) {
            int random_time = AllSchemas.random(1, 13);
            String language = AllSchemas.randomElement(languages);
            String country = AllSchemas.randomElement(countries);
            insertMovie(i, "Movie" + i, 2000 + i, random_time, language, AllSchemas.randomDate(), country);
        }

        // specified movie #1

        insertMovie(20001, "Annie Hall", 1977, 4, "EN", Date.valueOf("1977-04-20"), "US");

        for (int i = 20002; i <= 80000; i++) {
            int random_time = AllSchemas.random(1, 13);
            String language = AllSchemas.randomElement(languages);
            String country = AllSchemas.randomElement(countries);
            insertMovie(i, "Movie" + i, 2000 + i, random_time, language, AllSchemas.randomDate(), country);
        }

        // specified movie #2

        insertMovie(80001, "Eyes Wide Shut", 1999, 7, "EN", Date.valueOf("1999-09-03"), "US");

        for (int i = 80002; i <= 100000; i++) {
            int random_time = AllSchemas.random(1, 13);
            String language = AllSchemas.randomElement(languages);
            String country = AllSchemas.randomElement(countries);
            insertMovie(i, "Movie" + i, 2000 + i, random_time, language, AllSchemas.randomDate(), country);
        }
    }

    private void populateReviewer() throws SQLException {
        System.out.println("Populating Reviewer");
        for (int i = 1; i <= 10000; i++) {
            insertReviewer(i, AllSchemas.randomName());
        }
    }

    private void populateGenres() throws SQLException {
        System.out.println("Populating Genres");
        for (int i = 1; i <= genres.length; i++) {
            insertGenres(i, genres[i - 1]);
        }
    }

    private void populateActor() throws SQLException {
        System.out.println("Populating Actor");
        for (int i = 1; i <= 120000; i++) {
            String gender = AllSchemas.randomElement(genders);

            insertActor(i, AllSchemas.randomFirstName(), AllSchemas.randomElement(AllSchemas.lastNames), gender);
        }
    }

    private void populateDirector() throws SQLException {
        System.out.println("Populating Director");
        for (int i = 1; i <= 6000; i++) {
            if (i == 3000) {
                insertDirector(i, "Woddy", "Allen"); // insert woddy allen in between
            } else {
                insertDirector(i, AllSchemas.randomFirstName(), AllSchemas.randomElement(AllSchemas.lastNames));
            }
        }
    }

    private void populateMovieDirection() throws SQLException {
        System.out.println("Populating Movie Direction");
        for (int i = 1; i <= 350; i++) {
            insertMovieDirection(3000, i);
        }
        for (int i = 351; i <= 5999; i++) {
            if (i == 3000) {
                continue;
            }
            insertMovieDirection(i, i);
        }
        insertMovieDirection(6000, 80001);
        insertMovieDirection(5999, 80002);
        insertMovieDirection(5998, 80005);
        insertMovieDirection(5997, 20001);
        insertMovieDirection(5996, 50000);
    }

    private void populateMovieCast() throws SQLException {
        System.out.println("Populating Movie Cast");
        List<Integer> Movies_Shuffled = new ArrayList<>();
        for (int i = 1; i <= 100000; i++) {
            Movies_Shuffled.add(i); // before shuffle
        }
        Collections.shuffle(Movies_Shuffled, AllSchemas.rand); // after shuffle

        // random actors
        for (int j = 1; j <= 1000; j++) {
            int movieId = Movies_Shuffled.get(j);
            if (movieId == 80001 || movieId == 20001)
                continue;
            insertMovieCast(j, movieId, "Role" + j);

        }

        // Actors who act in eyes wide shut
        for (int j = 1000; j <= 1150; j++) {
            insertMovieCast(j, 80001, "Role" + j);
            int movieId1 = AllSchemas.random(1, 100000);
            if (movieId1 != 20001 && movieId1 != 80001) {
                insertMovieCast(j, movieId1, "Role" + j);
            }

            movieId1 = AllSchemas.random(1, 100000);
            if (movieId1 != 20001 && movieId1 != 80001) {
                insertMovieCast(j, movieId1, "Role" + j);
            }
        }

        for (int j = 1; j <= 222; j++) {
            insertMovieCast(j, 20001, "Role" + j);
        }

        for (int j = 1024; j <= 1124; j++) {
            insertMovieCast(j, 80002, "Role" + j);
            insertMovieCast(j, 80005, "Role" + j);
        }
    }

    private void populateMovieGenres() throws SQLException {
        System.out.println("Populating Movie Genres");
        for (int i = 1; i <= 100000; i++) {
            int random_genre_id = AllSchemas.random(1, genres.length + 1);
            insertMovieGenres(i, random_genre_id);
        }
    }

    private void populateRating() throws SQLException {
        System.out.println("Populating Rating");
        for (int i = 1; i <= 10000; i++) {
            for (int j = 1; j <= 10; j++) {
                int rating_Stars = AllSchemas.random(1, 5);
                int zero_rate = AllSchemas.random(1, 11);
                insertRating(j, i, rating_Stars, zero_rate);
            }
        }
    }

    public static void insertSchema4(Connection connection) throws SQLException {
        Schema4 schema4 = new Schema4(connection);
        schema4.populateMovie();
        schema4.populateReviewer();
        schema4.populateGenres();
        schema4.populateActor();
        schema4.populateDirector();
        schema4.populateMovieDirection();
        schema4.populateMovieCast();
        schema4.populateMovieGenres();
        schema4.populateRating();
    }
}