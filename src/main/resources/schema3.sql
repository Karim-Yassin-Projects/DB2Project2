CREATE TABLE Sailors(sid INT PRIMARY KEY, sname CHAR(20), rating INT, age REAL);
CREATE TABLE Boat(bid INT PRIMARY KEY, bname CHAR(20), color CHAR(10));
CREATE TABLE Reserves(sid INT REFERENCES Sailors, bid INT REFERENCES Boat, day date, PRIMARY KEY(sid,bid));