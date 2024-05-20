CREATE EXTENSION btree_gin;
CREATE STATISTICS sailors_stats ON sid, sname FROM Sailors;
CREATE STATISTICS boats_stats ON bid, color FROM Boat;
CREATE STATISTICS reserves_stats ON sid, bid FROM Reserves;