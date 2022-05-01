-- SQL Script to convert EPL match data to League Table

-- Note: Tables beginning with # are temporary tables
DROP TABLE IF EXISTS #Match;

-- For simplicity, choose only the columns we need to build the league table
SELECT
    Date,
    HomeTeam,
    AwayTeam,
    FullTimeHomeGoals AS HomeGoals,
    FullTimeAwayGoals AS AwayGoals
INTO
    #Match
FROM dbo.EPLMatch;


SELECT Date, HomeTeam, AwayTeam, HomeGoals, AwayGoals FROM #Match;

-- *** Step 1:  Split and reshape

DROP TABLE IF EXISTS #HomeStats;
SELECT
    Date,
    HomeTeam AS Team,
    AwayTeam AS OpposingTeam,
    HomeGoals AS GF,
    AwayGoals AS GA
INTO
    #HomeStats
FROM #Match;

DROP TABLE IF EXISTS #AwayStats;
SELECT
    Date,
    AwayTeam AS Team,
    HomeTeam AS OpposingTeam,
    AwayGoals AS GF,
    HomeGoals AS GA
INTO
    #AwayStats
FROM #Match;

SELECT Date, Team, OpposingTeam, GF, GA FROM #HomeStats;
SELECT Date, Team, OpposingTeam, GF, GA FROM #AwayStats;

-- *** Step 2:  Append

DROP TABLE IF EXISTS #TeamStats;

SELECT Date, Team, OpposingTeam, GF, GA INTO #TeamStats FROM #HomeStats
UNION
SELECT Date, Team, OpposingTeam, GF, GA FROM #AwayStats;

SELECT Date, Team, OpposingTeam, GF, GA FROM #TeamStats;

-- *** Step 3:  Calculate result column

-- Firstly, calculate the Result column
DROP TABLE IF EXISTS #TeamStatsWithResult;

SELECT
    Date,
    Team,
    OpposingTeam,
    GF,
    GA,
    CASE WHEN GF > GA THEN 'W' WHEN GF = GA THEN 'D' ELSE 'L' END Result
INTO
    #TeamStatsWithResult
FROM #TeamStats;

SELECT Date, Team, OpposingTeam, GF, GA, Result FROM #TeamStatsWithResult;

-- Next, calculate the other columns, simpler now that we have the Result column

DROP TABLE IF EXISTS #TeamStatsFull;

SELECT
    Date,
    Team,
    OpposingTeam,
    GF,
    GA,
    Result,
    CASE Result WHEN 'W' THEN 3 WHEN 'D' THEN 1 ELSE 0 END Points,
    CASE Result WHEN 'W' THEN 1 ELSE 0 END Won,
    CASE Result WHEN 'D' THEN 1 ELSE 0 END Drawn,
    CASE Result WHEN 'L' THEN 1 ELSE 0 END Lost
INTO
    #TeamStatsFull
FROM #TeamStatsWithResult;

SELECT Date, Team, OpposingTeam, GF, GA, Result, Points, Won, Drawn, Lost FROM #TeamStatsFull;

-- *** Step 4:  Group By, Aggregate

DROP TABLE IF EXISTS #LeagueTable;
SELECT
    Team,
    COUNT(*) AS Played,
    SUM(Won) AS Won,
    SUM(Drawn) AS Drawn,
    SUM(Lost) AS Lost,
    SUM(GF) AS GF,
    SUM(GA) AS GA,
    SUM(GF) - SUM(GA) AS GD, -- *** Step 5:  Calculate (again)
    SUM(Points) AS Points
INTO
    #LeagueTable
FROM #TeamStatsFull
GROUP BY Team;

SELECT Team, Played, Won, Drawn, Lost, GF, GA, GD, Points FROM #LeagueTable;

-- *** Step 6:  Sort
SELECT
    Team,
    Played,
    Won,
    Drawn,
    Lost,
    GF,
    GA,
    GD,
    Points
FROM #LeagueTable
ORDER BY Points DESC,
         GD DESC;



/*
ALTER TABLE dbo.EPLMatch ALTER COLUMN Date DATE NOT NULL
*/

