# Build League Table.R
# Convert EPL Match data into league table

library(tidyverse) # (ggplot, tidyr, dplyr) - load data from CSV, tidy, transform and plot
library(readxl) # read from Excel

# Load the data from an Excel file
match_xlsx_filepath = "C:/Users/markw/OneDrive - Zomalex Ltd/Public/LBAG Online/Rosetta/EPL Data.xlsx"
match_full <-
  read_xlsx(path = match_xlsx_filepath, sheet = "EPL Match")

match <-
  match_full %>%
  select(
    Date,
    HomeTeam = `Home Team`,
    AwayTeam = `Away Team`,
    HomeGoals = `Full Time Home Goals`,
    AwayGoals = `Full Time Away Goals`
  )

# Step 1: Split and reshape
home_stats <-
  match %>%
  select(
    Date,
    Team = HomeTeam,
    OpposingTeam = AwayTeam,
    GF = HomeGoals,
    GA = AwayGoals
  )

away_stats <-
  match %>%
  select(
    Date,
    Team = AwayTeam,
    OpposingTeam = HomeTeam,
    GF = AwayGoals,
    GA = HomeGoals
  )

# Step 2: Append
team_stats <- home_stats %>%  union(away_stats)

# Step 3: Calculate
team_stats <-
  team_stats %>%
  mutate(
    Result = if_else(GF > GA, "W", if_else (GF == GA, "D", "L")),
    Points = if_else(Result == "W", 3, if_else(Result == "D", 1, 0)),
    Won = if_else(Result == "W", 1, 0),
    Drawn = if_else(Result == "D", 1, 0),
    Lost = if_else(Result == "L", 1, 0)
  )

# Step 4: Group By, Aggregate
league_table <-
  team_stats %>%
  group_by(Team) %>%
  summarise(
    Played = n(),
    Won = sum(Won),
    Drawn = sum(Drawn),
    Lost = sum(Lost),
    GF = sum(GF),
    GA = sum(GA),
    Points = sum(Points)
  ) %>%
  ungroup() %>%
  mutate(GD = GF - GA)  %>%  # Step 5: Calculate (again) GD = GF - GA
  arrange(-Points,-GD) # Step 6: Sort
