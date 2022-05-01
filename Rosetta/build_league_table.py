import pandas as pd
import numpy as np

# Import the match results into a pandas dataframe
df_match = pd.read_csv("../EPL Data 20-21.csv")


result_col_list = ('Team', 'Opposing Team', 'GF', 'GA')

#  Each match generates tow results - one for the home teams and one for teh away team
home_cols_list = ['Home Team', 'Away Team', 'Full Time Home Goals', 'Full Time Away Goals']
df_home = df_match.loc[:, home_cols_list]
df_home.columns = result_col_list # ('Team', 'Opposing Team', 'GF', 'GA')

# The columns are ordered from the away teams perspective
# e.g. for the away team , Goals For (GF) is the number of away goals
away_cols_list = ['Away Team', 'Home Team', 'Full Time Away Goals', 'Full Time Home Goals']
df_away = df_match.loc[:, away_cols_list]
df_away.columns =  result_col_list # ('Team', 'Opposing Team', 'GF', 'GA')

# This contains all results - two for each match - one for the home team and one for the away team
df_all = df_home.append(df_away)

# Calculate whether the match was won, drawn or lost by the team
df_all['won'] = np.where(df_all['GF'] > df_all['GA'], 1 , 0)
df_all['drawn'] = np.where(df_all['GF'] == df_all['GA'], 1 , 0)
df_all['lost'] = np.where(df_all['GF'] < df_all['GA'], 1 , 0)

#  Group by Teams to get the league table and the total won / drawn / lost
df_league = df_all.groupby(['Team']).sum()

#  A  match will either result ina win / drw / loss so summing these (since either 1 or 0) will return matches played
df_league['played'] = df_league['won'] + df_league['drawn'] + df_league['lost']
#  Tams get 3 points for a win, 1 for a draw and no points for a loss
df_league['points'] = 3 * df_league['won'] + df_league['drawn']

#  The goal difference (GD) 
df_league['GD'] = df_league['GF'] - df_league['GA']

# Sort by Points (high to low) and then by GD (also high to low)
df_league  = df_league.sort_values(by = ['points', 'GD'], ascending = False)

# Now that the league tabkle is in the proper order , assign positions
df_league['position'] = np.arange(len(df_league)) + 1

#  Put the columns in the usual exapected order
df_league = df_league.loc[:, ['position', 'played', 'won', 'drawn', 'lost', 'GF', 'GA', 'GD', 'points']]

#  Write out to an Excel files
df_league.to_excel("league_table.xlsx")