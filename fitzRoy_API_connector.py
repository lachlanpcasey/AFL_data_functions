from functions import fetch_fixture, func_initialise, fetch_player_stats
func_initialise()
df = fetch_player_stats(season=2020, round_number=2)
print(df.header)
