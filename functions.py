import rpy2.robjects.packages as rpackages
import rpy2.robjects as robjects
import pandas as pd


def func_initialise():
    # Set the package path
    package_path = "C:/Users/Lachlan/AppData/Local/R/win-library/4.3"

    # Activate the package in the R environment
    rpackages.importr("fitzRoy", lib_loc=package_path)

    print('Fitzroy Package imported')

    rpackages.importr("fitzRoy", lib_loc=package_path)

    print('tidyverse package imported')

    rpackages.importr("dplyr", lib_loc=package_path)

    print('dplyr package imported')


def fetch_fixture(season=None, round_number=None, source=None, comp=None):
    # Construct the R code for calling fetch_fixture
    r_code = 'fetch_fixture('

    if season is not None:
        if isinstance(season, list):
            season_str = ', '.join(str(s) for s in season)
            r_code += f'season = c({season_str}), '
        else:
            r_code += f'season = {season}, '

    if round_number is not None:
        r_code += f'round_number = {round_number}, '

    if source is not None:
        r_code += f'source = "{source}", '

    if comp is not None:
        r_code += f'comp = "{comp}", '

    r_code = r_code.rstrip(', ')  # Remove trailing comma and space
    r_code += ')'

    # Call fetch_fixture function
    fixture_data = robjects.r(r_code)
    # Extract each column from the R data frame
    columns = list(fixture_data.colnames)
    data = {col: fixture_data.rx2(col) for col in columns}
    # Convert to Pandas DataFrame
    pandas_df = pd.DataFrame(data)

    # Print the dataframe
    return pandas_df


def fetch_player_stats(season=None, round_number=None, source=None, comp=None):
    # Construct the R code for calling fetch_player_stats
    r_code = 'fetch_player_stats('

    if season is not None:
        if isinstance(season, list):
            season_str = ', '.join(str(s) for s in season)
            r_code += f'season = c({season_str}), '
        else:
            r_code += f'season = {season}, '

    if round_number is not None:
        r_code += f'round_number = {round_number}, '

    if source is not None:
        r_code += f'source = "{source}", '

    if comp is not None:
        r_code += f'comp = "{comp}", '

    r_code = r_code.rstrip(', ')  # Remove trailing comma and space
    r_code += ')'

    # Call fetch_player_stats function
    player_stats_data = robjects.r(r_code)
    # Extract each column from the R data frame
    columns = list(player_stats_data.colnames)
    data = {col: player_stats_data.rx2(col) for col in columns}
    # Convert to Pandas DataFrame
    pandas_df = pd.DataFrame(data)

    # Print the dataframe
    return pandas_df


def fetch_results(season=None, round_number=None, source=None, comp=None):
    # Construct the R code for calling fetch_player_stats
    r_code = 'fetch_results('

    if season is not None:
        if isinstance(season, list):
            season_str = ', '.join(str(s) for s in season)
            r_code += f'season = c({season_str}), '
        else:
            r_code += f'season = {season}, '

    if round_number is not None:
        r_code += f'round_number = {round_number}, '

    if source is not None:
        r_code += f'source = "{source}", '

    if comp is not None:
        r_code += f'comp = "{comp}", '

    r_code = r_code.rstrip(', ')  # Remove trailing comma and space
    r_code += ')'

    # Call fetch_player_stats function
    player_stats_data = robjects.r(r_code)
    # Extract each column from the R data frame
    columns = list(player_stats_data.colnames)
    data = {col: player_stats_data.rx2(col) for col in columns}
    # Convert to Pandas DataFrame
    pandas_df = pd.DataFrame(data)

    # Print the dataframe
    return pandas_df
