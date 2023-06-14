import rpy2.robjects.packages as rpackages
import rpy2.robjects as robjects
import pandas as pd


def func_initialise():
    # Set the package path
    package_path = "C:/Users/Lachlan/AppData/Local/R/win-library/4.3"

    # Activate the package in the R environment
    rpackages.importr("fitzRoy", lib_loc=package_path)

    print('Fitzroy Package imported')


def fetch_fixture(season, round_number, source=None, comp=None):
    # Construct the R code for calling fetch_fixture
    r_code = f'fetch_fixture(season = {season}, round_number = {round_number}'
    if source is not None:
        r_code += f', source = "{source}"'
    if comp is not None:
        r_code += f', comp = "{comp}"'
    r_code += ')'

    # Call fetch_fixture function
    fixture_data = robjects.r(r_code)
    # Extract each column from the R data frame
    columns = list(fixture_data.colnames)
    data = {col: fixture_data.rx2(col) for col in columns}
    # Convert to Pandas DataFrame
    pandas_df = pd.DataFrame(data)

    # Print the dataframe
    print(pandas_df)


def fetch_player_stats(season, round_number=None, source=None, comp=None):
    # Construct the R code for calling fetch_player_stats
    r_code = f'fetch_player_stats({season}'
    if round_number is not None:
        r_code += f', round_number = "{round_number}"'
    if source is not None:
        r_code += f', source = "{source}"'
    if comp is not None:
        r_code += f', comp = "{comp}"'
    r_code += ')'

    # Call fetch_player_stats function
    player_data = robjects.r(r_code)

    # Convert to Pandas DataFrame
    pandas_df = pd.DataFrame(player_data)

    # Print the dataframe
    print(pandas_df)
