import rpy2.robjects.packages as rpackages
import rpy2.robjects as robjects
import pandas as pd

# Set the package path
package_path = "C:/Users/Lachlan/AppData/Local/R/win-library/4.3"

# Activate the package in the R environment
rpackages.importr("fitzRoy", lib_loc=package_path)

# Call fetch_fixture function
fixture_data = robjects.r('fetch_fixture(season = 2020, comp = "AFLM")')

# Extract each column from the R data frame
columns = list(fixture_data.colnames)
data = {col: fixture_data.rx2(col) for col in columns}

# Convert to Pandas DataFrame
pandas_df = pd.DataFrame(data)

# Print the dataframe
print(pandas_df)
