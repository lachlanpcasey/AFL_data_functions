# AFL_data_functions
This repo is made to use the "fitzRoy" package accessible in Python from R.
**How to use:**
1. Ensure you have R installed on your machine.
2. Download the fitzRoy package using install.package('fitzRoy') in the R command line. Do also do this for tidyverse and dplyr using the same command.
3. Find where the package is located on your machine, using find.pack('fitzRoy') in R. For example: C:/Users/Lachlan/AppData/Local/R/win-library/4.3
4. Paste this into requirements.py
5. In a python script, run:
```
from functions import func_initialise
func_initialise()
```
  This will import the correct R packages.

6. You can now use the functions described below within Python, and they will return a dataframe that can then be used for analysis.

**Functions:**

**func_initialise()**: Doesn't take any arguments. Imports the correct R packages needed for the other functions to work.

**fetch_fixture(season, round_number, source, comp)**: Returns the fixture for any season and round number. Source and comp can be None.

**fetch_player_stats(season, round_number, source, comp)**: Returns player stats for a season and round number. Source and comp can be None.

**Note**: Season can be input as a list
