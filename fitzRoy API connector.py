import pandas as pd
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
import rpy2.robjects.packages as rpackages
import rpy2.robjects.packages as rpackages

pandas2ri.activate()
fitzRoy = rpackages.importr("fitzRoy")
