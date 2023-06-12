import rpy2.robjects as robjects
from rpy2.robjects import packages

r_code = """
.libPaths()
"""

lib_paths = robjects.r(r_code)
fitzRoy_path = None

for path in lib_paths:
    if "fitzRoy" in packages.isinstalled("fitzRoy", lib_loc=path):
        fitzRoy_path = path
        break

if fitzRoy_path is not None:
    fitzRoy = packages.importr("fitzRoy", lib_loc=fitzRoy_path)
    print("fitzRoy package found at:", fitzRoy_path)
else:
    print("fitzRoy package not found.")
