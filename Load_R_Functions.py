#stolen from https://stackoverflow.com/questions/55797564/how-to-import-r-packages-in-python
import rpy2
import rpy2.robjects.packages as rpackages
from rpy2.robjects.vectors import StrVector
from rpy2.robjects.packages import importr
utils = rpackages.importr('utils')
utils.chooseCRANmirror(ind=1)


#Install packages shit is BROKEN -> go to r and type ".libPaths()", 
# copy from the base path into the one used by rpy2 - sorry
#packnames = ("TSA", "forecast", "tsbox", "stats")
#utils.install_packages(StrVector(packnames))


#Load packages
base = importr("base")
TSA = importr("TSA",lib_loc=base._libPaths()[0])
forecast = importr("forecast",lib_loc=base._libPaths()[0])
stats = importr("stats",lib_loc=base._libPaths()[0])
tsbox = importr("tsbox",lib_loc=base._libPaths()[0])

if __name__== "__main__":
    print(base._libPaths()[0])
