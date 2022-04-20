#stolen from https://stackoverflow.com/questions/55797564/how-to-import-r-packages-in-python
import rpy2
import rpy2.robjects.packages as rpackages
from rpy2.robjects.vectors import StrVector
from rpy2.robjects.packages import importr
utils = rpackages.importr('utils')
utils.chooseCRANmirror(ind=1)


#Install packages
packnames = ("TSA")
utils.install_packages(StrVector(packnames))

#Load packages
base = importr("base")
TSA = importr("TSA",lib_loc=base._libPaths()[0])


