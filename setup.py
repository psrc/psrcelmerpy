# Import required functions
from setuptools import setup, find_packages

# Call setup function
setup(
    author="Chris Peak",
    description="A package for interacting with Elmer, PSRC's data warehouse.",
    name="psrcelmerpy",
    packages=find_packages(include=["psrcelmerpy", "psrcelmerpy.*"]),
    version="0.1.0",
    install_requires=['pandas',
                      #'scipy==1.10.1',
                      'pyodbc',
                      #'urllib',
                      'sqlalchemy',
                      'geopandas',
                      'shapely']
)
