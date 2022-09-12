
# minigpkg

This repo is a proof-of-concept [GeoPackage](https://www.geopackage.org/) IO library based on [nanoarrow](https://github.com/apache/arrow-nanoarrow). Right now I'm just using it as a real world application of the nanoarrow C library to make sure it scales to input from some real-world data.

You can configure and build the project using CMake. You'll need sqlite3 installed (cmake can find most system installations of sqlite3 without any special configuration).

```bash
git clone https://github.com/paleolimbot/minigpkg.git
cd minigpkg
mkdir build && cd build
cmake ..
cmake --build .
```

Currently the only non-library thing you can do is benchmark the time it takes to loop over result in SQLite3 vs. building the array:

```bash
# cd minigpkg/build
curl -L https://github.com/paleolimbot/geoarrow-data/releases/download/v0.0.1/nshn_basin_line.gpkg --output nshn_basin_line.gpkg
./nanoarrow_sqlite3_bench nshn_basin_line.gpkg "SELECT * from nshn_basin_line" 
#> Running query SELECT * from nshn_basin_line
#> ...the magic number is 3
#> ...looped through result in 0.001406 seconds
#> Building Arrow result for query SELECT * from nshn_basin_line
#> ...processed 255 rows in 0.000987 seconds
```
