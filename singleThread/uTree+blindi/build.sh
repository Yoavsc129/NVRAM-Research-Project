#!/bin/bash

g++ -O3 -march=native -std=c++20 -DNDEBUG -m64 -D_REENTRANT -fno-strict-aliasing -DINTEL -Wno-unused-value -Wno-format  -o ./main  ./main.cpp -lprofiler
