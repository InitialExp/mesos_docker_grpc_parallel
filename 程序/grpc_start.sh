#!/bin/bash
docker exec -itd $(cat masterContainer) mpiexec -n 5 -f /test_data/host -map-by machine python3 /test_data/mpi_hptuning.py
