#!/bin/bash
#SBATCH --partition=cloud
#SBATCH --time=01:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1

module load Python/3.6.1-intel-2017.u2

time mpiexec -n 8 python3 twitter.py 