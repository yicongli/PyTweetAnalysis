#!/bin/bash
#SBATCH --partition=cloud
#SBATCH --time=00:20:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1

module load Python/3.6.1-intel-2017.u2

time mpiexec -n 1 python3 twitter.py bigTwitter.json
