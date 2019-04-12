#!/bin/bash
#SBATCH --partition=physical
#SBATCH --time=00:05:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1

module load Python/3.6.1-intel-2017.u2

time mpirun -np 8 python3 twitter.py bigTwitter.json # might need to use mpirun
