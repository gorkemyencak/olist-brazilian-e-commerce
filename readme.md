## Dynamic Programming-Based Courier Dispatch System

### Overview

This projects implements a $\textbf{\textit{courier dispatch optimization system}}$ using:
* Greedy insertion heuristics
* Dynamic Programming (DP)
* Value Function Approximation (Reinforcement Learning)
* XGBoost for value function training

The goal is to assign delivery jobs to couriers efficiently while minimizing:
* Travel time
* Delivery delays
* Unassigned jobs

### How to Run

    pip install -r requirements.txt
    python main.py

Then open notebooks to explore results.

### Dataset

The dataset contains:
* Pickup & delivery coordinates
* Ready time & due date
* Service duration

### Key Features

1. Simulation Engine
* Time-stepped simulation of delivery operations
* Streaming job arrivals
* Multi-courier simulation environment

2. Greedy Routing Policy
* Time-window aware insertion heuristic
* Evaluating all insertion positions
* Minimizing route duration

3. Dynamic Programming Scheduler
* Evaluating future impact of decisions using:

    cost + $\gamma$ * V(next state)

* Replacing greedy decision-making with learned policy

4. Value Function Approximation
* Training by XGBoost
* Predicting long-term cost of a system state
* Learning from simulation rollouts

5. Feature Engineering
State features include:
* Number of active jobs
* Number of couriers
* Average route length
* System load indicators 
