# PASE: Pro-Active Service Embedding in the Mobile Edge

Mobile edge computing offers ultra-low latency, high bandwidth, and high reliability. Thus, it can support a plethora of emerging services that can be placed in close proximity to the user. One of the fundamental problems in this context is maximizing the benefit from the placement of networked services, while meeting bandwidth and latency constraints.

In this study, we propose an adaptive and predictive resource allocation strategy for virtual-network function placement comprising services at the mobile edge. Our study focuses on maximizing the service provider's benefit under user mobility, i.e., uncertainty. This problem is NP-hard. Therefore, we propose a heuristic solution: we exploit local knowledge about the likely movements of users to speculatively allocate service functions. We allow the service functions to be allocated at different edge nodes, as long as latency and bandwidth constraints are met. We evaluate our proposal against a theoretically optimal algorithm as well as against recent previous work, using widely used simulation tools. Through an extensive simulation study, we demonstrate that under realistic scenarios, an adaptive and proactive strategy coupled with flexible placement can achieve close-to-optimal benefit.

## Project Usage and Information

### Project Structure
- PASE.jar: JAR version of the code (compiled with OpenJDK 22)
- src:
    - ExperimentLauncher.java – the main file for executing experiment sets
    - ConfigParser.java - parses config inputs
    - FileHandler.java - output file handler
    - MamiModel.java - baseline algorithm
    - Model.java - PASE model (requires CPLEX)
    - OptimizationLauncher.java - executed individual experiments
    - vnfPlace.java
- include:
    - probabilities.xlsx - the probabilities to transition between DCs in each time stamp in SUMO generated trace
    -  requests.xlsx - requests in each time stamp for the SUMO generated trace
    -  userProbsLW.csv - the probabilities to transition between DCs in each time stamp in Levy Walk trace
    -  requestsLW.xlsx - requests in each time stamp for the Lavy Walk trace
    - applications.csv
    - config.xml
    - cplex_config.prm
    - datacenters.csv
    - functions.csv
    - intervalsInNodes.csv
    - links.csv
    - inputs

### Execution
Execute ExperimentLauncher.java or JAR with the experiment ID. Three experiment sets are supported (as described in the paper): misprediction rate (ID 1), setup cost (ID 2), and utilization (ID 3).
Use 'values' field in config.xml to set the list of examined values in each experiment set.

You must have CPLEX installed to run the code.

Example for execution using JAR:

`java  -Djava.library.path=<PATH>/CPLEX_STUDIO2211/cplex/bin/x86-64_linux/ -jar 5GEdgeModel.jar 3`

## Citing PASE
To cite this repository:
```
@article{PASE,
  title={PASE: Pro-Active Service Embedding in the Mobile Edge},
  author={Kolosov, Oleg and Yadgar, Gala and Breitgand, David and Lorenz, Dean H},
  journal={Journal of Network and Systems Management},
  publisher={Springer}
}
```
