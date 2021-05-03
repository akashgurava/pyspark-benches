# Bench PySpark

A collection of simple utility programs to benchmark different PySpark scenarios

- [Bench PySpark](#bench-pyspark)
  - [getOrCreate](#getorcreate)
    - [Inferences](#inferences)
    - [TODO](#todo)

## getOrCreate

Module to benchmark different scenarios related to get or create PySpark session

### Inferences

1. `spark-submit` adds tiny 2s overhead
2. `spark-submit` will not create session in the background. It waits till first
   `getOrCreate` call is made
3. Even if ran with `python get_or_create.py` total time equals `spark-submit` + first
    `getOrCreate` call
4. First call for `getOrCreate` in a `spark-submit` takes time, next calls are instant
5. Stopping session and creating new using `stop` then `getOrCreate` takes approximately
   80% of time to the initial `getOrCreate`

### TODO

- [ ] Test in cluster mode
- [ ] Test start in client mode stop and start in cluster mode. vice versa
- [ ] Test impact of allocating more resources(Memory and # of executors)
