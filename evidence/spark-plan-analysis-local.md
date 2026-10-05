# Spark physical-plan evidence

Generated: `2026-10-05T01:49:43Z`

This report proves plan selection and result equivalence on local synthetic data; it is not a throughput benchmark or production tuning result.

## Baseline Sort-Merge Join

Features: `{"AQEShuffleRead": 0, "AdaptiveSparkPlan": 0, "BroadcastExchange": 0, "BroadcastHashJoin": 0, "Exchange": 6, "SortMergeJoin": 2, "coalesced": 0}`

```text
== Physical Plan ==
* HashAggregate (13)
+- Exchange (12)
   +- * HashAggregate (11)
      +- * Project (10)
         +- * SortMergeJoin Inner (9)
            :- * Sort (4)
            :  +- Exchange (3)
            :     +- * Project (2)
            :        +- * Range (1)
            +- * Sort (8)
               +- Exchange (7)
                  +- * Filter (6)
                     +- * Scan ExistingRDD (5)


(1) Range [codegen id : 1]
Output [1]: [id#<id>]
Arguments: Range (0, 1000, step=1, splits=Some(2))

(2) Project [codegen id : 1]
Output [2]: [format_string(ST-%03d, ((id#<id> % 8) + 1)) AS station_id#<id>, ((id#<id> % 17) + 1) AS trip_minutes#<id>]
Input [1]: [id#<id>]

(3) Exchange
Input [2]: [station_id#<id>, trip_minutes#<id>]
Arguments: hashpartitioning(station_id#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(4) Sort [codegen id : 2]
Input [2]: [station_id#<id>, trip_minutes#<id>]
Arguments: [station_id#<id> ASC NULLS FIRST], false, 0

(5) Scan ExistingRDD [codegen id : 3]
Output [2]: [station_id#<id>, district#<id>]
Arguments: [station_id#<id>, district#<id>], MapPartitionsRDD[4] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:0, ExistingRDD, UnknownPartitioning(0)

(6) Filter [codegen id : 3]
Input [2]: [station_id#<id>, district#<id>]
Condition : isnotnull(station_id#<id>)

(7) Exchange
Input [2]: [station_id#<id>, district#<id>]
Arguments: hashpartitioning(station_id#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(8) Sort [codegen id : 4]
Input [2]: [station_id#<id>, district#<id>]
Arguments: [station_id#<id> ASC NULLS FIRST], false, 0

(9) SortMergeJoin [codegen id : 5]
Left keys [1]: [station_id#<id>]
Right keys [1]: [station_id#<id>]
Join type: Inner
Join condition: None

(10) Project [codegen id : 5]
Output [2]: [trip_minutes#<id>, district#<id>]
Input [4]: [station_id#<id>, trip_minutes#<id>, station_id#<id>, district#<id>]

(11) HashAggregate [codegen id : 5]
Input [2]: [trip_minutes#<id>, district#<id>]
Keys [1]: [district#<id>]
Functions [2]: [partial_count(1), partial_sum(trip_minutes#<id>)]
Aggregate Attributes [2]: [count#<id>, sum#<id>]
Results [3]: [district#<id>, count#<id>, sum#<id>]

(12) Exchange
Input [3]: [district#<id>, count#<id>, sum#<id>]
Arguments: hashpartitioning(district#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(13) HashAggregate [codegen id : 6]
Input [3]: [district#<id>, count#<id>, sum#<id>]
Keys [1]: [district#<id>]
Functions [2]: [count(1), sum(trip_minutes#<id>)]
Aggregate Attributes [2]: [count(1)#<id>, sum(trip_minutes#<id>)#<id>]
Results [3]: [district#<id>, count(1)#<id> AS trip_count#<id>, sum(trip_minutes#<id>)#<id> AS total_trip_minutes#<id>]
```

## Explicit Broadcast Hash Join

Features: `{"AQEShuffleRead": 0, "AdaptiveSparkPlan": 0, "BroadcastExchange": 2, "BroadcastHashJoin": 2, "Exchange": 4, "SortMergeJoin": 0, "coalesced": 0}`

```text
== Physical Plan ==
* HashAggregate (10)
+- Exchange (9)
   +- * HashAggregate (8)
      +- * Project (7)
         +- * BroadcastHashJoin Inner BuildRight (6)
            :- * Project (2)
            :  +- * Range (1)
            +- BroadcastExchange (5)
               +- * Filter (4)
                  +- * Scan ExistingRDD (3)


(1) Range [codegen id : 2]
Output [1]: [id#<id>]
Arguments: Range (0, 1000, step=1, splits=Some(2))

(2) Project [codegen id : 2]
Output [2]: [format_string(ST-%03d, ((id#<id> % 8) + 1)) AS station_id#<id>, ((id#<id> % 17) + 1) AS trip_minutes#<id>]
Input [1]: [id#<id>]

(3) Scan ExistingRDD [codegen id : 1]
Output [2]: [station_id#<id>, district#<id>]
Arguments: [station_id#<id>, district#<id>], MapPartitionsRDD[4] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:0, ExistingRDD, UnknownPartitioning(0)

(4) Filter [codegen id : 1]
Input [2]: [station_id#<id>, district#<id>]
Condition : isnotnull(station_id#<id>)

(5) BroadcastExchange
Input [2]: [station_id#<id>, district#<id>]
Arguments: HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=<id>]

(6) BroadcastHashJoin [codegen id : 2]
Left keys [1]: [station_id#<id>]
Right keys [1]: [station_id#<id>]
Join type: Inner
Join condition: None

(7) Project [codegen id : 2]
Output [2]: [trip_minutes#<id>, district#<id>]
Input [4]: [station_id#<id>, trip_minutes#<id>, station_id#<id>, district#<id>]

(8) HashAggregate [codegen id : 2]
Input [2]: [trip_minutes#<id>, district#<id>]
Keys [1]: [district#<id>]
Functions [2]: [partial_count(1), partial_sum(trip_minutes#<id>)]
Aggregate Attributes [2]: [count#<id>, sum#<id>]
Results [3]: [district#<id>, count#<id>, sum#<id>]

(9) Exchange
Input [3]: [district#<id>, count#<id>, sum#<id>]
Arguments: hashpartitioning(district#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(10) HashAggregate [codegen id : 3]
Input [3]: [district#<id>, count#<id>, sum#<id>]
Keys [1]: [district#<id>]
Functions [2]: [count(1), sum(trip_minutes#<id>)]
Aggregate Attributes [2]: [count(1)#<id>, sum(trip_minutes#<id>)#<id>]
Results [3]: [district#<id>, count(1)#<id> AS trip_count#<id>, sum(trip_minutes#<id>)#<id> AS total_trip_minutes#<id>]
```

## Adaptive aggregation

Features: `{"AQEShuffleRead": 0, "AdaptiveSparkPlan": 2, "BroadcastExchange": 0, "BroadcastHashJoin": 0, "Exchange": 6, "SortMergeJoin": 2, "coalesced": 0}`

```text
== Physical Plan ==
AdaptiveSparkPlan (14)
+- HashAggregate (13)
   +- Exchange (12)
      +- HashAggregate (11)
         +- Project (10)
            +- SortMergeJoin Inner (9)
               :- Sort (4)
               :  +- Exchange (3)
               :     +- Project (2)
               :        +- Range (1)
               +- Sort (8)
                  +- Exchange (7)
                     +- Filter (6)
                        +- Scan ExistingRDD (5)


(1) Range
Output [1]: [id#<id>]
Arguments: Range (0, 1000, step=1, splits=Some(2))

(2) Project
Output [1]: [format_string(ST-%03d, ((id#<id> % 8) + 1)) AS station_id#<id>]
Input [1]: [id#<id>]

(3) Exchange
Input [1]: [station_id#<id>]
Arguments: hashpartitioning(station_id#<id>, 16), REPARTITION_BY_NUM, [plan_id=<id>]

(4) Sort
Input [1]: [station_id#<id>]
Arguments: [station_id#<id> ASC NULLS FIRST], false, 0

(5) Scan ExistingRDD
Output [2]: [station_id#<id>, district#<id>]
Arguments: [station_id#<id>, district#<id>], MapPartitionsRDD[4] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:0, ExistingRDD, UnknownPartitioning(0)

(6) Filter
Input [2]: [station_id#<id>, district#<id>]
Condition : isnotnull(station_id#<id>)

(7) Exchange
Input [2]: [station_id#<id>, district#<id>]
Arguments: hashpartitioning(station_id#<id>, 16), ENSURE_REQUIREMENTS, [plan_id=<id>]

(8) Sort
Input [2]: [station_id#<id>, district#<id>]
Arguments: [station_id#<id> ASC NULLS FIRST], false, 0

(9) SortMergeJoin
Left keys [1]: [station_id#<id>]
Right keys [1]: [station_id#<id>]
Join type: Inner
Join condition: None

(10) Project
Output [1]: [district#<id>]
Input [3]: [station_id#<id>, station_id#<id>, district#<id>]

(11) HashAggregate
Input [1]: [district#<id>]
Keys [1]: [district#<id>]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#<id>]
Results [2]: [district#<id>, count#<id>]

(12) Exchange
Input [2]: [district#<id>, count#<id>]
Arguments: hashpartitioning(district#<id>, 16), ENSURE_REQUIREMENTS, [plan_id=<id>]

(13) HashAggregate
Input [2]: [district#<id>, count#<id>]
Keys [1]: [district#<id>]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#<id>]
Results [2]: [district#<id>, count(1)#<id> AS trip_count#<id>]

(14) AdaptiveSparkPlan
Output [2]: [district#<id>, trip_count#<id>]
Arguments: isFinalPlan=false
```
