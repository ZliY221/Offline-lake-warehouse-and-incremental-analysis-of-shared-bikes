# Spark physical-plan evidence

Generated: `2026-10-05T02:15:59Z`

This report proves plan selection and result equivalence on local synthetic data; it is not a throughput benchmark or production tuning result.

## Baseline Sort-Merge Join

Features: `{"AQEShuffleRead": 0, "AdaptiveSparkPlan": 0, "BroadcastExchange": 0, "BroadcastHashJoin": 0, "Exchange": 6, "SortMergeJoin": 2, "coalesced": 0, "isFinalPlan=true": 0, "skew=true": 0, "skewed": 0}`

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

Features: `{"AQEShuffleRead": 0, "AdaptiveSparkPlan": 0, "BroadcastExchange": 2, "BroadcastHashJoin": 2, "Exchange": 4, "SortMergeJoin": 0, "coalesced": 0, "isFinalPlan=true": 0, "skew=true": 0, "skewed": 0}`

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

Features: `{"AQEShuffleRead": 2, "AdaptiveSparkPlan": 2, "BroadcastExchange": 0, "BroadcastHashJoin": 0, "Exchange": 12, "SortMergeJoin": 4, "coalesced": 2, "isFinalPlan=true": 1, "skew=true": 0, "skewed": 0}`

```text
== Physical Plan ==
AdaptiveSparkPlan (28)
+- == Final Plan ==
   ResultQueryStage (18)
   +- * HashAggregate (17)
      +- AQEShuffleRead (16), coalesced
         +- ShuffleQueryStage (15), Statistics(sizeInBytes=320.0 B, rowCount=8)
            +- Exchange (14)
               +- * HashAggregate (13)
                  +- * Project (12)
                     +- * SortMergeJoin Inner (11)
                        :- * Sort (5)
                        :  +- ShuffleQueryStage (4), Statistics(sizeInBytes=23.4 KiB, rowCount=1.00E+3)
                        :     +- Exchange (3)
                        :        +- * Project (2)
                        :           +- * Range (1)
                        +- * Sort (10)
                           +- ShuffleQueryStage (9), Statistics(sizeInBytes=384.0 B, rowCount=8)
                              +- Exchange (8)
                                 +- * Filter (7)
                                    +- * Scan ExistingRDD (6)
+- == Initial Plan ==
   HashAggregate (27)
   +- Exchange (26)
      +- HashAggregate (25)
         +- Project (24)
            +- SortMergeJoin Inner (23)
               :- Sort (20)
               :  +- Exchange (19)
               :     +- Project (2)
               :        +- Range (1)
               +- Sort (22)
                  +- Exchange (21)
                     +- Filter (7)
                        +- Scan ExistingRDD (6)


(1) Range [codegen id : 1]
Output [1]: [id#<id>]
Arguments: Range (0, 1000, step=1, splits=Some(2))

(2) Project [codegen id : 1]
Output [1]: [format_string(ST-%03d, ((id#<id> % 8) + 1)) AS station_id#<id>]
Input [1]: [id#<id>]

(3) Exchange
Input [1]: [station_id#<id>]
Arguments: hashpartitioning(station_id#<id>, 16), REPARTITION_BY_NUM, [plan_id=<id>]

(4) ShuffleQueryStage
Output [1]: [station_id#<id>]
Arguments: 0

(5) Sort [codegen id : 3]
Input [1]: [station_id#<id>]
Arguments: [station_id#<id> ASC NULLS FIRST], false, 0

(6) Scan ExistingRDD [codegen id : 2]
Output [2]: [station_id#<id>, district#<id>]
Arguments: [station_id#<id>, district#<id>], MapPartitionsRDD[4] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:0, ExistingRDD, UnknownPartitioning(0)

(7) Filter [codegen id : 2]
Input [2]: [station_id#<id>, district#<id>]
Condition : isnotnull(station_id#<id>)

(8) Exchange
Input [2]: [station_id#<id>, district#<id>]
Arguments: hashpartitioning(station_id#<id>, 16), ENSURE_REQUIREMENTS, [plan_id=<id>]

(9) ShuffleQueryStage
Output [2]: [station_id#<id>, district#<id>]
Arguments: 1

(10) Sort [codegen id : 4]
Input [2]: [station_id#<id>, district#<id>]
Arguments: [station_id#<id> ASC NULLS FIRST], false, 0

(11) SortMergeJoin [codegen id : 5]
Left keys [1]: [station_id#<id>]
Right keys [1]: [station_id#<id>]
Join type: Inner
Join condition: None

(12) Project [codegen id : 5]
Output [1]: [district#<id>]
Input [3]: [station_id#<id>, station_id#<id>, district#<id>]

(13) HashAggregate [codegen id : 5]
Input [1]: [district#<id>]
Keys [1]: [district#<id>]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#<id>]
Results [2]: [district#<id>, count#<id>]

(14) Exchange
Input [2]: [district#<id>, count#<id>]
Arguments: hashpartitioning(district#<id>, 16), ENSURE_REQUIREMENTS, [plan_id=<id>]

(15) ShuffleQueryStage
Output [2]: [district#<id>, count#<id>]
Arguments: 2

(16) AQEShuffleRead
Input [2]: [district#<id>, count#<id>]
Arguments: coalesced

(17) HashAggregate [codegen id : 6]
Input [2]: [district#<id>, count#<id>]
Keys [1]: [district#<id>]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#<id>]
Results [2]: [district#<id>, count(1)#<id> AS trip_count#<id>]

(18) ResultQueryStage
Output [2]: [district#<id>, trip_count#<id>]
Arguments: 3

(19) Exchange
Input [1]: [station_id#<id>]
Arguments: hashpartitioning(station_id#<id>, 16), REPARTITION_BY_NUM, [plan_id=<id>]

(20) Sort
Input [1]: [station_id#<id>]
Arguments: [station_id#<id> ASC NULLS FIRST], false, 0

(21) Exchange
Input [2]: [station_id#<id>, district#<id>]
Arguments: hashpartitioning(station_id#<id>, 16), ENSURE_REQUIREMENTS, [plan_id=<id>]

(22) Sort
Input [2]: [station_id#<id>, district#<id>]
Arguments: [station_id#<id> ASC NULLS FIRST], false, 0

(23) SortMergeJoin
Left keys [1]: [station_id#<id>]
Right keys [1]: [station_id#<id>]
Join type: Inner
Join condition: None

(24) Project
Output [1]: [district#<id>]
Input [3]: [station_id#<id>, station_id#<id>, district#<id>]

(25) HashAggregate
Input [1]: [district#<id>]
Keys [1]: [district#<id>]
Functions [1]: [partial_count(1)]
Aggregate Attributes [1]: [count#<id>]
Results [2]: [district#<id>, count#<id>]

(26) Exchange
Input [2]: [district#<id>, count#<id>]
Arguments: hashpartitioning(district#<id>, 16), ENSURE_REQUIREMENTS, [plan_id=<id>]

(27) HashAggregate
Input [2]: [district#<id>, count#<id>]
Keys [1]: [district#<id>]
Functions [1]: [count(1)]
Aggregate Attributes [1]: [count(1)#<id>]
Results [2]: [district#<id>, count(1)#<id> AS trip_count#<id>]

(28) AdaptiveSparkPlan
Output [2]: [district#<id>, trip_count#<id>]
Arguments: isFinalPlan=true
```

## Skewed Sort-Merge baseline

Features: `{"AQEShuffleRead": 0, "AdaptiveSparkPlan": 0, "BroadcastExchange": 0, "BroadcastHashJoin": 0, "Exchange": 6, "SortMergeJoin": 2, "coalesced": 0, "isFinalPlan=true": 0, "skew=true": 0, "skewed": 0}`

```text
== Physical Plan ==
* HashAggregate (14)
+- Exchange (13)
   +- * HashAggregate (12)
      +- * Project (11)
         +- * SortMergeJoin Inner (10)
            :- * Sort (5)
            :  +- Exchange (4)
            :     +- * Project (3)
            :        +- * Filter (2)
            :           +- * Range (1)
            +- * Sort (9)
               +- Exchange (8)
                  +- * Filter (7)
                     +- * Scan ExistingRDD (6)


(1) Range [codegen id : 1]
Output [1]: [id#<id>]
Arguments: Range (0, 20000, step=1, splits=Some(2))

(2) Filter [codegen id : 1]
Input [1]: [id#<id>]
Condition : CASE WHEN (id#<id> < 18000) THEN true ELSE isnotnull((((id#<id> - 18000) % 7) + 1)) END

(3) Project [codegen id : 1]
Output [2]: [CASE WHEN (id#<id> < 18000) THEN 0 ELSE (((id#<id> - 18000) % 7) + 1) END AS station_key#<id>, sha2(cast(concat_ws(-, cast(id#<id> as string), skew-evidence) as binary), 256) AS payload#<id>]
Input [1]: [id#<id>]

(4) Exchange
Input [2]: [station_key#<id>, payload#<id>]
Arguments: hashpartitioning(station_key#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(5) Sort [codegen id : 2]
Input [2]: [station_key#<id>, payload#<id>]
Arguments: [station_key#<id> ASC NULLS FIRST], false, 0

(6) Scan ExistingRDD [codegen id : 3]
Output [2]: [station_key#<id>, district#<id>]
Arguments: [station_key#<id>, district#<id>], MapPartitionsRDD[50] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:0, ExistingRDD, UnknownPartitioning(0)

(7) Filter [codegen id : 3]
Input [2]: [station_key#<id>, district#<id>]
Condition : isnotnull(station_key#<id>)

(8) Exchange
Input [2]: [station_key#<id>, district#<id>]
Arguments: hashpartitioning(station_key#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(9) Sort [codegen id : 4]
Input [2]: [station_key#<id>, district#<id>]
Arguments: [station_key#<id> ASC NULLS FIRST], false, 0

(10) SortMergeJoin [codegen id : 5]
Left keys [1]: [station_key#<id>]
Right keys [1]: [station_key#<id>]
Join type: Inner
Join condition: None

(11) Project [codegen id : 5]
Output [2]: [payload#<id>, district#<id>]
Input [4]: [station_key#<id>, payload#<id>, station_key#<id>, district#<id>]

(12) HashAggregate [codegen id : 5]
Input [2]: [payload#<id>, district#<id>]
Keys [1]: [district#<id>]
Functions [2]: [partial_count(1), partial_sum(length(payload#<id>))]
Aggregate Attributes [2]: [count#<id>, sum#<id>]
Results [3]: [district#<id>, count#<id>, sum#<id>]

(13) Exchange
Input [3]: [district#<id>, count#<id>, sum#<id>]
Arguments: hashpartitioning(district#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(14) HashAggregate [codegen id : 6]
Input [3]: [district#<id>, count#<id>, sum#<id>]
Keys [1]: [district#<id>]
Functions [2]: [count(1), sum(length(payload#<id>))]
Aggregate Attributes [2]: [count(1)#<id>, sum(length(payload#<id>))#<id>]
Results [3]: [district#<id>, count(1)#<id> AS trip_count#<id>, sum(length(payload#<id>))#<id> AS payload_characters#<id>]
```

## Adaptive skew join

Features: `{"AQEShuffleRead": 4, "AdaptiveSparkPlan": 2, "BroadcastExchange": 0, "BroadcastHashJoin": 0, "Exchange": 12, "SortMergeJoin": 4, "coalesced": 0, "isFinalPlan=true": 1, "skew=true": 2, "skewed": 2}`

```text
== Physical Plan ==
AdaptiveSparkPlan (30)
+- == Final Plan ==
   ResultQueryStage (20)
   +- * HashAggregate (19)
      +- ShuffleQueryStage (18), Statistics(sizeInBytes=432.0 B, rowCount=9)
         +- Exchange (17)
            +- * HashAggregate (16)
               +- * Project (15)
                  +- * SortMergeJoin(skew=true) Inner (14)
                     :- * Sort (7)
                     :  +- AQEShuffleRead (6), skewed
                     :     +- ShuffleQueryStage (5), Statistics(sizeInBytes=1718.8 KiB, rowCount=2.00E+4)
                     :        +- Exchange (4)
                     :           +- * Project (3)
                     :              +- * Filter (2)
                     :                 +- * Range (1)
                     +- * Sort (13)
                        +- AQEShuffleRead (12)
                           +- ShuffleQueryStage (11), Statistics(sizeInBytes=320.0 B, rowCount=8)
                              +- Exchange (10)
                                 +- * Filter (9)
                                    +- * Scan ExistingRDD (8)
+- == Initial Plan ==
   HashAggregate (29)
   +- Exchange (28)
      +- HashAggregate (27)
         +- Project (26)
            +- SortMergeJoin Inner (25)
               :- Sort (22)
               :  +- Exchange (21)
               :     +- Project (3)
               :        +- Filter (2)
               :           +- Range (1)
               +- Sort (24)
                  +- Exchange (23)
                     +- Filter (9)
                        +- Scan ExistingRDD (8)


(1) Range [codegen id : 1]
Output [1]: [id#<id>]
Arguments: Range (0, 20000, step=1, splits=Some(2))

(2) Filter [codegen id : 1]
Input [1]: [id#<id>]
Condition : CASE WHEN (id#<id> < 18000) THEN true ELSE isnotnull((((id#<id> - 18000) % 7) + 1)) END

(3) Project [codegen id : 1]
Output [2]: [CASE WHEN (id#<id> < 18000) THEN 0 ELSE (((id#<id> - 18000) % 7) + 1) END AS station_key#<id>, sha2(cast(concat_ws(-, cast(id#<id> as string), skew-evidence) as binary), 256) AS payload#<id>]
Input [1]: [id#<id>]

(4) Exchange
Input [2]: [station_key#<id>, payload#<id>]
Arguments: hashpartitioning(station_key#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(5) ShuffleQueryStage
Output [2]: [station_key#<id>, payload#<id>]
Arguments: 0

(6) AQEShuffleRead
Input [2]: [station_key#<id>, payload#<id>]
Arguments: skewed

(7) Sort [codegen id : 3]
Input [2]: [station_key#<id>, payload#<id>]
Arguments: [station_key#<id> ASC NULLS FIRST], false, 0

(8) Scan ExistingRDD [codegen id : 2]
Output [2]: [station_key#<id>, district#<id>]
Arguments: [station_key#<id>, district#<id>], MapPartitionsRDD[50] at applySchemaToPythonRDD at NativeMethodAccessorImpl.java:0, ExistingRDD, UnknownPartitioning(0)

(9) Filter [codegen id : 2]
Input [2]: [station_key#<id>, district#<id>]
Condition : isnotnull(station_key#<id>)

(10) Exchange
Input [2]: [station_key#<id>, district#<id>]
Arguments: hashpartitioning(station_key#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(11) ShuffleQueryStage
Output [2]: [station_key#<id>, district#<id>]
Arguments: 1

(12) AQEShuffleRead
Input [2]: [station_key#<id>, district#<id>]

(13) Sort [codegen id : 4]
Input [2]: [station_key#<id>, district#<id>]
Arguments: [station_key#<id> ASC NULLS FIRST], false, 0

(14) SortMergeJoin(skew=true) [codegen id : 5]
Left keys [1]: [station_key#<id>]
Right keys [1]: [station_key#<id>]
Join type: Inner
Join condition: None

(15) Project [codegen id : 5]
Output [2]: [payload#<id>, district#<id>]
Input [4]: [station_key#<id>, payload#<id>, station_key#<id>, district#<id>]

(16) HashAggregate [codegen id : 5]
Input [2]: [payload#<id>, district#<id>]
Keys [1]: [district#<id>]
Functions [2]: [partial_count(1), partial_sum(length(payload#<id>))]
Aggregate Attributes [2]: [count#<id>, sum#<id>]
Results [3]: [district#<id>, count#<id>, sum#<id>]

(17) Exchange
Input [3]: [district#<id>, count#<id>, sum#<id>]
Arguments: hashpartitioning(district#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(18) ShuffleQueryStage
Output [3]: [district#<id>, count#<id>, sum#<id>]
Arguments: 2

(19) HashAggregate [codegen id : 6]
Input [3]: [district#<id>, count#<id>, sum#<id>]
Keys [1]: [district#<id>]
Functions [2]: [count(1), sum(length(payload#<id>))]
Aggregate Attributes [2]: [count(1)#<id>, sum(length(payload#<id>))#<id>]
Results [3]: [district#<id>, count(1)#<id> AS trip_count#<id>, sum(length(payload#<id>))#<id> AS payload_characters#<id>]

(20) ResultQueryStage
Output [3]: [district#<id>, trip_count#<id>, payload_characters#<id>]
Arguments: 3

(21) Exchange
Input [2]: [station_key#<id>, payload#<id>]
Arguments: hashpartitioning(station_key#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(22) Sort
Input [2]: [station_key#<id>, payload#<id>]
Arguments: [station_key#<id> ASC NULLS FIRST], false, 0

(23) Exchange
Input [2]: [station_key#<id>, district#<id>]
Arguments: hashpartitioning(station_key#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(24) Sort
Input [2]: [station_key#<id>, district#<id>]
Arguments: [station_key#<id> ASC NULLS FIRST], false, 0

(25) SortMergeJoin
Left keys [1]: [station_key#<id>]
Right keys [1]: [station_key#<id>]
Join type: Inner
Join condition: None

(26) Project
Output [2]: [payload#<id>, district#<id>]
Input [4]: [station_key#<id>, payload#<id>, station_key#<id>, district#<id>]

(27) HashAggregate
Input [2]: [payload#<id>, district#<id>]
Keys [1]: [district#<id>]
Functions [2]: [partial_count(1), partial_sum(length(payload#<id>))]
Aggregate Attributes [2]: [count#<id>, sum#<id>]
Results [3]: [district#<id>, count#<id>, sum#<id>]

(28) Exchange
Input [3]: [district#<id>, count#<id>, sum#<id>]
Arguments: hashpartitioning(district#<id>, 8), ENSURE_REQUIREMENTS, [plan_id=<id>]

(29) HashAggregate
Input [3]: [district#<id>, count#<id>, sum#<id>]
Keys [1]: [district#<id>]
Functions [2]: [count(1), sum(length(payload#<id>))]
Aggregate Attributes [2]: [count(1)#<id>, sum(length(payload#<id>))#<id>]
Results [3]: [district#<id>, count(1)#<id> AS trip_count#<id>, sum(length(payload#<id>))#<id> AS payload_characters#<id>]

(30) AdaptiveSparkPlan
Output [3]: [district#<id>, trip_count#<id>, payload_characters#<id>]
Arguments: isFinalPlan=true
```
