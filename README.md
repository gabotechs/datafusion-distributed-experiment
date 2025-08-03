# datafusion-distributed-experiment

This is an experiment for adding distributed execution capabilities to DataFusion. It introduces 
several core components that allow query plans to be executed across multiple physical machines. Rather than
providing an out-of-the-box solution, it aims to be the foundational building blocks over which
distributed engines can be built using DataFusion.


## Motivation

The goal of this project is to provide a framework of reusable components for building a distributed
execution engine on top of DataFusion. Instead of providing an executable binary or an opinionated framework, 
it provides a library as close as possible to vanilla DataFusion that allows developers to build their
own distributed engine based on their own rules. The core tenets are:

- Be as close to vanilla DataFusion as possible.
- Be unopinionated about the networking setup.
- Be unopinionated about what makes sense to distribute and what not.
- Be performant, adding as little overhead as possible to the equivalent non-distributed query.

## Architecture

There are two main core components that allow a plan to be distributed:
- `ArrowFlightReadExec`: an `ExecutionPlan` implementation that, instead of directly executing its children
  as any other `ExecutionPlan`, it serializes them and sends them over the wire to an Arrow Flight endpoint
  for the rest of the plan to be executed there.
- `ArrowFlightEndpoint`: a `FlightService` implementation based on https://github.com/hyperium/tonic that
  listens for serialized DataFusion `ExecutionPlan`s, executes them in a DataFusion runtime, and sends the
  resulting Arrow `RecordBatch` stream to the caller.

There's a many-to-many relationship between these two core components:
- A single `ArrowFlightReadExec` can gather data from multiple `ArrowFlightEndpoint`
- A single `ArrowFlightEndpoint` can stream data back to multiple `ArrowFlightReadExec`

## Example

Imagine we have a plan that looks like this:
```
   ┌──────────────────────┐
   │    ProjectionExec    │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    DataSourceExec    │
   └──────────────────────┘  
```

And we want to distribute the aggregation, to something like this:
```
                              ┌──────────────────────┐
                              │    ProjectionExec    │
                              └──────────────────────┘  
                                         ▲
                              ┌──────────┴───────────┐
                              │    AggregateExec     │
                              │       (final)        │
                              └──────────────────────┘  
                                       ▲ ▲ ▲
              ┌────────────────────────┘ │ └─────────────────────────┐
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │    AggregateExec     │   │    AggregateExec     │   │    AggregateExec     │
   │      (partial)       │   │      (partial)       │   │      (partial)       │
   └──────────────────────┘   └──────────────────────┘   └──────────────────────┘
              ▲                          ▲                           ▲
              └────────────────────────┐ │ ┌─────────────────────────┘
                              ┌────────┴─┴─┴─────────┐
                              │    DataSourceExec    │
                              └──────────────────────┘
```

Where each partial `AggregateExec` executes on a physical machine.

This can be done be injecting the appropriate `ArrowFlightReadExec` nodes in the appropriate places:
```
                               ┌──────────────────────┐
                               │    ProjectionExec    │
                               └──────────────────────┘  
                                          ▲
                               ┌──────────┴───────────┐
                               │    AggregateExec     │
                               │       (final)        │
                               └──────────────────────┘  
                                          ▲
                               ┌──────────┴───────────┐
                               │  ArrowFlightReadExec │
                               └──────────────────────┘  
                                        ▲ ▲ ▲                                          
               ┌────────────────────────┘ │ └─────────────────────────┐                   
               │                          │                           │               ┐
   ┌─── task 0 (stage 1) ───┐ ┌── task 1 (stage 1) ────┐ ┌── task 2 (stage 1) ────┐   │
   │┌─ ─ ─ ─ ─ ┴ ─ ─ ─ ─ ─ ┐│ │┌─ ─ ─ ─ ─ ┴ ─ ─ ─ ─ ─ ┐│ │┌ ─ ─ ─ ─ ─ ┴ ─ ─ ─ ─ ─┐│   │
   ││ ArrowFlightEndpoint  ││ ││ ArrowFlightEndpoint  ││ ││ ArrowFlightEndpoint  ││   │
   │└─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ┘│ │└─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ┘│ │└ ─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─┘│   │
   │┌──────────┴───────────┐│ │┌──────────┴───────────┐│ │┌───────────┴──────────┐│   │
   ││    AggregateExec     ││ ││    AggregateExec     ││ ││    AggregateExec     ││   │
   ││      (partial)       ││ ││      (partial)       ││ ││      (partial)       ││   │ stage 1
   │└──────────┬───────────┘│ │└──────────┬───────────┘│ │└───────────┬──────────┘│   │
   │┌──────────┴───────────┐│ │┌──────────┴───────────┐│ │┌───────────┴──────────┐│   │
   ││ ArrowFlightReadExec  ││ ││ ArrowFlightReadExec  ││ ││ ArrowFlightReadExec  ││   │
   │└──────────────────────┘│ │└──────────────────────┘│ │└──────────────────────┘│   │
   └───────────▲────────────┘ └───────────▲────────────┘ └────────────▲───────────┘   │
               │                          │                           │               ┘
               └────────────────────────┐ │ ┌─────────────────────────┘               
                              ┌─── task 1 (stage 0) ───┐          
                              │┌─ ─ ─ ─ ┴ ┴ ┴ ─ ─ ─ ─ ┐│                              ┐
                              ││  ArrowFlightEndpoint ││                              │
                              │└─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ┘│                              │ stage 0
                              │┌──────────┴───────────┐│                              │
                              ││    DataSourceExec    ││                              │ 
                              │└──────────────────────┘│                              ┘
                              └────────────────────────┘
```

Note that because of us placing an `ArrowFlightReadExec`, a new `ArrowFlightEndpoint` was also introduced.
In practice, this is not represented as a physical execution node in the plan; instead, it's just an Arrow Flight
service that listens to gRPC comms and executes the provided plan.

Now, how does the top `ArrowFlightReadExec` know which are the URLs for the three target `ArrowFlightEndpoint`s?
Initially, once the appropriate `ArrowFlightReadExec` nodes have been placed in the plan, they contain some empty
slots, which an extra planning step is supposed to fill.

In order to understand that, let's visualize again the original plan:

```
   ┌──────────────────────┐
   │    ProjectionExec    │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    DataSourceExec    │
   └──────────────────────┘  
```

We want to distribute this, so we tweak the plan ourselves to inject the `ArrowFlightReadExec` where it makes sense,
and divide the aggregation in two: one partial and one final aggregation:


```
   ┌──────────────────────┐
   │    ProjectionExec    │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   │       (final)        │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐ input_tasks=3
   │ ArrowFlightReadExec  │
   └──────────────────────┘
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   │      (partial)       │
   └──────────────────────┘
              ▲
   ┌──────────┴───────────┐ input_tasks=1 shuffle_hash=[grouping_col]
   │ ArrowFlightReadExec  │
   └──────────────────────┘
              ▲
   ┌──────────┴───────────┐
   │    DataSourceExec    │
   └──────────────────────┘  
```

This is all the information the user needed to provide while injecting the `ArrowFlightReadExec` nodes:
- The number of input tasks from which each `ArrowFlightReadExec`
- An optional expression telling the target stage how data should be shuffled (or round-robin by default)

The extra planning step will assign all the missing information to each `ArrowFlightReadExec`:
- A unique ID identifying the stage in which the `ArrowFlightReadExec` lives
- A unique ID identifying the input stage from which the `ArrowFlightReadExec` needs to read
- The total number of tasks involved in the current stage
- The target URLs serving data from the input stage to which the `ArrowFlightReadExec` is supposed to go


```
  ┌             ┌──────────────────────┐
  │             │    ProjectionExec    │
  │             └──────────────────────┘  
  │                        ▲
  │             ┌──────────┴───────────┐   * Note that this is the head of the plan, and this will run normally
  │             │    AggregateExec     │     as if it was single node, so the stage identifier here is irrelevant
  │ stage 2     │       (final)        │
  │             └──────────────────────┘  
  │                        ▲
  │             ┌──────────┴───────────┐ input_tasks=3 shuffle_hash=None
  │             │ ArrowFlightReadExec  │ stage_id=2 n_tasks=1
  └             └──────────────────────┘ input_stage_id=1 input_urls=[10.0.0.25, 10.0.0.26, 10.0.0.27]
                           ▲
  ┌             ┌──────────┴───────────┐
  │             │    AggregateExec     │
  │             │      (partial)       │
  │             └──────────────────────┘
  │ stage 1                ▲
  │             ┌──────────┴───────────┐ input_tasks=1 shuffle_hash=[grouping_col]
  │             │ ArrowFlightReadExec  │ stage_id=1 n_tasks=3
  └             └──────────────────────┘ input_stage_id=0 input_urls=[10.0.0.28]
                           ▲
  ┌             ┌──────────┴───────────┐
  │ stage 0     │    DataSourceExec    │
  └             └──────────────────────┘  
```

# Example

There's an example about what this looks like in [tests/distributed_aggregation.rs](./tests/distributed_aggregation.rs)