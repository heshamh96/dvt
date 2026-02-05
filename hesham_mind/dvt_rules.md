

###  dvt compute rules
1) dvt uses push down first using adapters whenever possible using dbt adapters , only uses federation path when cross engines are used using spark jdbc connectors
1.1) dvt will always use filter pushdown methods when using the federation path for optimizing the data transfer between engines over the network
2) dvt will always use the default compute engine specified in the computes.yml
3) the compute engine specified in the model config should override the default compute engine specified in the computes.yml , this will give the user the ability to specify more performant compute engines for specific models when needed
3.1) if the compute engine specified in the model config is not available in the computes.yml it should be neglected as it is a user error
all compute engines specified in the computes.yml should be available in the dvt compute engine registry
4) if the compute is specified in the cli config it should override the compute engine specified in the model config and the default compute engine specified in the computes.yml as this compute will be used to only execute the models that requried compute only since dvt will always favor adapter pushdown over federation path

### dvt target rules

1)profiles.yml target is the default to materialize models to

2)the target model config should override the profiles.yml target

3)the target cli config overrides profiles.yml target and model target configs , meaning that it will force materializing models to the specified target
3.1) meaning all source() refrences will be  federated using the spark jdbc connectors and then all the downstream ref() models will be materialized using the adapter to again pushdown

#### dvt materialization rules

1)when a model has no specidied materialization type it should be materialized as the default materialization type specified in the dbt_project.yml

2)when a model has a specified materialization type it should be materialized as the specified materialization type

2.1) in case of user errors the materialization type should be neglected and default to a table
3) the table materialization type should be materialized as a table using the adapter to pushdown the model iff all the upstream models exist in the same target as the model
3) all incremental materialization types should be materialized as a incremental using the adapter to pushdown the model iff all the upstream models exist in the same target as the model
4) all ephemeral materialization types should be materialized as a ephemeral using the adapter to pushdown the model iff all the upstream models exist in the same target as the model
5) all view materialization types should be materialized as a view using the adapter to pushdown the model iff all the upstream models exist in the same target as the model
6) all materialization types should be materialized as table   using by federartion using spark jdbc connector to materialize the model iff at least one of the upstream models exist in a different target as the model
6.1) all incremental materialization types should be materialized as a incremental  by federartion using spark jdbc connector to materialize the model iff at least one of the upstream models exist in a different target as the model



### dvt dag rules for each model
0) dvt should resolve the target configs at first then the materialization configs in order to decide when to pushdown the model and when to federate it , and then associate all of that per each model in the dag , and it will always favor pushdown using adapter over federation path using the spark compute engine
general rules of thumb for each model to follow
1) when the upstream models of a model exist in the same target the model should be materialized using the adapter to pushdown the model
2) when at least 1 of the upstream models of a model exist in a different target the model should be materialized using federation path using spark jdbc connector to materialize the model
3) when choosing federation path , models should work with all syntax rules specified in the mdm.duckdb in order for the syntax to carry out the execution accross adapters perfectly with no issues


### dvt backward compitabilty with dbt

in case of using only 1 adapter it should be perfectly working as dbt , using the adapter for pushdown doing stuff to the underlying engine

==========


1. DVT Compute Rules

   * 1) Primary Directive (Pushdown Preference): DVT prioritizes Adapter Pushdown (SQL execution on the DB) whenever the model and all its inputs reside in the same Target.
       * 1.1 DVT only uses the Federation Path (Spark Compute) when a model requires inputs from a Target different than its own.
   * 2) Filter Optimization: When using the Federation Path, DVT enforces Predicate Pushdown (sending WHERE clauses to the source DB) to minimize network transfer.
   * 3) Compute Selection Hierarchy:
       * Level 1 (Lowest): Default compute engine in computes.yml.
       * Level 2: Model-specific compute config (overrides Level 1).
       * Level 3 (Highest): CLI --compute argument (overrides Level 2 & 1).
   * 4) Validity Check (Crucial Change):
       * If a specified compute engine (via Config or CLI) does not exist in the registry, DVT raises a compilation error and stops. It does not fallback to default.
   * 5) Scope of Compute: The selected Compute Engine is only utilized for models requiring the Federation Path (Rule 1.1). Models eligible for Adapter Pushdown (Rule 1) ignore the Compute Engine setting to
     ensure optimal performance.

  2. DVT Target Rules

   * 1) Target Hierarchy:
       * Level 1: profiles.yml default target.
       * Level 2: Model-specific target config.
       * Level 3: CLI --target argument (Forces Global Target Override).
   * 2) Global Target Implication: If the CLI --target is used:
       * All models are forced to materialize in this target.
       * Any model reading from a source not in this target triggers the Federation Path.

  3. DVT Materialization Rules

  A. Defaults & Validation
   * 1) If materialization is unspecified: Use project default (usually view).
   * 2) If materialization is invalid/unknown: Raise a compilation error.

  B. Same-Target Execution (Adapter Pushdown Path)
   * Condition: Model Target == All Upstream Targets.
   * 1) table → Materialized as Table via Adapter.
   * 2) incremental → Materialized as Incremental via Adapter.
   * 3) view → Materialized as View via Adapter.
   * 4) ephemeral → Compiled as a CTE (Common Table Expression) injected into downstream queries.

  C. Cross-Target Execution (Federation/Spark Path)
   * Condition: Model Target != At least one Upstream Target.
   * 1) table → Spark reads sources, joins/transforms, and writes Table to Target via JDBC.
   * 2) incremental → Spark reads sources, calculates delta, and merges/appends to Target Table via JDBC.
   * 3) view → COERCED TO TABLE. (Logic Change: You cannot create a cross-DB view). DVT will log a warning: "Model X is configured as view but requires federation. Materializing as Table."
   * 4) ephemeral → RESOLVED IN MEMORY. Spark computes the dataframe and passes it to the downstream step. It is not written to the DB.

  4. DVT DAG & Execution Strategy

   * Phase 1: Resolution
       1. Resolve Target for every node (Apply CLI overrides, then Configs, then Profile).
       2. Resolve Execution Path for every node:
           * If Node.Target == All_Upstream.Target → Pushdown.
           * Else → Federation.
   * Phase 2: Grouping
       * DVT should respect the dag execution sequence between models despite regardless of the type of compute.
   * Phase 3: Execution
       * Pushdown Nodes: Execute dvt run logic using the specific adapter.
       * Federation Nodes: Spin up Spark Session (using resolved Compute Engine).
           * Read Upstreams (using JDBC or native Spark connectors).
           * Apply Transforms (Spark SQL).
           * Write to Target (JDBC).






## the following rules are writing rules that are specific to the declarative nature of dvt which is similar to dbt 

### for consecative executions the writing rules should be as follows
1)  for models any execution should be like how dbt executes things so in "run" models that get recreated again are done via truncate and insert unless --full-refresh is specified then it should drop or in some cases when needed dop cascade and then 

2) for seeds it should also work like dbt , truncate and insert unless --full-refresh is specified so that the table gets dropped or drop cascade that it gets fully refreshed 

