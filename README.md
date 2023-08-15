
<!-- <p align='center'>
    <img src='./.docs/cctv.png' width='20%' height='20%'>
</p> -->

<h1 align='center'>
    <strong> TBD </strong>
</h1>

<p align='center'>
    Designed to be the "big brother" of your running pipelines - allow the ghosts to get into your stack but make sure they don't get out without a fair punishment. 
</p>

<div align="center">

  <a href="code coverage">![coverage](https://img.shields.io/badge/coverage-98%25-brightgreen)</a>
  <a href="tests">![tests](https://img.shields.io/badge/tests-21%20passed%2C%200%20failed-brightgreen)</a>
  <a href="opentelemetry version">![scala_version](https://img.shields.io/badge/opentelemetry-1.19.1-blue)</a>
  <a href="python version">![sbt_version](https://img.shields.io/badge/python-3.10.9-blue?logo=python&logoColor=white)</a>

</div>

<div align="center">

  <a href="tests">![tests](https://img.shields.io/badge/azure-%20-brightgreen?logo=microsoft-azure)</a>
  <a href="tests">![tests](https://img.shields.io/badge/AWS-%20-red?logo=amazon&logoColor=white)</a>
  <a href="tests">![tests](https://img.shields.io/badge/GCP-%20-red?logo=google-cloud&logoColor=white)</a>

</div>

### **1. Introduction**

This solution works not only like a wrapper around the observability core functionalities of open telemetry but also works like a **_plug-and-play_ solution** where you spend less time worrying not only about the configuration of this tool but also about the connection with your platform.

### **2. Components Structure and Hierarchization**

<p align='center'>
    <img src='./.docs/components.png' width='55%'>
</p>

We have two hierarchical dimensions on this solution, both of which are part of the open telemetry:
1. `tracers` - create spans containing more information about what is happening for a given operation, these are created from Tracer Providers. Our specific solution is the personification of pipelines running on our platform like the one mentioned in the image above;
2. `spans` - represents a unit of work or operation. In this specific case, leverage the following naming convention:

````
{service_name}.{resource_type_monitored}.{OPTIONAL_var_name}
````

this point out the services where these spans are being created and the type of attributes they provide. Here are a few examples: 


````
databricks.df_attributes
databricks.SparkSession_specs
azure_function_app.api_extracted_data
````

### **3. Concept of Action**

This section answers the question: _"How does this all work together?"_ and the next image pictures the relationships that are established between the multiple components.

**[ADD_IMAGE_HERE]**

### **4. Project Organization**

### **5. Actions per hierarchy**
This framework leverages Manual Instrumentation completely.

#### **5.a. PipelineTracer**
This unit is responsible for the configuration of the tracer (actions like the configuration of the type processor and exporter). A baseline tracer is created and from that one, multiple tracers with different tracer_ids - defined by the user -, are created.

Here are the following actions supported:

- `get_tracer`: method used for the creation of new tracers;
- `_set_exporter_type`: at the moment only one exporter type is supported: `CONSOLE`;
- `_create_processor`: there's two kinds of processors that are supported: `BATCH` and `SIMPLE`.

#### **5.b. ServiceSpan**
The ServiceSpan is essentially a wrapper around two core actions on the span level: set_attribute and add_event.

The following actions can be found in this module:

- `set_attributes`: that adds a list of attributes to a given span;
- `add_events`: similarly to the previous one but this time for events.

#### **5.c. Attributes**
Depending on the type of variable that is being observed different kinds of attributes can be accessed. DFs (_Dataframes_), rdd, and SparkSessions are among the resources that have built-in observability metrics for Spark.  

More attributes will be added in the near future.
### **6. How to Use - _high level_**

As it can be seen in the [engine.py](./app/core/engine.py) script - that intends to be a high-level example that congregates all the elements of the solution -, this **TBD** can be used by taking into account the following code snippet:

````
Code to be added here
````

**_More documentation will be added in the near future on this._**

### **A1. Future Work**

There are still some points that we should tackle:

- test this solution in a 'production' scenario i.e. leverage it in resources like Azure Function App, Databricks notebooks, and/or scripts and see the behavior. As an example, we could clone the streaming pipeline that is now in production but make some adaptations so that this clone leverages the solution produced;

- gather with the Engineering team to define a set of attributes that would be nice to add to this solution (billing metrics and performance - which were already mentioned in some calls - should be held into account);

- analyze if Zipkin can be a good option to visualize this observability data.