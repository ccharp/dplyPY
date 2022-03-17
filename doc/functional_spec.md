## Functional Specification

### Background
The well-known R package, dplyr, offers convenient pipelining methods for data transformations. Data pipelines like this mesh well with the functional programming style R implements: the data transformation becomes a simple chain of functions. This makes compositions of transformations as simple as composing functions. Contrasted against Pandas' OOP-style interface, dplyr pipelines are much easier to parse, read, and ultimately, understand and implement. We bring this functional API for transformations to Python. 

### User Profile
The user is likely someone experienced with dplyr in R but with little experience using Python or simply prefers dplyr-style transformations over what Python currently offers. These people may be:
- Researchers
- Students
- Data engineers

### Example use cases
<ol>
<li>Data Cleaning and Exploratory Data Analysis
  <ol>
    <li>USER: Reads data from CSV into DplyPY dataframe</li>
    <li>USER: Using the dplyPY API and simple dataset e.g. world economic indicators, construct a pipeline that:
        <ol>
            <li>Drops all rows with null values</li>
            <li>Filters out rows with years before 1950</li>
            <li>Melts years into a single column</li>
            <li>Pivots categorical variables and associated values to their own columns</li>
            <li>Visualizes missing data after these operations</li>
            <li>Sorts the rows alphabetically by data</li>
            <li>Selects rows with GDP greater than X</li>
            <li>Writes transformed data to a CSV</li>
        </ol>
    <li>USER: use transformed data in an arbitrary way</li>
  </ol>
</li>
<li>Auditable data provenance: regulation demands breadcrumb trail of transformation in the previous example
  <ol>
    <li>USER: Implements function `log` that takes a dataframe and logs it (locally, Logstash, etc.)</li>
    <li>USER: intersperses `side_effect` method between all transformations in the previous pipeline, passing in `log.`</li>
  </ol>
</li>
</ol>
