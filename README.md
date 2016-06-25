# Overview

Sample Spark code to parse NYSE trade and quote data.

# Build

```
sbt clean package
```

# Run

```
./run <input_file> <output_dir>
```

`<output_dir>` must not exist.

You will get some parquet files in `<output_dir>`.
