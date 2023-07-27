# PySpark Common Data Model

A Python library for working with the Common Data Model (CDM) using PySpark.

## Usage

### Manifest
When using a manifest based CDM export you can use the following code to load the data:

```python
from pyspark_cdm import Manifest
from cdm.objectmodel import CdmCorpusDefinition
from cdm.storage import LocalAdapter

# Create a corpus definition.
corpus = CdmCorpusDefinition()

# Point to the root of your cdm export.
corpus.storage.mount("local", LocalAdapter(root="/dbfs/mnt/cdm/"))

# Create the manifest, and point to your entry manifest.
manifest = Manifest(corpus=corpus, path="local:/Tables/Tables.manifest.cdm.json")

# You can now inspect all the entities.
print([entity.name for entity in manifest.entities])
```

### Model
The package also works with the older model cdm export:

```python
from pyspark_cdm import Manifest
from cdm.objectmodel import CdmCorpusDefinition
from cdm.storage import LocalAdapter
from cdm.storage.adls import ADLSAdapter

# Create a corpus definition.
corpus = CdmCorpusDefinition()

# Point to the root of your cdm export.
corpus.storage.mount('local', LocalAdapter(root="/dbfs/mnt/cdm/"))

# If your export contains adls paths, you need to define an adls adapter.
corpus.storage.mount('adls', ADLSAdapter(
    root='/<container_name>',
    hostname='<endpoint>.dfs.core.windows.net',
    tenant='72f988bf-86f1-41af-91ab-2d7cd011db47',
    resource='https://storage.azure.com',
))

# Create the manifest, and point to your model json file.
manifest = Manifest(corpus=corpus, path="local:/model.json")

# You can now inspect all the entities.
print([entity.name for entity in manifest.entities])
```

### Loading data
You can load data from the entities themselves.
```python
# Load the dataframe.
df = entity.get_dataframe(spark)

# Print the number of rows.
print(df.count())
```

## Setup

### MacOs

1. Install [Homebrew](https://brew.sh/)

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

2. Install Java

```bash
brew install adoptopenjdk
```
