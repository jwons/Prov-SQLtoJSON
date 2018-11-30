# SQLite to Language Level Prov-JSON Converter

This tool takes provenance created from [noWorkflow](https://github.com/gems-uff/noworkflow) and converts it to prov-JSON compliant language level provenance (LL-Prov). This exact format is described [here](https://github.com/End-to-end-provenance/ExtendedProvJson)

This repository builds off the work started [here](https://github.com/End-to-end-provenance/python_tools/tree/master/noWorkflow) and is currently a work in progress. 

## Why do this?
There are benefits to having multiple languages that can produce langugage level provenance that follows the same format. For example, [provDebugR](https://github.com/End-to-end-provenance/provdebugr) has shown the benefits LL-Prov by providing a debugger that can trace lineage of variables, type check, and allow for reverse-execution debugging. A language agnostic version of provDebugR is being produced that will use the provenance generated from this converter. 

## Usage
```{python}
from sql_to_json import SqlToJson

inputDB = "prov.sqlite"
outputJSON = "prov.json"
trialNumList = [1]
converter = SqlToJson()
converter.convert(trialNumList, inputDB, outputJSON)

```

## Updates made (so far)

- Expanded environment node
- Added agent node
- Added prefix node
- Begin refactoring to a more OOP format
- Data nodes now save variable names
- Assignment of literals to variables now shows in procedure nodes
- Environment node moved from activity to entity
- Edges renamed from 'e' to either "pp", "pd", or "dp"

## Current versions
- Provenance captured from Python 3.5
- noWorkflow 1.11
- Language Level Prov-JSON 2.1 

## Future Features
- Improve interface to converter
- more support for provenance inside of function calls
- more support for provenance inside of loops
- Finish refactoring to OOP format
- Data nodes should save variable type 
- make it look less hacky 

