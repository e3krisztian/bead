# Quickstart

- [create a new bead from existing data](#create-from-data)
- [derive a new bead from existing beads by computation](#derive-new)
- [update a bead for new input data (create a new version)](#update-with-new-data)
- [update algorithm to create a bead (create a new version)](#update-with-new-algorithm)
- [use data in bead without the tool](#oblivous-use)
- visualize data flow
- create a reproducible set of beads
- publish how-to-reproduce document


## <a name="create-from-data"></a>Create a new bead from existing data

There are existing data that are to be converted to beads.

- `bead new BEAD-NAME`  (create workspace for a new bead)
- `cp data-files BEAD-NAME/output`  (copy data into workspace)
- `cd BEAD-NAME`
- `bead save --to REPOSITORY`  (create bead from workspace)


## <a name="derive-new"></a>Derive a new bead from existing beads by computation

- `bead new BEAD-NAME`  (create workspace for a new bead)
- `cd BEAD-NAME`
- `bead mount BEAD1 NAME1`  (make data from `BEAD1` [version] available at `input/NAME1`)
- ...
- `bead mount BEADn NAMEn`  (make data from `BEADn` [version] available at `input/NAMEn`)
- create some program under the current directory (e.g. python or STATA scripts) that uses data from `input` and produces some data under `output`
- run the program
- `bead save --to REPOSITORY`  (create bead from workspace)


## <a name="update-with-new-data"></a>Update a bead for new input data (create a new version)

- `bead develop BEAD-NAME`  (create workspace )
- `cd BEAD-NAME`
- `bead input update NEW-INPUT-NAME` (update data at `input/NEW-INPUT-NAME` from the newest version)
  - By default, matches by both input name and kind for precise updates
  - Use `--no-kind` to update to any bead with the same name regardless of kind
  - Use `--no-name` to update to any bead with the same kind regardless of name
- run the program
- `bead save`


## <a name="update-with-new-algorithm"></a>Update algorithm to create a bead (create a new version)

- `bead develop BEAD-NAME`  (create workspace )
- `cd BEAD-NAME`
- modify code
- run the program
- `bead save`

## <a name="oblivous-use"></a>Use data in bead without the tool

A bead is a zip file, so with an unarchiver the data can be extracted and used.

Although it is easy to get access to data, it should be last resort - if you process the data in this way the references to inputs can not be tracked for you.
