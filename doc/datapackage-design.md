# Requirements

- as simple as possible
- make it easy to work with for non-programmers
- make it hard to make mistakes
- peer to peer by default - file exchange is outside the core system
- reproducibility must be verifiable
- must be cross-platform (Win-Mac-Lin)
- updates: easily create newer packages based on new input
- access control - allow for restricted packages
- packages might be owned, but ownership surely changes over time


# Assumptions

- code is small relative to data
- python app is OK


# Roles

- package developer (programmer)
- package data user (programmer, analyst)
- [external] reproducer (anyone?)


# Use cases

- developer:
    + create new package layout - with directory structure
    + unpack existing pkg for development (w/ most recent input data)
    + manage input data
        + mount input data (name, channel, version, input_name)
            * extract data under input/<input_name>
            * register (name-uuid, content-hash, input_name) in metadata
        + unmount input data (delete input_name)
        + rename input data (rename input_name)
        + set upgrade channel (input_name, channel)
        + upgrade input data (input_name[, version-hash])
    + create archive from directory
    + publish package as archive on a repository
    + make package available on a channel
    + export package from a repository as an archive

- data user:
    + get data
    ? get data as okfn datapackage

- reproducer:
    + check [partial] reproducibility
        * get exported data packages
        * build a multi-package environment with
            - input data
            - code for non-input data
            - script to build (snakemake?)
            - reference output


# Datapackage

A datapackage is an immutable archive.

During development it is a directory with well defined layout.
The directory layout differs from the archive layout (they have different intended uses), but tools can easily convert between the two.


## Archive

The archive contains

- data
- code
- meta:
    + ordering: time stamp
    + catalog:
        + package name (package-uuid)
        + optional per-input_name update channel information (channel-uuids)
	+ how to reproduce:
	    + references to input data packages (name, version, input_name)
	    + how to run the code

## Directory

The directory contains

- input directory
    + read only, mount points for input data packages:
        * input_name1/data...
        * input_name2/data...
        * ...
- output directory
    + initially empty
    + it is what becomes the *data* directory in the datapackage archive
- tmp directory
    + initially empty
    + the script can use it to keep intermediate results, between its processing phases
- .pkgmeta
    + datapackage descriptor, which defines
        * the name of the package (uuid)
        * what is contained in the input directory
        * what output files are to be found
        * how to run the code
- source code, which
    + is every other file, directory, not described above
    + is stored under *code* in the datapackage archive


### Storage standards

- bagit (data integrity with md5, sha1 sums)
- okfn datapackage (metadata & generic tools)


## Attributes

A package has a set of immutable attributes:

- package name
    - embdedded in the archive
    - it is a machine processable UUID, rather than human readable name
- creation time
    - embedded in the archive
- version
    - secure hash generated from the archive content as a whole


# Repository

A repository is maintaining a collection of packages.

- new packages can be deposited in
- existing packages can be retrieved by (name, version)
- provides manageable mapping between human mnemonics and technical uuids (package name and channel)
- provides ordering of packages via per-package channels for upgrades:
    (name, channel) -> ordered list of versions


## Requirements

- must be easy to set up - one or more by developers?, one per project?
- must be cross-platform (Win-Mac-Lin)
- may provide remote access
- must have local (offline) mode
- the same package can be stored again under different channel
- must ensure integrity of packages
- should be easy to incrementally back up
- may provide tools to discover and remove unused or unreproducible packages


## Annotations

Annotations are volatile and stored separately from the archives in the repository.

- description
- developer info
- flags ("reproduced")
- tags ("exhibit-20140000-1", "test")

# Channels

Channels can be used for keeping track of improvements of compatible packages.
Channels can also be used to differentiate between incompatible streams of packages sharing the same name.
They are a tool to enable continuous improvements and updates.
Packages within channels are automatically sorted by their timestamps.

Out of order package releases can be made without disrupting a channel - by not including the package in any channel.

Position in a channel can be thought of as a numeric version, the last one is the most current version.

Channels are mutable, packages can be added and removed.
Constraint: packages with timestamps in the future can not be in channels.


## Channel attributes

### Closed channels

Channels can be *open* or *closed*.

Closed channels do not get more updates.

Closed channels can point to another channel as incompatible continuation (*the next version*).

### Test channels

A channel can point to *test channel*, which holds compatible packages with significantly smaller size.
Tools can support switching to test channels during development.

Special problem: data package created from test data

Tools MUST allow to publish them to *test channels*

Tools MUST make it hard to accidentally release them on *production channels* accidentally


# Storage

- http://en.wikipedia.org/wiki/Content-addressable_storage
- git? http://git-scm.com/book/en/Git-Internals-Git-Objects


# Open problems

- how to configure repos

    XXX-config <repo> <cache>

- how do releasing work?

    XXX-release [<channel>]

- security: restricted package access

    - Encrypt packages?
    - apply unix permissions on parts of a repo?
    - chain multiple repos with different permissions?
