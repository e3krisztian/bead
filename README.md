[![Build Status](https://travis-ci.org/e3krisztian/bead.svg?branch=master)](https://travis-ci.org/e3krisztian/bead)

    B-E-+
     \ \ \
      +-A-D

# BEAD


BEAD is a format for freezing and storing computations while `bead` is a tool that helps
capturing and managing computations in BEAD formats.


## Concept

Given a discrete computation of the form

    output = function(*inputs)

a BEAD captures all three named parts:

- `output` is the result of the computation - data, it is contained verbatim
- `function` is a code that computes `output` from the `inputs`
- `inputs` are stored as references to other BEADs - they are the `output` part of
  computations

It also has some other metadata - e.g. a unique identifier that is supposed to be common 
between different versions (either part is updated) of the same computation and a timestamp
when it was frozen.


## Status

### bead as a tool *is not production ready, yet!*

Although most of the important stuff is implemented, the metadata format is still not fixed,
which might render every existing BEAD incompatible after the change.

Documentation for the tool is non-existent at this point - except for command line help.
The doc directory currently contains design fragments - you will be mislead by them as they 
are nor describing the current situations nor are they showing the future.


## Install instructions

Ensure you have Python installed.

Run the `build.py` script to create the `bead` tool:

```
$ python build.py
```

This generates one-file executables for unix, mac, and windows in the `build` directory:
- `bead` unix & mac
- `bead.cmd` windows

Move/copy the `bead` binary for your platform to some directory on your `PATH`.

E.g.

```
$ cp build/bead ~/.local/bin
```

If you test it, please give [feedback](../../issues) on
- general usability
- misleading/unclear help (currently: command line help)
- what is missing (I know about documentation)
- what is not working as you would expect

Any other nuisance reported - however minor you think it be - is important and welcome!

Thank you for your interest!


## TODOs

Updated by script ([./ci](https://github.com/e3krisztian/bead/blob/prev-next/ci)), text in this section and afterwards will be overwritten

- [arg(BEAD_QUERY)  # XXX: add: arg(BEAD_TIME) as a generic query should not be supported](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/input.py#L59)
- [arg(BEAD_QUERY)  # XXX: develop: arg(BEAD_TIME) as a generic query should not be supported](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/workspace.py#L131)
- [FIXME: die with message when directory already exists](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/workspace.py#L50)
- [FIXME: find_beads(self, time) -> (right before, exactly at, right after)](https://github.com/e3krisztian/bead/blob/prev-next/bead/box.py#L113)
- [FIXME: newer-than, older-than -> -t time](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/common.py#L128)
- [FIXME: robot: environment file should be built by a function in environment](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/test_robot.py#L36)
- [FIXME: test_archive.given_a_bead is fragile and yields an invalid BEAD](https://github.com/e3krisztian/bead/blob/prev-next/bead/test_archive.py#L46)
- [FIXME: update: is an inconsistent mess](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/input.py#L129)
- [from tracelog import TRACELOG  # TODO: remove TRACELOG](https://github.com/e3krisztian/bead/blob/prev-next/bead/box.py#L10)
- [from tracelog import TRACELOG  # TODO: remove TRACELOG](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/common.py#L6)
- [TODO: add tests for timestamps, parse_iso8601, parse_timedelta](https://github.com/e3krisztian/bead/blob/prev-next/bead/tech/timestamp.py#L232)
- [TODO: Box: support user maintained directory hierarchy](https://github.com/e3krisztian/bead/blob/prev-next/bead/box.py#L98)
- [TODO: implement more options](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/common.py#L120)
- [TODO: support shortened content hashes](https://github.com/e3krisztian/bead/blob/prev-next/bead/spec.py#L19)
- [TODO: update: assert there is no other argument](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/input.py#L117)
- [TODO: use a template and render it with passing in all data](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/workspace.py#L233)
- [XXX: cli parsing: revisit when python 2.x no longer supported](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/cmdparse.py#L86)
- [XXX: directory itself might be a pattern - is it OK?](https://github.com/e3krisztian/bead/blob/prev-next/bead/box.py#L139)
- [XXX: introspect parameter names, default values, annotations?](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/cmdparse.py#L121)
- [XXX: list command: use tabulate?](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/box.py#L60)
- [XXX: order_and_limit_beads is called twice - first in find_beads](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/common.py#L207)
- [XXX: try to load smaller inputs?](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/workspace.py#L162)
- [XXX: (usability) save - support saving directly to a directory outside of workspace](https://github.com/e3krisztian/bead/blob/prev-next/bead_cli/workspace.py#L90)
