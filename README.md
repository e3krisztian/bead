[![Tests](../../actions/workflows/test.yml/badge.svg)](../../actions/workflows/test.yml)


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

- `output` - *data files* (results of the computation)
- `function` - *source code files*, that when run hopefully compute `output` from `inputs`
- `inputs` - are other BEADs' `output` and thus stored as *references to* those *BEADs*

As a special case pure data can be thought of as *constant computation*
having only output but neither inputs nor source code.

A BEAD has some other metadata - notably it has a `kind` property which is shared by
different versions of the conceptually same computation (input or function may be updated/improved)
and a timestamp when the computation was frozen.

The `kind` and timestamp properties enable a meaningful `update` operation on inputs.

New computations get a new, universally unique `kind` (technically an uuid).


## Status

### Used in production since 2015, there are hundreds of frozen computations

Although most of the important stuff is implemented, there are still some raw edges.

Documentation for the tool is mostly the command line help.

The `doc` directory has concept descriptions, maybe some use cases,
but there are also design fragments - you might be mislead by them as they
are nor describing the current situations nor are they showing the future.

FIXME: clean up documentation.

NOTE: https://bead.zip has new user documentation (as of September, 2025).


## Install instructions

Ensure you have Python 3.10+ installed.

Run `make executables` to create the `bead` tool:

This generates one-file executables for unix, mac, and windows in the `executables` directory:
- `bead` unix & mac
- `bead.cmd` windows

Move/copy the `bead` binary for your platform to some directory on your `PATH`.

E.g.

```
$ cp executables/bead ~/.local/bin
```

----

Alternatively it can be installed with pipx:

For production use choose the latest released, non-pre-release version:
```
pipx install git+https://github.com/bead-project/bead@VERSION
```

or for latest development version:

```
pipx install git+https://github.com/bead-project/bead
```

----

If you test it, please give [feedback](../../issues) on
- general usability
- misleading/unclear help (currently: command line help)
- what is missing (I know about documentation)
- what is not working as you would expect

Any other nuisance reported - however minor you think it be - is important and welcome!

## Contributing

We welcome contributions! If you feel like working on code, please open an issue first to discuss your ideas.

This project is dedicated to the public domain via the [LICENSE](LICENSE) file. By submitting a pull request, you agree to irrevocably release your work under the same license.

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for detailed guidelines on our development process.

Thank you for your interest!
