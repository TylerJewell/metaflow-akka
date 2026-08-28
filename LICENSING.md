# Licensing

All of this repository is licensed under the Apache License 2.0, Copyright 2026
Tyler Jewell. See `LICENSE` and `NOTICE`.

## Why one licence and not two

Other rebuilds in this experiment carry a second licence, because they reuse the
original's front end verbatim under this harness's RENDERING.md R3, and that code stays
under the licence its authors chose. This one vendors nothing: every file of code here was
written for this project, so there is no second body of code for a second licence to
govern.

A `LICENSE-metaflow` file may sit beside this one. That is the original's licence text
reproduced for attribution, cited by `ACKNOWLEDGEMENTS.md`; it is not a grant over
anything in this repository, because nothing here is the original's code.

The boundary is checked rather than asserted. Every file in this repository was compared
against the cloned original looking for byte-identical matches, and the `.vendored`
manifest records what that found. A second licence appears here the moment it finds
something.

## What the original was

This is a clean-room implementation of [Netflix/metaflow](https://github.com/Netflix/metaflow), Copyright
2020 Netflix, Inc, licensed under Apache-2.0. It was written against a specification derived
by running that system and recording what it does, not by translating its source. See
`../metaflow-port/specs/SPEC-001-metaflow.md` for the rules it was built to, and `ACKNOWLEDGEMENTS.md` for any text carried
across and why.

## A note on the runtime

The rebuild runs on the Akka SDK, which is distributed under the Business Source License
1.1 and converts to Apache-2.0 three years after each release. Apache-2.0 on this
repository's own code does not grant any right to Akka; running this in production needs
whatever Akka's licence requires at the time.
