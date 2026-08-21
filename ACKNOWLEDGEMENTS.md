# Acknowledgements

This project is a port of **[Netflix/metaflow](https://github.com/Netflix/metaflow)**.

**Licence and copyright.** Netflix/metaflow is under the Apache License 2.0, Copyright 2020
Netflix, Inc. — read from the `LICENSE` file of the clone at version 2.19.38, not from a
badge.

**Was anything copied verbatim?** No source was copied. The rebuild is written in Java from
the specification in `specs/SPEC-001-metaflow.md`, and shares no file, no function body and
no test fixture with the original. Four short strings were reproduced deliberately and are
worth naming: the artifact metadata field names `size`, `type` and `encoding`, the encoding
label `pickle-v4`, and the shape of a task address `flow/run/step/task`. They are
reproduced because an address or a label that differs is a system a caller cannot move
between, which is the whole point of the comparison in `bench/REPORT.md`.

**Is behaviour derived?** Yes, and deliberately. Every rule in the specification's
deterministic contract was established by running Netflix/metaflow and writing down what it
did — the probes in `probes/` are that work, and `docs/question-log.md` says which claim was
settled by which run. Three rules are this port's own where the original had no settled
answer, and each says so in the specification and in the published README.

**What licence does that force on this project?** Nothing was copied, so nothing carries
Apache-2.0 into this repository by inheritance. The port is nonetheless a derived work in
the ordinary sense — its behaviour was learned from someone else's system — and the
attribution above is part of publishing it.

## Also used

- **Akka SDK for Java** 3.6.3 (`io.akka:akka-javasdk-parent`), Business Source License 1.1,
  Lightbend Inc. — the platform the rebuild runs on.
- **CPython 3.11** and the `python:3.11-slim` container image, used to run the original
  during the probes because metaflow does not import on Windows.
