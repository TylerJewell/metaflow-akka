# metaflow-akka

Records what a workflow run produced: every artifact stored once under the fingerprint of
its own bytes, every attempt at a task kept, and every later run that inherits an artifact
pointing at the same bytes rather than a copy.

A port of [Netflix/metaflow](https://github.com/Netflix/metaflow) onto **Akka**, built with
**Akka Specify**.

---

## Where it came from

Netflix/metaflow is a framework for writing data-science workflows in Python and running
them on a laptop or on a cluster. It was ported to derive a specification format precise
enough to regenerate a system on a different stack — the port is the vehicle, the
specification is the deliverable.

This port takes one tier of it: the part that decides what a run recorded, which is the
part that has to be right for any of the rest to mean anything. It does not run workflows.

The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness)
under `metaflow-port/`.

---

## Netflix/metaflow → this port

📉 1,255 Python lines → **883 Java lines**<br>
📁 5 files → **21 files**<br>
⚡ 0.235 ms to store an artifact → **19.578 ms**<br>
⚡ 0.046 ms to read a task's latest attempt → **15.360 ms**<br>
🎯 14 scenarios put to both, 11 answered identically → **11 identical, 3 differences decided in advance**<br>
🔑 artifact fingerprints matching across both systems → **all of them**<br>
📦 no limit on how large one artifact may be → **786,000 bytes, refused above that**

Full method and the numbers that did *not* make this list:
[`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/metaflow-port/bench/REPORT.md).

---

## What it took to build

⏱️ **1.2 hours** from the first command to the published repository, **1.2** of them active<br>
💬 **312** exchanges with the model<br>
✍️ **322,000** tokens written by the model, **67,400,000** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **66** tests

```bash
python toolkit/tokens.py --port metaflow    # turns, tokens, elapsed and active time
```

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

From the specification:

- **An artifact's name is the fingerprint of its own bytes.** Storing the same bytes twice
  writes them once, and an address worked out here matches the address Netflix/metaflow
  works out for the same bytes.
- **A finished attempt never changes again.** Once a task's attempt is closed, nothing can
  be added to it, and it cannot be opened a second time.
- **An unfinished attempt hides the finished one under it.** Asking a task what it holds,
  without saying which attempt, is refused while the newest attempt is still running —
  asking for the earlier attempt by number still works.
- **Inheriting an artifact copies its address, not its bytes.** A run that resumes another,
  a step that hands a named value on, and a branch point that combines two paths all record
  the same address the artifact already had.
- **Two paths that meet may only combine values they agree on.** Where two incoming paths
  carry the same name pointing at different bytes, the combination is refused and every
  disagreeing name is listed.
- **Any stored artifact can be traced.** Given a fingerprint, the service names every task
  in every run that recorded it.

---

## Design decisions

**Content addressing.** Naming a stored value after a fingerprint of the value itself means
two callers who save the same thing cannot disagree about where it lives. Storing it twice
costs nothing, and any two systems that use the same fingerprint can hand addresses to each
other.

**One record per task, holding every attempt.** All the decisions about a task — which try
is the newest, which tries finished, what each one produced — need to be made together, so
they are kept together and changed one command at a time. Nothing can read a half-written
attempt, and two commands arriving at once cannot interleave.

**Refusing rather than answering.** Where the original answers a question about an
unfinished attempt with an internal failure, this one refuses in a sentence that names the
task and says why. A caller can tell "still running" apart from "there is nothing here",
which is the difference between waiting and giving up.

**A stated ceiling on artifact size.** The platform this runs on stops accepting a value
somewhere just past three-quarters of a megabyte, and it was measured rather than guessed.
Refusing above a stated size, with the size in the message, is something a caller can
handle; discovering the limit at the first large value is not.

**Success is something the task says, not something the record implies.** A task that fails
still finishes and still records what it had, exactly as a successful one does. Whether it
worked is a separate answer, so a failed attempt's artifacts can be read afterwards instead
of being lost with it.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/metaflow-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Try it** — store a value, record it against a task, read the task back:

```bash
curl -s -X POST --data-binary "the artifact" \
  -H "Content-Type: application/octet-stream" http://localhost:9026/artifacts
# {"key":"df8419382543043a6f6b87551c6138022fa58c8c","stored":true,"size":12}

curl -s -X POST http://localhost:9026/flows/Demo/runs/run-1/steps/start/tasks/t1/attempts/0
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"artifacts":[{"name":"x","key":"df8419382543043a6f6b87551c6138022fa58c8c","size":12,"encoding":"pickle-v4"}]}' \
  http://localhost:9026/flows/Demo/runs/run-1/steps/start/tasks/t1/attempts/0/artifacts
curl -s -X POST -H "Content-Type: application/json" -d '{"successful":true}' \
  http://localhost:9026/flows/Demo/runs/run-1/steps/start/tasks/t1/attempts/0/finish

curl -s http://localhost:9026/flows/Demo/runs/run-1/steps/start/tasks/t1
curl -s http://localhost:9026/lineage/artifacts/df8419382543043a6f6b87551c6138022fa58c8c
```

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9026**.

### Run the tests

```bash
mvn test
```

66 tests: the rules on their own, the same rules through each record, and every rule again
through the web interface with the service really running.

---

## What you can ask it

| What you want | How to ask |
|---|---|
| Store some bytes | `POST /artifacts` with the bytes as the body |
| Fetch bytes back | `GET /artifacts/{fingerprint}` |
| Start a try at a task | `POST /flows/{flow}/runs/{run}/steps/{step}/tasks/{task}/attempts/{n}` |
| Record what it produced | `POST …/attempts/{n}/artifacts` |
| Close it | `POST …/attempts/{n}/finish` with `{"successful": true}` |
| Read the newest closed try | `GET /flows/{flow}/runs/{run}/steps/{step}/tasks/{task}` |
| Read one try by number | `GET …/attempts/{n}` |
| Inherit everything another task recorded | `POST …/attempts/{n}/clone-from` with `{"originTaskId": "…"}` |
| Inherit some of it by name | `POST …/attempts/{n}/passdown` with `{"originTaskId": "…", "names": […]}` |
| Combine what two paths recorded | `POST …/attempts/{n}/merge` with `{"branchTaskIds": […]}` |
| Start a run, saying which run it continues | `POST /flows/{flow}/runs/{run}` with `{"originRunId": "…"}` |
| Find every task that recorded a fingerprint | `GET /lineage/artifacts/{fingerprint}` |
| List what a run recorded | `GET /lineage/runs/{flow}/{run}` |

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `akka.javasdk.dev-mode.http-port` | `9026` | set in `src/main/resources/application.conf`; the port the service answers on when run locally |

There is nothing else to configure. The service calls no model provider and no outside
service.

---

## Where it differs from Netflix/metaflow

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **Opening a try that already finished.** Netflix/metaflow accepts it: the earlier record
  is replaced by the new one and nothing says it was. This port refuses, because every other
  promise it makes — that inheriting an artifact copies what a task did, that two paths can
  be compared by what they recorded, that tracing a fingerprint tells you the truth — is
  about a record that does not move afterwards.
- **Asking a task what it holds while its newest try is still running.** Netflix/metaflow
  builds the answer object and then fails with an internal error the moment anything is read
  from it. This port refuses the question in a sentence naming the task, the try and its
  state, because an empty answer and a still-running one are different things to a caller and
  only one of them is worth waiting on.
- **Telling "no such task" apart from "nothing finished yet".** Netflix/metaflow gives the
  same error for both. This port gives two different ones, so a caller can tell whether to
  wait or to stop.
- **Listing the names two paths could not agree on.** Where three paths disagree about one
  name, Netflix/metaflow names it once per disagreement — `[v, v]` for three paths. This port
  names it once, because the list is what a caller has to act on and a repeat adds nothing to
  act on. The set of names is the same.
- **A limit on how large one artifact may be.** Netflix/metaflow writes whatever its storage
  accepts. This port refuses anything over 786,000 bytes and says so with both numbers in the
  message, because the platform underneath stops accepting a value a little above that and a
  stated limit is easier to work with than a discovered one.
- **What is turned into bytes, and by whom.** Netflix/metaflow turns Python values into
  bytes itself and fingerprints the result. This port takes bytes from the caller and
  fingerprints those, so the same value produces the same fingerprint in both systems only
  when the caller turns it into bytes the same way — which is what makes the two comparable
  at all.
- **How a stored artifact is squeezed.** Both compress what they keep. Netflix/metaflow uses
  a compression level of 3; this port uses the platform default. The fingerprint is taken
  before compression, so the addresses match either way, and the compressed forms are not
  compared — `not checked`.
- **Which try a trace answer describes.** A trace answer here describes a task's newest
  finished try. Whether Netflix/metaflow's own tracing, which lives in its metadata service
  and its separately-published browser interface, answers with the same try was `not checked`
  — neither is in the repository this port was read from.
- **Everything above the record.** Running workflow code, retrying steps, fanning out over a
  collection, and the compute backends are not attempted here. That is scope rather than a
  difference, and it is stated in the specification's first section.

---

## Licence

Netflix/metaflow is under the Apache License 2.0, © 2020 Netflix, Inc. This port
reimplements the behaviour without copied source; see
[`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md).
