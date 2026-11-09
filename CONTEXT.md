# Worker fleet

The worker fleet supplies execution capacity for cards and flows across orchestration engines.

## Language

**Execution worker**:
A registered service that runs card or flow tasks on a machine.

**Worker unregistration**:
A server-side removal of an execution worker from the Fleet that revokes its relay and engine routing and frees its name for a new registration. The local installation remains; restoring access requires a new Fleet install link, which replaces the local worker identity in place.

**Worker pool**:
A named group of execution workers used to select where tasks can run. An execution worker can belong to multiple pools.

**Capability tag**:
A named attribute used to describe an execution worker and match it to task requirements.

**Detected software**:
An agent or command observed on an execution worker during a capability refresh. Detection is separate from manually assigned capability tags.

**Program card**:
A card that runs one command on an execution worker, without an agent.

**Attached source**:
A file, git repository or Confluence page tree given to one run when it starts, as opposed to a flow's `environment`.

**Schedule**:
A stored card that starts later, repeatedly or both. The flow host stores it and the board starts it.

**Worker capacity**:
The execution capacity of a worker shared by tasks from all orchestration engines.

**PC-native Hatchet worker**:
A Hatchet SDK worker running on a registered PC, using that PC's dedicated worker-capable Hatchet token and executing compatible routed work there.

**PC-native Prefect worker**:
A Prefect process worker running on a registered PC and polling a reused or created work pool/queue to execute compatible routed work there.

**Trigger.dev Fleet relay**:
The shared Fleet execution-service bridge used by Trigger.dev; it is not a Trigger.dev worker on each PC. Trigger.dev deployed workers remain isolated containers.

**Native runtime bundle**:
The offline-distribution artifacts `native-runtime-windows.tar.gz` and `native-runtime-linux.tar.gz` stored under the bundle's `worker/` directory.

**Protected claim secret**:
The per-PC Hatchet token delivered only in the response to that PC's valid install claim; it is not part of the install command, public registration view or worker status.
