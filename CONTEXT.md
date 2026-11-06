# Worker fleet

The worker fleet supplies execution capacity for cards and flows across orchestration engines.

## Language

**Execution worker**:
A registered service that runs card or flow tasks on a machine.

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
