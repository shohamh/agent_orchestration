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

**Worker capacity**:
The execution capacity of a worker shared by tasks from all orchestration engines.
