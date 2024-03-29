Date created: 10/12/23

---

This project was an exercise in the Raft replicated state machine protocol and the Go language. Referencing a lab assignment adapted from MIT's 2021 6.824 course with the support of Frans Kaashoek, Robert Morris, and Nickolai Zeldovich:

	"Raft is a replicated state machine protocol. A replicated service (e.g., key/value database) achieves 
	fault tolerance by storing copies of its data on multiple replica servers. Replication allows the 
	service to continue operating even if some of its servers experience failures (crashes or a broken or 
	flaky network). The challenge is that failures may cause the replicas to hold differing copies
	of the data...
	...Raft manages a service's state replicas, and in particular it helps the service sort out what the correct
	state is after failures. Raft implements state machine replication. It organizes client requests into a 
	sequence, called the log, and ensures that all the replicas agree on the contents of the log. Each replica 
	executes the client requests in the log in the order they appear in the log, applying those requests to 
	the replica's local copy of the service's state. Since all the live replicas see the same log contents, 
	they all execute the same requests in the same order, and thus continue to have identical service state. 
	If a server fails but later recovers, Raft takes care of bringing its log up to date. Raft will continue 
	to operate as long as at least a majority of the servers are alive and can talk to each other. If there is
	no such majority, Raft will make no progress, but will pick up where it left off as soon as a majority 
	can communicate again."

See https://en.wikipedia.org/wiki/Raft_(algorithm) for more details about Raft. More project details are available upon request.

*Note: This implemention relies on function and type definitions from files not provided in this repository. As such, the provided Raft.go file does not compile on its own.*