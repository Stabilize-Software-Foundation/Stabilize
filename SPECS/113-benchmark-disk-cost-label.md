# 113: benchmark-overhead.py reports disk commit time as engine overhead

Ticket: #113. Opened 2026-11-24 from Webmail v2 rerun of 0.34.6 (AgentBus thread 01M85TXENQDS1Z7YKV1Y1QVD90).

EARS SPEC:
- The examples/benchmark-overhead.py summary shall print the SQLite journal_mode and synchronous setting it ran with and the measured cost of one commit on the directory it used (median of at least 50 commits, in ms).
- The summary shall report the time spent in commits (commit count x measured commit cost) as a line of its own, apart from the line it labels orchestration overhead.

Reported by Webmail v2 on 2026-11-24 (thread 01M85TXENQDS1Z7YKV1Y1QVD90, message 01M871FBTC9BQ2K3RXK54T64Z6) after rerunning on 0.34.6: phase 3 took 22.1, 24.2 and 27.1 s with the temporary directory on an xfs disk and 1.73 and 1.60 s on tmpfs, and the summary printed +22124ms (+117960%) as overhead against the raw baseline. Their sampling found 7.9 threads on average inside persistence/connection.py execute. The benchmark runs with the default tier safe (journal_mode DELETE, synchronous NORMAL, sqlite_config.py). A one-worker profile here counted 862 SQLite commits for the 198 messages of the fan-out.

Synthesis: solution domain benchmarking, concept separating storage latency from the measured system. Alternatives: print the mode and a measured commit cost [CHOSEN]; run the benchmark on tmpfs by default [REJECTED: hides the cost a reader will meet on disk]; switch the example to WAL [REJECTED: the example would no longer run the default configuration].

## Update 2026-11-24

Webmail measured on their disk: one commit under the benchmark default (journal DELETE, synchronous NORMAL) costs 15.6 to 16.0 ms, and WAL did not shorten phase 3 there. strace on the default run showed 5,105 pread64 calls taking 21.4 s (4.2 ms each), 2,703 fsync calls taking 7.0 s, and 9,941 clock_nanosleep calls summing to 215 s across threads. The slow reads are unexplained.

Added requirement: the summary shall also print the measured cost of one read of the database file in its directory (median of at least 50 reads, in ms).
