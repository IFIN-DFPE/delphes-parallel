# Parallel Delphes

## Description

[Delphes](https://delphes.github.io/) is a well-known C++ framework for fast detector simulation (e.g. for high-energy particle physics). Unfortunately, it does not natively take advantage of multiple processor cores; the Delphes executable runs on a single thread. Furthermore, due to architectural limitations (e.g. global variables), it would be pretty hard to make it run in parallel.

Fortunately, there is one simple solution: _data sharding_. We split up an input HepMC file into multiple parts (smaller HepMC files, each containing an equally-sized subset of the original list of events), then run many Delphes processes in parallel (with the same configuration). The result is an overall speed-up of the detector simulation process.
