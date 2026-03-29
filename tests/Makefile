CXXFLAGS ?= -Wall -Wextra -Werror -O3 -march=native -flto=auto

LINKER_FLAGS ?= -lpythia8 -lHepMC

DELPHES_CONFIG_FILE ?= /data/bsm/software/installed/delphes/3.5.1/cards/delphes_card_ATLAS.tcl

.PHONY: run clean clean-binaries clean-data clean-output clean-logs

run: output.root

clean: clean-binaries clean-data clean-output clean-logs

clean-binaries:
	-rm *.exe

clean-data:
	-rm *.hepmc

clean-output:
	-rm *.root

clean-logs:
	-rm *.log

output.root: events.hepmc
	DelphesHepMC2 $(DELPHES_CONFIG_FILE) output.root events.hepmc &> delphes.log

events.hepmc: | pythia8-main.exe
	./pythia8-main.exe &> pythia8-output.log

pythia8-main.exe: pythia8-main.c++
	$(CXX) $(CXXFLAGS) $(LINKER_FLAGS) $< -o $@
