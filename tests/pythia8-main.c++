// main131.cc is a part of the PYTHIA event generator.
// Copyright (C) 2026 Torbjorn Sjostrand.
// PYTHIA is licenced under the GNU GPL v2 or later, see COPYING for details.
// Please respect the MCnet Guidelines, see GUIDELINES for details.

// Authors:
//            Mikhail Kirsanov

// Keywords:
//            Basic usage
//            Hepmc

// This program illustrates how HepMC files can be written by Pythia8.
// It also studies the charged multiplicity distribution at the LHC.
// HepMC events are output to the main131.hepmc file.

// WARNING: typically one needs 25 MB/100 events at the LHC.
// Therefore large event samples may be impractical.

#include "Pythia8/Pythia.h"

// Use HepMC2, since that's what MadGraph defaults to.
#define HEPMC2

// Preferably use HepMC3, but alternatively HepMC2.
#ifndef HEPMC2
#include "Pythia8Plugins/HepMC3.h"
#else
#include "Pythia8Plugins/HepMC2.h"
#endif

using namespace Pythia8;

//==========================================================================

int main() {
  // Interface for conversion from Pythia8::Event to HepMC event.
  // Specify file where HepMC events will be stored.
  Pythia8ToHepMC toHepMC("events.hepmc");

  // Generator. Process selection. LHC initialization. Histogram.
  Pythia pythia;
  pythia.readString("Beams:eCM = 13600.0");
  pythia.readString("HardQCD:all = on");
  pythia.readString("PhaseSpace:pTHatMin = 20.0");

  // If Pythia fails to initialize, exit with error.
  if (!pythia.init()) return 1;

  // Begin event loop. Generate event. Skip if error.
  for (int iEvent = 0; iEvent < 5000; ++iEvent) {
    if (!pythia.next()) continue;

    // Construct new empty HepMC event, fill it and write it out.
    toHepMC.writeNextEvent(pythia);
  }

  // Done.
  return 0;
}
