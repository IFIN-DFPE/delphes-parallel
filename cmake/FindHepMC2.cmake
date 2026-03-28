# Find the HepMC2 includes and library.

set(_hepmc2dirs
    ${HEPMC}
    $ENV{HEPMC}
    ${HEPMC2}
    $ENV{HEPMC2}
    ${HEPMC_DIR}
    $ENV{HEPMC_DIR}
    ${HEPMC2_DIR}
    $ENV{HEPMC2_DIR}
    ${HEPMC2_ROOT_DIR}
    $ENV{HEPMC2_ROOT_DIR}
    /usr
    /usr/local
    /opt/hepmc
    /opt/hepmc2)

find_path(HEPMC2_INCLUDE_DIR
          NAMES HepMC/GenEvent.h
          HINTS ${_hepmc2dirs}
          PATH_SUFFIXES include include/HepMC)

find_library(HEPMC2_LIBRARY
             NAMES hepmc HepMC
             HINTS ${_hepmc2dirs}
             PATH_SUFFIXES lib)

unset(_hepmc2dirs)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(HepMC2 DEFAULT_MSG HEPMC2_INCLUDE_DIR HEPMC2_LIBRARY)
mark_as_advanced(HEPMC2_INCLUDE_DIR HEPMC2_LIBRARY)
