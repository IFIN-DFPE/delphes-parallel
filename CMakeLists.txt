cmake_minimum_required(VERSION 3.23)

project(DelphesParallel)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
find_package(HepMC2 REQUIRED)

add_executable(DelphesHepMC2Parallel)
target_sources(DelphesHepMC2Parallel
  PRIVATE
    src/DelphesHepMC2Parallel.c++
)

target_include_directories(DelphesHepMC2Parallel PRIVATE ${HEPMC2_INCLUDE_DIR})
target_link_libraries(DelphesHepMC2Parallel ${HEPMC2_LIBRARY})
