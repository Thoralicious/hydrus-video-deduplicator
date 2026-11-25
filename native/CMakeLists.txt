cmake_minimum_required(VERSION 3.18...3.29)
project(hvd_native LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)
find_package(Threads REQUIRED)

add_subdirectory(pdq)
add_subdirectory(vpdq)
pybind11_add_module(_native MODULE bindings.cpp)
target_link_libraries(_native PRIVATE pdqlib vpdqlib Threads::Threads)
install(TARGETS _native DESTINATION hydrusvideodeduplicator)

install(FILES LICENSE-hvdaccelerators.txt DESTINATION hydrusvideodeduplicator/_licenses)
install(FILES pdq/LICENSE.txt DESTINATION hydrusvideodeduplicator/_licenses RENAME PDQ.txt)
install(FILES vpdq/LICENSE.txt DESTINATION hydrusvideodeduplicator/_licenses RENAME VPDQ.txt)
