cmake_minimum_required(VERSION 3.10)

project(cbp-extractor)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
add_definitions(-DBOOST_LOG_DYN_LINK)
find_package(Boost 1.76.0 COMPONENTS system program_options log log_setup REQUIRED)
include_directories( ${BOOST_INCLUDE_DIRS} )

set(CURL_LIBRARY -lcurl)
find_package(CURL REQUIRED)
include_directories( ${CURL_INCLUDE_DIR} )

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR/lib})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR/lib})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR/bin})

add_subdirectory(src)
