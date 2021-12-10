add_executable(book_extractor book_extractor.cpp)

set_property(TARGET book_extractor PROPERTY CXX_STANDARD 17)

target_link_libraries(book_extractor ${CURL_LIBRARIES})
target_link_libraries(book_extractor Threads::Threads)
target_link_libraries(book_extractor Boost::program_options Boost::log Boost::system Boost::log_setup)
