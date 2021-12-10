#!/bin/sh

g++ -o book_extractor -DBOOST_LOG_DYN_LINK -lcurl -lboost_program_options -lboost_log -lboost_log_setup -lboost_thread -lboost_system -lpthread book_extractor.cpp
