#include <string.h>
#include <unistd.h>
#include <curl/curl.h>
#include <cstdlib>

#include <string>
#include <sstream>
#include <fstream>
#include <vector>
#include <thread>
#include <chrono>
#include <boost/program_options.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <iostream>

namespace s = std;
namespace c = s::chrono;
namespace l = boost::log;
namespace sev = boost::log::trivial;
namespace po = boost::program_options;

const char* const COINBASE_URL = "https://api.exchange.coinbase.com/products/";
const char* const USER_AGENT = "the_crypt";

struct memory {
  char* response;
  size_t size;
};

size_t write_buffer(void* dat, size_t sz, size_t nmemb, void* userp){
  size_t realsize = sz * nmemb;
  memory* mem = (memory*)userp;

  void* ptr = realloc(mem->response, mem->size + realsize + 1);
  if (ptr == NULL){
    return 0; // out of memory
  }

  mem->response = (char*)ptr;
  memcpy(&(mem->response[mem->size]), dat, realsize);
  mem->size += realsize;
  mem->response[mem->size] = 0;

  return realsize;
}

l::sources::severity_logger<l::trivial::severity_level>& logger(){
  static l::sources::severity_logger<l::trivial::severity_level> logger;
  return logger;
}

void init_log(const char* filename){
  l::add_file_log(
      l::keywords::file_name = filename,
      l::keywords::format ="[%TimeStamp%]: %Message%"
  );
  l::core::get()->set_filter(l::trivial::severity >= l::trivial::info);
  l::add_common_attributes();
}

void extraction(const s::string& url, CURL* hnd, s::ofstream& ofile){
    memory chunk = {0};

    curl_easy_setopt(hnd, CURLOPT_CUSTOMREQUEST, "GET");
    curl_easy_setopt(hnd, CURLOPT_URL, url.c_str());
    curl_easy_setopt(hnd, CURLOPT_USERAGENT, USER_AGENT);
    curl_easy_setopt(hnd, CURLOPT_WRITEFUNCTION, write_buffer);
    curl_easy_setopt(hnd, CURLOPT_WRITEDATA, (void*)&chunk);

    curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Accept: application/json");
    curl_easy_setopt(hnd, CURLOPT_HTTPHEADER, headers);

    // cannot use ret to do anything useful
    curl_easy_perform(hnd);

    ofile << chunk.response;
    ofile.flush();
}

void sim_extraction(const s::string& product, int level, int interval, int total, const s::string& prefix, int epoch){
  s::stringstream book_outss;
  book_outss << prefix << "_book_" << s::to_string(epoch) << ".json";
  s::string book_outfile = book_outss.str();
  s::ofstream book_ofile(book_outfile.c_str());

  s::stringstream stats_outss;
  stats_outss << prefix << "_stats_" << s::to_string(epoch) << ".json";
  s::string stats_outfile = stats_outss.str();
  s::ofstream stats_ofile(stats_outfile.c_str());

  s::stringstream ticker_outss;
  ticker_outss << prefix << "_ticker_" << s::to_string(epoch) << ".json";
  s::string ticker_outfile = ticker_outss.str();
  s::ofstream ticker_ofile(ticker_outfile.c_str());

  s::stringstream book_urlss;
  book_urlss << COINBASE_URL << product << "/book?level=" << s::to_string(level);
  s::string book_url = book_urlss.str();

  s::stringstream stats_urlss;
  stats_urlss << COINBASE_URL << product << "/stats";
  s::string stats_url = stats_urlss.str();

  s::stringstream ticker_urlss;
  ticker_urlss << COINBASE_URL << product << "/ticker";
  s::string ticker_url = ticker_urlss.str();

  CURL* hnd = curl_easy_init();

  if (not hnd){
    BOOST_LOG_SEV(logger(), sev::error) << __FUNCTION__ << " CURL handle cannot be allocated. product: " << product << " prefix: " << prefix;
    return;
  }

  bool book_is_first = true;
  bool stats_is_first = true;
  bool ticker_is_first = true;

  book_ofile << "[";
  stats_ofile << "[";
  ticker_ofile << "[";

  for (int i = 0; i < total; ++i){
    if (book_is_first) book_is_first = false;
    else               book_ofile << ",";
    extraction(book_url, hnd, book_ofile);
    usleep(1000000);
    if (stats_is_first) stats_is_first = false;
    else                stats_ofile << ",";
    extraction(stats_url, hnd, stats_ofile);
    usleep(1000000);
    if (ticker_is_first) ticker_is_first = false;
    else                 ticker_ofile << ",";
    extraction(ticker_url, hnd, ticker_ofile);
    usleep(interval - 2000000);
  }

  book_ofile << "]";
  stats_ofile << "]";
  ticker_ofile << "]";

  book_ofile.close();
  stats_ofile.close();
  ticker_ofile.close();

  curl_easy_cleanup(hnd);
}

s::vector<s::string> parse_products(const s::string& pids){
  s::vector<s::string> products;

  if (pids.size() == 0) return products;

  int beg = 0;
  for (int i = 1; i < pids.size(); ++i){
    if (pids[i] == ':'){
      s::string pid = pids.substr(beg, i - beg);
      products.push_back(pid);
      beg = i+1;
    }
  }
  s::string pid = pids.substr(beg, pids.size() - beg);
  products.push_back(pid);

  return products;
}

int main(int argc, char* argv[]){
  po::options_description desc("Parameters");

  try {
    desc.add_options()
      ("pid", po::value<s::string>()->required(), "list of product id separated by ':'")
      ("level", po::value<int>()->default_value(1), "order book level")
      ("interval", po::value<int>()->required(), "time between extraction in microseconds")
      ("total", po::value<int>()->required(), "total number of sequences to extract")
      ("prefix", po::value<s::string>()->required(), "output file name prefix")
      ("logfile", po::value<s::string>()->required(), "logfile name");
    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    s::string pids = vm["pid"].as<s::string>();
    int level = vm["level"].as<int>();
    int interval = vm["interval"].as<int>();
    int total = vm["total"].as<int>();
    s::string prefix = vm["prefix"].as<s::string>();
    s::string logfile = vm["logfile"].as<s::string>();

    init_log(logfile.c_str());

    s::vector<s::string> products = parse_products(pids);
    s::vector<s::thread> threads;

    int epoch = c::duration_cast<c::seconds>(c::system_clock::now().time_since_epoch()).count();

    BOOST_LOG_SEV(logger(), sev::info) << "Extracting for products: ";
    for (int i = 0; i < products.size(); ++i){
      BOOST_LOG_SEV(logger(), sev::info) << products[i];
    }

    for (const s::string& product : products){
      threads.emplace_back(sim_extraction, product, level, interval, total, prefix, epoch);
    }

    for (s::thread& t : threads){
      t.join();
    }

    BOOST_LOG_SEV(logger(), sev::info) << __FUNCTION__ << "Extraction complete";
  } catch (po::error& e){
    s::cerr << "option parsing error: " << e.what() << s::endl;
    s::cerr << desc << s::endl;
  } catch (s::exception& e){
    s::cerr << "exception: " << e.what() << s::endl;
  }
}
