#include <signal.h>
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
#include <boost/log/sinks/text_ostream_backend.hpp>
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

l::sources::severity_logger<l::trivial::severity_level>& logger(){
  static l::sources::severity_logger<l::trivial::severity_level> logger;
  return logger;
}

void init_log(const char* filename){
  boost::shared_ptr< l::core > core = l::core::get();

  // Create a backend and attach a couple of streams to it
  boost::shared_ptr< l::sinks::text_ostream_backend > backend = boost::make_shared< l::sinks::text_ostream_backend >();
  backend->add_stream(boost::shared_ptr< std::ostream >(new std::ofstream(filename)));

  // Enable auto-flushing after each log record written
  backend->auto_flush(true);

  // Wrap it into the frontend and register in the core.
  // The backend requires synchronization in the frontend.
  typedef l::sinks::synchronous_sink< l::sinks::text_ostream_backend > sink_t;
  boost::shared_ptr< sink_t > sink(new sink_t(backend));
  core->add_sink(sink);
  core->set_filter(l::trivial::severity >= l::trivial::info);
  l::add_common_attributes();
}

void sighandler(int s){
  BOOST_LOG_SEV(logger(), sev::info) << "caught signal " << s << ". ignore";
}

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
  book_outss << prefix << "_" << product << "_book_" << s::to_string(epoch) << ".json";
  s::string book_outfile = book_outss.str();
  s::ofstream book_ofile(book_outfile.c_str());

  s::stringstream stats_outss;
  stats_outss << prefix << "_" << product << "_stats_" << s::to_string(epoch) << ".json";
  s::string stats_outfile = stats_outss.str();
  s::ofstream stats_ofile(stats_outfile.c_str());

  s::stringstream ticker_outss;
  ticker_outss << prefix << "_" << product << "_ticker_" << s::to_string(epoch) << ".json";
  s::string ticker_outfile = ticker_outss.str();
  s::ofstream ticker_ofile(ticker_outfile.c_str());

  s::stringstream trades_outss;
  trades_outss << prefix << "_" << product << "_trades_" << s::to_string(epoch) << ".json";
  s::string trades_outfile = trades_outss.str();
  s::ofstream trades_ofile(trades_outfile.c_str());

  s::stringstream book_urlss;
  book_urlss << COINBASE_URL << product << "/book?level=" << s::to_string(level);
  s::string book_url = book_urlss.str();

  s::stringstream stats_urlss;
  stats_urlss << COINBASE_URL << product << "/stats";
  s::string stats_url = stats_urlss.str();

  s::stringstream ticker_urlss;
  ticker_urlss << COINBASE_URL << product << "/ticker";
  s::string ticker_url = ticker_urlss.str();

  s::stringstream trades_urlss;
  trades_urlss << COINBASE_URL << product << "/trades";
  s::string trades_url = trades_urlss.str();


  bool book_is_first = true;
  bool stats_is_first = true;
  bool ticker_is_first = true;
  bool trades_is_first = true;

  book_ofile << "[";
  stats_ofile << "[";
  ticker_ofile << "[";
  trades_ofile << "[";

  for (int i = 0; i < total; ++i){
    try {
      CURL* hnd = curl_easy_init();

      if (not hnd){
        BOOST_LOG_SEV(logger(), sev::error) << __FUNCTION__ << " CURL handle cannot be allocated. product: " << product << " prefix: " << prefix;
        return;
      }

      if (book_is_first) book_is_first = false;
      else               book_ofile << ",";
      extraction(book_url, hnd, book_ofile);
      usleep(15000000);
      if (stats_is_first) stats_is_first = false;
      else                stats_ofile << ",";
      extraction(stats_url, hnd, stats_ofile);
      usleep(15000000);
      if (ticker_is_first) ticker_is_first = false;
      else                 ticker_ofile << ",";
      extraction(ticker_url, hnd, ticker_ofile);
      usleep(15000000);
      if (trades_is_first) trades_is_first = false;
      else                 trades_ofile << ",";
      extraction(trades_url, hnd, trades_ofile);

      curl_easy_cleanup(hnd);

      usleep(interval - 45000000);
    } catch (s::exception& e){
      BOOST_LOG_SEV(logger(), sev::error) << __FUNCTION__ << " exception has been caught for " << product << ". msg: " << e.what();
      usleep(interval);
    }
  }

  book_ofile << "]";
  stats_ofile << "]";
  ticker_ofile << "]";
  trades_ofile << "]";

  book_ofile.close();
  stats_ofile.close();
  ticker_ofile.close();
  trades_ofile.close();
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
  signal(SIGPIPE, sighandler);
  signal(SIGTERM, sighandler);
  po::options_description desc("Parameters");

  // boost log bug fix
  // https://www.boost.org/doc/libs/1_62_0/libs/log/doc/html/log/rationale/why_crash_on_term.html
  boost::filesystem::path::imbue(s::locale("C"));

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
