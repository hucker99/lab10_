// Copyright 2018 Your Name <your_email>

#include <header.hpp>
#include <constants.h>
int main(int argc, char **argv) {
    std::string loglevel, pathin, pathout;
    try {
        boost::program_options::options_description desc("Allowed options");
        desc.add_options()
                ("log-level", boost::program_options::value<std::string>(),
                        "logirovanye")
                ("thread-count", boost::program_options::value<int>(),
                 "potoki")
                ("output", boost::program_options::value<std::string>(),
                 "path out")
                ("input", boost::program_options::value<std::string>(),
                 "path in");
        boost::program_options::variables_map vm;
        store(parse_command_line(argc, argv, desc), vm);
        notify(vm);
        if (vm.count("log-level")) {
            loglevel = vm["log-level"].as<std::string>();
            std::cout << "log-level:" << loglevel << std::endl;
        } else {
            loglevel = "Severity";
            std::cout << "log-level:" << loglevel << std::endl;
        }
        if (vm.count("thread-count")) {
            threadcount = vm["thread-count"].as<int>();
            std::cout << "threads:" << threadcount << std::endl;
        } else {
            threadcount = std::thread::hardware_concurrency();
            std::cout << "threads:" << threadcount << std::endl;
        }
        if (vm.count("output")) {
            pathout = vm["output"].as<std::string>();
            std::cout << "output:" << pathout << std::endl;
        } else {
            pathout = Path2;
            std::cout << "output:" << pathout << std::endl;
        }
        if (vm.count("input")) {
            pathin = vm["input"].as<std::string>();
            std::cout << "input:" << pathin << std::endl;
        } else {
            pathin = Path1;
            std::cout << "input:" << pathin << std::endl;
        }
    }
    catch (...) {
        std::cout << "lazha" << "\n";
    }
    boost::log::register_simple_formatter_factory<boost::log
    ::trivial::severity_level, char>(loglevel);
    boost::log::add_file_log(
            boost::log::keywords::file_name = "log.log",
            boost::log::keywords::rotation_size = 256 * 1024 * 1024,
            boost::log::keywords::time_based_rotation =
                    boost::log::sinks::file
            ::rotation_at_time_point(0, 0, 0),
            boost::log::keywords::filter = boost::log::trivial::severity
                                           >= boost::log::trivial::error,
            boost::log::keywords::format =
                    (
                     boost::log::expressions::stream
                     << boost::posix_time
                     ::second_clock::local_time()
                     << " : <" << boost::log::
                     trivial::severity
                     << "> " << boost::log::expressions::smessage));
    boost::log::add_console_log(
            std::cout,
            boost::log::keywords::format =
                    "[%ThreadID%][%TimeStamp%][%Severity%]: %Message%");
    boost::log::add_common_attributes();
    db bd1;
    bd1.parse(pathin);
    bd1.open_db();
    bd1.read_all();
    db bd2;
    bd2.create_db(pathout, bd1._column_families_names);
    bd2.parse(pathout);
    bd2.open_db();
    bd2.my();
    bd1.print();
    bd1.close_db();
    std::cout << "bd2:" << std::endl;
    bd2.print();
    bd2.close_db();
    BOOST_LOG_TRIVIAL(info) << "done";
    return 0;
}
