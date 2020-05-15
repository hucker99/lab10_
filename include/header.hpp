// Copyright 2018 Your Name <your_email>

#ifndef INCLUDE_HEADER_HPP_
#define INCLUDE_HEADER_HPP_

#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include <algorithm>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
//#include <rocksdb/delete_scheduler.h>
#include <queue>
#include <mutex>
#include <boost/thread/thread.hpp>
#include <boost/log/trivial.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <system_error>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>
#include <picosha2.h>
#include <key_val.h>

static std::queue <element> elements;
static std::mutex m1, m2;
int threadcount;

struct database {
    database() {}

    void print() {
        for (const auto iter : _handles) {
            std::cout << "family " + iter->GetName() << std::endl;
            rocksdb::Iterator *it = _db->NewIterator
                    (rocksdb::ReadOptions(), iter);
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                std::cout << it->key().ToString() << ": "
                << it->value().ToString() << std::endl;
            }
            assert(it->status().ok());
            delete it;
        }
    }

    void open_db() {
        rocksdb::DB::Open(rocksdb::DBOptions(),
                _way, _column_families, &_handles, &_db);
    }

    void my() {
        std::vector <std::thread> threads;
        for (int i = 0; i < threadcount; ++i) {
            threads.emplace_back(&database::fill_bd, this);
        }
        for (auto &it : threads) {
            it.join();
        }
    }

    element read_value(std::string key, std::string column_family_name) {
        std::vector<std::string>::iterator it =
                std::find(_column_families_names.begin(),
                        _column_families_names.end(),
                        column_family_name);
        if (it != _column_families_names.end()) {
            //found
            std::string value;
            _db->Get(rocksdb::ReadOptions(),
                    _handles[std::distance
                    (_column_families_names.begin(), it)], key, &value);
            element tmp(column_family_name, key, value);
            return tmp;
        } else {
            return element();
        }
    }

    void write_value(element el) {
        m2.lock();
        std::vector<std::string>::iterator it =
                std::find(_column_families_names.begin(),
                        _column_families_names.end(),
                        el._family_name);
        if (it != _column_families_names.end()) {
            //found
            _db->Put(rocksdb::WriteOptions(),
                    _handles[std::distance
                    (_column_families_names.begin(), it)],
                    el._key,
                     picosha2::hash256_hex_string(el._key + el._value));
            BOOST_LOG_TRIVIAL(info) << el._key + ":" + el._value;
            m2.unlock();
        } else {
            //not found
            m2.unlock();
        }
    }

    void read_all() {
        for (const auto iter : _handles) {
            rocksdb::Iterator *it =
                    _db->NewIterator(rocksdb::ReadOptions(),
                            iter);
            for (it->SeekToFirst(); it->Valid(); it->Next()) {
                element tmp(iter->GetName(),
                        it->key().ToString(),
                        it->value().ToString());
                elements.push(tmp);
            }
            assert(it->status().ok());
            delete it;
        }
    }

    void fill_bd() {
        bool status = false;
        while (!status) {
            m1.lock();
            if (!elements.empty()) {
                element tmp = elements.front();
                write_value(tmp);
                elements.pop();
                m1.unlock();
            } else {
                status = true;
                m1.unlock();
            }
        }
    }

    void create_db(std::string way, std::vector <std::string> family_names) {
        rocksdb::Options options;
        options.create_if_missing = true;
        rocksdb::DB *db;
        rocksdb::Status s = rocksdb::DB::Open(options, way, &db);
        assert(s.ok());
        rocksdb::ColumnFamilyHandle *h1;
        for (auto &iter : family_names) {
            s = db->CreateColumnFamily
            (rocksdb::ColumnFamilyOptions(), iter, &h1);
            delete h1;
        }
        delete db;
    }

    void parse(std::string way_to_db) {
        _way = way_to_db;
        rocksdb::Status status =
                rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(),
                                                way_to_db,
                                                &_column_families_names);
        assert(status.ok());
        for (auto a : _column_families_names) {
            _column_families.emplace_back(a,
                    rocksdb::ColumnFamilyOptions());
        }
    }

    void close_db() {
        delete _db;
    }

    std::string _way;
    std::vector <std::string> _column_families_names;
    std::vector <rocksdb::ColumnFamilyDescriptor> _column_families;
    std::vector<rocksdb::ColumnFamilyHandle *> _handles;
    rocksdb::DB *_db;
};

typedef database db;
#endif // INCLUDE_HEADER_HPP_
