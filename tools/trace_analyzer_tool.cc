//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "tools/trace_analyzer_tool_imp.h"

#include <inttypes.h>
#include <time.h>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "db/db_impl.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "options/cf_options.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/write_batch.h"
#include "table/meta_blocks.h"
#include "table/plain_table_factory.h"
#include "table/table_reader.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/file_reader_writer.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/trace_replay.h"

namespace rocksdb {

/*
// write bach handler to be used for WriteBache iterator
// when processing the write trace
class TraceWriteHandler : public WriteBatch::Handler {
 private:
  TraceAnalyzer * ta_ptr;
  std::string tmp_use;
  TraceWriteHandler() { ta_ptr = nullptr; }
 public:
  TraceWriteHandler(TraceAnalyzer * _ta_ptr) { ta_ptr = _ta_ptr; }
  ~TraceWriteHandler() {}

  virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {
    if (ta_ptr->write_map_.find(column_family_id) == ta_ptr->write_map_.end()) {
      TraceStats write_stats;
      write_stats.cf_id = column_family_id;
      ta_ptr->write_map[column_family_id] = write_stats;
    }

    StatsUnit stats_unit;
    stats_unit.key_id = 0;
    stats_unit.cf_id = write_stats.cf_id;
    stats_unit.value_size = value.size();




    return Status::OK();
  }
    virtual Status DeleteCF(uint32_t column_family_id,
                            const Slice& key) override {
      return Status::OK();
    }
    virtual Status SingleDeleteCF(uint32_t column_family_id,
                                  const Slice& key) override {
    }
    virtual Status DeleteRangeCF(uint32_t column_family_id,
                                 const Slice& begin_key,
                                 const Slice& end_key) override {
      return Status::OK();
    }
    virtual Status MergeCF(uint32_t column_family_id, const Slice& key,
                           const Slice& value) override {
      return Status::OK();
    }
    virtual void LogData(const Slice& blob) override {
      tmp_use = blob.ToString();
    }
    virtual Status MarkBeginPrepare() override {
      return Status::OK();
    }
    virtual Status MarkEndPrepare(const Slice& xid) override {
      tmp_use = xid.ToString();
      return Status::OK();
    }
    virtual Status MarkCommit(const Slice& xid) override {
      tmp_use = xid.ToString();
      return Status::OK();
    }
    virtual Status MarkRollback(const Slice& xid) override {
      tmp_use = xid.ToString();
      return Status::OK();
    }
};


TraceOutputWriter::~TraceOutputWriter() { file_writer_.reset(); }

Status TraceOutputWriter::WriteHeader() { return Status::OK(); }

Status TraceOutputWriter::WriteFooter() { return Status::OK(); }

Status TraceOutputWriter::WriteTraceUnit(TraceUnit &unit) {
  Status s;
  std::ostringstream out_format;
  out_format << unit.type << "\t" << unit.cf_id
             << "\t" << unit.value_size << "\t" << unit.key.size() << "\t"
             << MicrosdToDate(unit.ts) << "\t" << StringToHex(unit.key) << "\n";
  std::string content(out_format.str());
  std::cout << content;

  s = file_writer_->Append(Slice(content));
  return s;
}
*/

std::string TraceAnalyzer::MicrosdToDate(uint64_t time_in) {
  time_t tx = static_cast<time_t>(time_in / 1000000);
  int rest = static_cast<int>(time_in % 1000000);
  std::string date_time(ctime(&tx));
  date_time.pop_back();
  date_time += " +: " + std::to_string(rest);
  return date_time;
}

std::string TraceAnalyzer::StringToHex(const std::string &input) {
  static const char *const lut = "0123456789ABCDEF";
  size_t len = input.length();

  std::string output;
  output.reserve(2 * len);
  for (size_t i = 0; i < len; ++i) {
    const unsigned char c = input[i];
    output.push_back(lut[c >> 4]);
    output.push_back(lut[c & 15]);
  }
  return output;
}

AnalyzerOptions::AnalyzerOptions() {
  output_key_stats = false;
  output_access_count_stats = false;
  output_trace_unit = false;
  output_time_serial = false;
  use_get = true;
  use_put = false;
  use_delete = false;
  use_merge = false;
  print_overall_stats = false;
  print_key_distribution = false;
  print_value_distribution = false;
  print_top_k_access = false;
  output_ignore_count = 0;
  start_time = 0;
  value_interval = 128;
  top_k = 0;
  output_prefix = "/trace_output";
}

AnalyzerOptions::~AnalyzerOptions() {}

TraceAnalyzer::TraceAnalyzer(std::string &trace_path, std::string &output_path,
                             AnalyzerOptions _analyzer_opts)
    : trace_name_(trace_path),
      output_path_(output_path),
      analyzer_opts_(_analyzer_opts) {
  offset_ = 0;
  buffer_ = new char[1024];
  guid_ = 0;
  total_requests = 0;
  total_keys = 0;
  total_get = 0;
  total_write_batch = 0;
}

TraceAnalyzer::~TraceAnalyzer() {}

Status TraceAnalyzer::PrepareProcessing() {
  rocksdb::EnvOptions env_options;
  rocksdb::Env *env = rocksdb::Env::Default();
  env_ = env;
  Status s;

  unique_ptr<rocksdb::RandomAccessFile> trace_file;
  s = env_->NewRandomAccessFile(trace_name_, &trace_file, env_options);
  if (!s.ok()) {
    return s;
  }
  unique_ptr<rocksdb::RandomAccessFileReader> trace_file_reader;
  trace_file_reader.reset(
      new rocksdb::RandomAccessFileReader(std::move(trace_file), trace_name_));
  trace_reader_.reset(new rocksdb::TraceReader(std::move(trace_file_reader)));

  if (!need_output_) {
    return Status::OK();
  }

  /*
  std::string output_name;
  output_name = output_path_ + "/" +analyzer_opts_.output_prefix
  +"-trace_unit.txt"; unique_ptr<WritableFile> output_file; s =
  env_->NewWritableFile(output_name, &output_file, env_options); if (!s.ok()) {
    return s;
  }
  unique_ptr<WritableFileWriter> output_file_writer;
  output_file_writer.reset(
      new WritableFileWriter(std::move(output_file), env_options));
  trace_output_writer_.reset(
      new TraceOutputWriter(env_, std::move(output_file_writer)));
  */

  return Status::OK();
}

// process the trace and generate the analysis
Status TraceAnalyzer::StartProcessing() {
  Status s;
  Trace header;
  s = trace_reader_->ReadHeader(header);
  if (!s.ok()) {
    return s;
  }

  Trace footer;
  s = trace_reader_->ReadFooter(footer);
  if (!s.ok()) {
    return s;
  }

  Trace trace;
  while (s.ok()) {
    trace.reset();
    s = trace_reader_->ReadRecord(trace);
    if (!s.ok()) {
      break;
    }
    TraceUnit unit;
    total_requests++;
    if (trace.type == kTraceWrite) {
      total_write_batch++;
      unit.type = 0;
    } else if (trace.type == kTraceGet) {
      total_get++;
      unit.type = 1;
      unit.key = trace.payload;
      unit.value_size = 0;
      unit.ts = trace.ts;
      unit.cf_id = trace.cf_id;
      if (get_map_.find(trace.cf_name) == get_map_.end()) {
        TraceStats get_stats;
        get_stats.cf_id = trace.cf_id;
        get_stats.cf_name = trace.cf_name;
        get_stats.trace_unit_file = nullptr;
        get_stats.get_count = 1;
        get_stats.total_count = 1;
        s = TraceStatsInsertionGet(unit, get_stats);
        if (!s.ok()) {
          fprintf(stderr, "Cannot insert the trace unit to the map\n");
          return s;
        }
        get_map_[trace.cf_name] = get_stats;
      } else {
        s = TraceStatsInsertionGet(unit, get_map_[trace.cf_name]);
        get_map_[trace.cf_name].get_count++;
        get_map_[trace.cf_name].total_count++;
        if (!s.ok()) {
          fprintf(stderr, "Cannot insert the trace unit to the map\n");
          return s;
        }
      }

      if (analyzer_opts_.output_trace_unit) {
        if (get_map_[trace.cf_name].trace_unit_file == nullptr) {
          std::string trace_file_name =
              output_path_ + "/" + analyzer_opts_.output_prefix + "-" +
              get_map_[trace.cf_name].cf_name + "-trace_unit.txt";
          get_map_[trace.cf_name].trace_unit_file =
              fopen(trace_file_name.c_str(), "w");
        }
        s = TraceUnitWriter(get_map_[trace.cf_name].trace_unit_file, unit);
        if (!s.ok()) {
          fprintf(stderr, "Cannot write the trace unit to the file\n");
          return s;
        }
      }
    }
  }
  if (s.IsIncomplete()) {
    // Fix it: Reaching eof returns Incomplete status at the moment.
    //
    return Status::OK();
  }

  return s;
}

Status TraceAnalyzer::MakeStatistics() {
  for (auto i = get_map_.begin(); i != get_map_.end(); i++) {
    if (i->second.trace_unit_file != nullptr) {
      fclose(i->second.trace_unit_file);
    }

    uint64_t keyid = 0;
    for (auto it = i->second.key_stats.begin(); it != i->second.key_stats.end();
         it++) {
      it->second.key_id = keyid;
      keyid++;

      if (it->second.access_count <= analyzer_opts_.output_ignore_count) {
        continue;
      }

      if (analyzer_opts_.output_access_count_stats) {
        if (i->second.access_count_stats.find(it->second.access_count) ==
            i->second.access_count_stats.end()) {
          i->second.access_count_stats[it->second.access_count] = 1;
        } else {
          i->second.access_count_stats[it->second.access_count]++;
        }
      }

      if (analyzer_opts_.print_key_distribution) {
        if (i->second.key_size_stats.find(it->first.size()) ==
            i->second.key_size_stats.end()) {
          i->second.key_size_stats[it->first.size()] = 1;
        } else {
          i->second.key_size_stats[it->first.size()]++;
        }
      }
    }

    if (analyzer_opts_.output_key_stats) {
      std::string key_stats_path = output_path_ + "/" +
                                   analyzer_opts_.output_prefix + "-" +
                                   i->second.cf_name + "-key_access_stats.txt";
      std::ofstream key_stats_file(key_stats_path, std::ofstream::out);
      if (!key_stats_file.is_open()) {
        fprintf(stderr, "Cannot open the key access stats output file\n");
        exit(1);
      }
      for (auto it = i->second.key_stats.begin();
           it != i->second.key_stats.end(); it++) {
        key_stats_file << it->second.key_id << " " << it->second.cf_id << " "
                       << it->second.value_size << " "
                       << it->second.access_count << "\n";
      }
      key_stats_file.close();
    }

    if (analyzer_opts_.output_access_count_stats) {
      std::string access_count_path =
          output_path_ + "/" + analyzer_opts_.output_prefix + "-" +
          i->second.cf_name + "-access_count_stats.txt";
      std::ofstream access_count_file(access_count_path, std::ofstream::out);
      if (!access_count_file.is_open()) {
        fprintf(stderr, "Cannot open the access count stats output file\n");
        exit(1);
      }
      for (auto it = i->second.access_count_stats.begin();
           it != i->second.access_count_stats.end(); it++) {
        access_count_file << "access_count: " << it->first
                          << " nums: " << it->second << "\n";
      }
      access_count_file.close();
    }
  }

  return Status::OK();
}

Status TraceAnalyzer::ReProcessing() { return Status::OK(); }

// End the processing, print the requested results
Status TraceAnalyzer::EndProcessing() {
  PrintGetStatistics();
  return Status::OK();
}

// add the trace access count to the map
Status TraceAnalyzer::TraceStatsInsertionGet(TraceUnit &unit,
                                             TraceStats &stats) {
  StatsUnit stats_unit;
  stats_unit.cf_id = stats.cf_id;
  stats_unit.value_size = unit.value_size;
  stats_unit.key_id = 0;
  auto found = stats.key_stats.find(unit.key);
  if (found == stats.key_stats.end()) {
    stats_unit.access_count = 1;
    stats.key_stats[unit.key] = stats_unit;
  } else {
    found->second.access_count++;
  }
  return Status::OK();
}

void TraceAnalyzer::PrintGetStatistics() {
  uint64_t total_key_num = 0;
  for (auto i = get_map_.begin(); i != get_map_.end(); i++) {
    total_key_num += static_cast<uint64_t>(i->second.key_stats.size());
    std::cout << "*********************************************************\n";
    std::cout << "colume family name: " << i->second.cf_name
              << " cf_id: " << i->second.cf_id << "\n";
    std::cout << "Total keys of this colume family: "
              << i->second.key_stats.size() << "\n";
    printf("Total requests: %" PRIu64 " Total gets: %" PRIu64 "\n",
           i->second.total_count, i->second.get_count);
    if (analyzer_opts_.print_key_distribution) {
      std::cout << "The key size distribution\n";
      for (auto it = i->second.key_size_stats.begin();
           it != i->second.key_size_stats.end(); it++) {
        std::cout << "key size: " << it->first << " nums: " << it->second
                  << "\n";
      }
    }
  }

  if (analyzer_opts_.print_overall_stats) {
    std::cout << "*********************************************************\n";
    std::cout << "total_reqeusts: " << total_requests
              << " total_get: " << total_get
              << " total_write_batch: " << total_write_batch
              << " total_keys: " << total_key_num << "\n";
  }
}

Status TraceAnalyzer::TraceUnitWriter(FILE *file_p, TraceUnit &unit) {
  if (file_p == nullptr) {
    return Status::Corruption("Empty file pointer");
  }
  std::string hex_key = StringToHex(unit.key);
  uint64_t ts = unit.ts;
  fprintf(file_p, "%u %u %zu %" PRIu64 " %s\n", unit.type, unit.cf_id,
          unit.value_size, ts, hex_key.c_str());
  return Status::OK();
}

namespace {

void print_help() {
  fprintf(stderr,
          R"(trace_analyzer --trace_file=<trace file path> [--comman=]
      --trace_file=<trace file path>
        The trace path
      --output_dir=<the output dir>
        The directory to store the output files
      --output_prefix=<the prefix of all output>
        The prefix used for all the output files
      --output_key_stats
        Output the key access count statistics to file
      --output_access_count_stats
        Output the access count distribution statistics to file
      --output_trace_unit
        Output the trace unit to file for further analyze
      --output_time_serial=<trace collect time>
        Output the access time sequence of keys with key space of GET
      --use_get
        Analyze the GET operations
      --use_put
        Analyze the PUT operations
      --use_delete
        Analyze the SingleDELETE operations
      --use_merge
        Analyze the MERGE operations
      --print_overall_stats
        Print the stats of the whole trace, like total requests, keys, and etc.
      --print_key_distribution
        Print the key size distribution
      --print_value_distribution
        Print the value size distribution, only available for write
      --output_ignore_count=
        ignores the access count <= this value to shorter the output
   )");
}
}  // namespace

int TraceAnalyzerTool::Run(int argc, char **argv) {
  std::string trace_path;
  std::string output_path;

  AnalyzerOptions analyzer_opts;

  if (argc <= 1) {
    print_help();
    exit(1);
  }

  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--trace_file=", 13) == 0) {
      trace_path = argv[i] + 13;
    } else if (strncmp(argv[i], "--output_dir=", 13) == 0) {
      output_path = argv[i] + 13;
    } else if (strncmp(argv[i], "--output_prefix=", 16) == 0) {
      analyzer_opts.output_prefix = argv[i] + 16;
    } else if (strncmp(argv[i], "--output_key_stats", 18) == 0) {
      analyzer_opts.output_key_stats = true;
    } else if (strncmp(argv[i], "--output_access_count_stats", 27) == 0) {
      analyzer_opts.output_access_count_stats = true;
    } else if (strncmp(argv[i], "--output_trace_unit", 19) == 0) {
      analyzer_opts.output_trace_unit = true;
    } else if (strncmp(argv[i], "--output_time_serial=", 21) == 0) {
      std::string::size_type sz = 0;
      std::string tmp = argv[i] + 21;
      analyzer_opts.start_time = std::stoull(tmp, &sz, 0);
      analyzer_opts.output_time_serial = true;
    } else if (strncmp(argv[i], "--use_get", 9) == 0) {
      analyzer_opts.use_get = true;
    } else if (strncmp(argv[i], "--use_put", 9) == 0) {
      analyzer_opts.use_put = true;
    } else if (strncmp(argv[i], "--use_delete", 12) == 0) {
      analyzer_opts.use_delete = true;
    } else if (strncmp(argv[i], "--use_merge", 11) == 0) {
      analyzer_opts.use_merge = true;
    } else if (strncmp(argv[i], "--print_overall_stats", 21) == 0) {
      analyzer_opts.print_overall_stats = true;
    } else if (strncmp(argv[i], "--print_key_distribution", 24) == 0) {
      analyzer_opts.print_key_distribution = true;
    } else if (strncmp(argv[i], "--print_top_k_access", 20) == 0) {
      std::string tmp = argv[i] + 20;
      analyzer_opts.top_k = std::stoi(tmp);
      analyzer_opts.print_top_k_access = true;
    } else if (strncmp(argv[i], "--output_ignore_count=", 22) == 0) {
      std::string tmp = argv[i] + 22;
      analyzer_opts.output_ignore_count = std::stoi(tmp);
    } else if (strncmp(argv[i], "--value_interval=", 17) == 0) {
      std::string tmp = argv[i] + 17;
      analyzer_opts.value_interval = std::stoi(tmp);
      analyzer_opts.print_value_distribution = true;
    } else {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      print_help();
      exit(1);
    }
  }

  TraceAnalyzer *analyzer =
      new TraceAnalyzer(trace_path, output_path, analyzer_opts);

  rocksdb::Status s = analyzer->PrepareProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot initiate the trace reader\n");
    exit(1);
  }

  s = analyzer->StartProcessing();
  if (!s.ok()) {
    analyzer->EndProcessing();
    fprintf(stderr, "Cannot processing the trace\n");
    exit(1);
  }

  s = analyzer->MakeStatistics();
  if (!s.ok()) {
    fprintf(stderr, "Cannot make the statistics\n");
    exit(1);
  }

  s = analyzer->EndProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot ouput the result\n");
    exit(1);
  }

  return 0;
}
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
