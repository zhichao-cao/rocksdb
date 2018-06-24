//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "tools/trace_analyzer_tool_imp.h"

#include <inttypes.h>
#include <time.h>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>
#include <stdexcept>

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
TraceOutputWriter::~TraceOutputWriter() { file_writer_.reset(); }

Status TraceOutputWriter::WriteHeader() { return Status::OK(); }

Status TraceOutputWriter::WriteFooter() { return Status::OK(); }

Status TraceOutputWriter::WriteTraceUnit(TraceUnit &unit) {
  Status s;
  std::ostringstream out_format;
  out_format << unit.type << "\t" << unit.cf_id << "\t" << unit.key_id << "\t"
            << unit.access_count
             << "\t" << unit.value_size << "\t" << unit.key.size() << "\t"
             << MicrosdToDate(unit.ts) << "\t" << StringToHex(unit.key) << "\n";
  std::string content(out_format.str());
  std::cout << content;

  s = file_writer_->Append(Slice(content));
  return s;
}

std::string TraceOutputWriter::MicrosdToDate(uint64_t time_in) {
  time_t tx = static_cast<time_t>(time_in / 1000000);
  int rest = static_cast<int>(time_in % 1000000);
  std::string date_time(ctime(&tx));
  date_time.pop_back();
  date_time += " +: " + std::to_string(rest);
  return date_time;
}

std::string TraceOutputWriter::StringToHex(const std::string &input) {
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
  use_get = true;
  use_put = false;
  use_delete = false;
  use_merge = false;
  print_stats = false;
  output_ignore_count = 0;
}

AnalyzerOptions::~AnalyzerOptions() {}

TraceAnalyzer::TraceAnalyzer(std::string &trace_path, std::string &output_path,
                             bool need_output, AnalyzerOptions _analyzer_opts)
    : trace_name_(trace_path),
      output_name_(output_path),
      need_output_(need_output),
      analyzer_opts_(_analyzer_opts) {
  offset_ = 0;
  buffer_ = new char[1024];
  guid_ = 0;
  cf_id_ = 0;
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

  unique_ptr<WritableFile> output_file;
  s = env_->NewWritableFile(output_name_, &output_file, env_options);
  if (!s.ok()) {
    return s;
  }
  unique_ptr<WritableFileWriter> output_file_writer;
  output_file_writer.reset(
      new WritableFileWriter(std::move(output_file), env_options));
  trace_output_writer_.reset(
      new TraceOutputWriter(env_, std::move(output_file_writer)));

  return Status::OK();
}

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
      unit.ts = trace.ts;
      unit.value_size = 0;
      unit.uid = 0;
      unit.key_id = 0;
      unit.access_count = 0;
      if(cf_map_.find(trace.cf_name) == cf_map_.end()) {
        cf_map_[trace.cf_name] = cf_id_;
        cf_id_++;
        unit.cf_id = cf_map_[trace.cf_name];
      } else {
        unit.cf_id = cf_map_[trace.cf_name];
      }
      s = TraceMapInsertion(unit);
      if (!s.ok()) {
        fprintf(stderr, "Cannot insert the trace unit to the map\n");
      }
    }
  }
  // fprintf(stderr, "Ops Written: %ld\n", ops);

  if (s.IsIncomplete()) {
    // Fix it: Reaching eof returns Incomplete status at the moment.

    return Status::OK();
  }
  return s;
}

Status TraceAnalyzer::EndProcessing() {
  uint64_t keyid = 0;
  for (auto it = trace_map_.begin(); it != trace_map_.end(); it++) {
    it->second.key_id = keyid;
    keyid++;

    if (it->second.access_count <= analyzer_opts_.output_ignore_count) {
      continue;
    }
    if (need_output_) {
        trace_output_writer_->WriteTraceUnit(it->second);
    }
    if (analyzer_opts_.print_stats) {
      // Build the access count map to get distribution
      if (count_map_.find(it->second.access_count) == count_map_.end()) {
        count_map_[it->second.access_count] = 1;
      } else {
        count_map_[it->second.access_count]++;
      }

      // build the key size distribution set
      if (key_stats_.find(it->second.key.size()) == key_stats_.end()) {
        key_stats_[it->second.key.size()] = 1;
      } else {
        key_stats_[it->second.key.size()]++;
      }
    }
  }
  if (analyzer_opts_.print_stats) {
    PrintStatistics();
  }
  return Status::OK();
}

Status TraceAnalyzer::TraceMapInsertion(TraceUnit &unit) {
  auto found = trace_map_.find(unit.key);
  if (found == trace_map_.end()) {
    unit.uid = guid_;
    guid_++;
    unit.access_count = 1;
    trace_map_[unit.key] = unit;
  } else {
    found->second.access_count++;
    unit.uid = found->second.uid;
    unit.access_count = found->second.access_count;
  }
  return Status::OK();
}


void TraceAnalyzer::PrintStatistics() {
  std::cout << "total_reqeusts: " << total_requests
            << " total_get: " << total_get
            << " total_write_batch: " << total_write_batch
            << " offset: " << trace_reader_->get_offset()
            << " total_keys: "<< trace_map_.size() <<"\n";
  std::cout <<"colume family name : cf_id\n";
  for(auto it = cf_map_.begin(); it !=cf_map_.end(); it++) {
    std::cout << it->first << " : " << it->second <<"\n";
  }

  std::cout << "\nThe access count distribution\n";

  for(auto it = count_map_.begin(); it != count_map_.end(); it++) {
    std::cout << "access: " << it->first << " nums: " << it->second <<"\n";
  }

  std::cout << "\n The key sizes: \n";
  for(auto it = key_stats_.begin(); it != key_stats_.end(); it++) {
    std::cout << "key_size: " << it->first << " nums: " << it->second <<"\n";
  }

}




namespace {

void print_help() {
  fprintf(stderr,
          R"(trace_analyzer --trace_file=<trace file path> [--comman=]
      --trace_file=<trace file path>
        The trace path
      --output_file=<the output file path>
        Create readble file of the trace
      --use_get
        Analyze the GET operations
      --use_put
        Analyze the PUT operations
      --use_delete
        Analyze the SingleDELETE operations
      --use_merge
        Analyze the MERGE operations
   )");
}
}  // namespace

int TraceAnalyzerTool::Run(int argc, char **argv) {
  std::string trace_path;
  std::string output_path;
  bool need_output = false;

  AnalyzerOptions analyzer_opts;

  if (argc <= 1) {
    print_help();
    exit(1);
  }

  for (int i = 1; i < argc; i++) {
    if (strncmp(argv[i], "--trace_file=", 13) == 0) {
      trace_path = argv[i] + 13;
    } else if (strncmp(argv[i], "--output_file=", 14) == 0) {
      output_path = argv[i] + 14;
      need_output = true;
    } else if (strncmp(argv[i], "--use_get", 9) == 0) {
      analyzer_opts.use_get = true;
    } else if (strncmp(argv[i], "--use_put", 9) == 0) {
      analyzer_opts.use_put = true;
    } else if (strncmp(argv[i], "--use_delete", 12) == 0) {
      analyzer_opts.use_delete = true;
    } else if (strncmp(argv[i], "--use_merge", 11) == 0) {
      analyzer_opts.use_merge = true;
    } else if (strncmp(argv[i], "--print_stats", 13) == 0) {
      analyzer_opts.print_stats = true;
     } else if (strncmp(argv[i], "--output_ignore_count=", 22) == 0) {
       std::string tmp = argv[i] + 22;
       analyzer_opts.output_ignore_count = std::stoi(tmp);
    } else {
      fprintf(stderr, "Unrecognized argument '%s'\n\n", argv[i]);
      print_help();
      exit(1);
    }
  }

  TraceAnalyzer *analyzer =
      new TraceAnalyzer(trace_path, output_path, need_output, analyzer_opts);

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

  s = analyzer->EndProcessing();
  if (!s.ok()) {
    fprintf(stderr, "Cannot ouput the result\n");
    exit(1);
  }

  return 0;
}
}  // namespace rocksdb

#endif  // ROCKSDB_LITE
