//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "tools/trace_analyzer_tool_imp.h"

#include <inttypes.h>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <vector>
#include <time.h>

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
  out_format << unit.type << "\t" << unit.uid << "\t" << unit.access_count << "\t"
    << unit.value_size << "\t" << unit.key.size() << "\t"
    << MicrosdToDate(unit.ts) << "\t" << StringToHex(unit.key) << "\n";
  std::string content(out_format.str());

  s = file_writer_->Append(Slice(content));
  return s;
}

std::string TraceOutputWriter::MicrosdToDate(uint64_t time_in) {
  time_t tx = static_cast<time_t>(time_in/1000000);
  int rest = static_cast<int>(time_in%1000000);
  std::string date_time(ctime(&tx));
  date_time.pop_back();
  date_time += " +: "+std::to_string(rest);
  return date_time;
}

std::string TraceOutputWriter::StringToHex(const std::string& input)
{
    static const char* const lut = "0123456789ABCDEF";
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



AnalyzerOptions::AnalyzerOptions(bool _use_get, bool _use_put, bool _use_delete,
                                 bool _use_merge) {
  use_get = _use_get;
  use_put = _use_put;
  use_delete = _use_delete;
  use_merge = _use_merge;
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
  trace_file_reader.reset(new rocksdb::RandomAccessFileReader(
      std::move(trace_file), trace_name_));
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
  output_file_writer.reset(new WritableFileWriter(std::move(output_file), env_options));
  trace_output_writer_.reset(new TraceOutputWriter(env_, std::move(output_file_writer)));

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
      unit.access_count = 0;
      s = TraceMapInsertion(unit);
      std::cout<<trace_reader_->get_offset() << "\n";
      if(!s.ok()) {
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
  if (need_output_) {
    std::cout << "total reqeusts: " << total_requests
              << " total get: " << total_get
              << " total write batch: " << total_write_batch <<" offset: "<<trace_reader_->get_offset()<< "\n";
    for(auto it = trace_map_.begin(); it != trace_map_.end(); it++) {
      trace_output_writer_->WriteTraceUnit(it->second);
    }
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

  AnalyzerOptions analyzer_opts(true, false, false, false);

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
    } else if (strncmp(argv[i], "--use_get", 0) == 0) {
      analyzer_opts.use_get = true;
    } else if (strncmp(argv[i], "--use_put", 0) == 0) {
      analyzer_opts.use_put = true;
    } else if (strncmp(argv[i], "--use_delete", 0) == 0) {
      analyzer_opts.use_delete = true;
    } else if (strncmp(argv[i], "--use_merge", 0) == 0) {
      analyzer_opts.use_merge = true;
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
