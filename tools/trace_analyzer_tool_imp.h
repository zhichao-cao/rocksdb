//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/trace_analyzer_tool.h"

#include <list>
#include <map>
#include <utility>
#include <set>

#include "rocksdb/env.h"
#include "util/trace_replay.h"

namespace rocksdb {

class DBImpl;
class WriteBatch;
class AnalyzerOptions;
class TraceAnalyzer;
class TraceOutputWriter;

struct TraceUnit {
  int type;
  std::string key;
  uint64_t value_size;
  uint64_t ts;
  uint64_t uid;
  int cf_id;
  uint64_t key_id;
  uint64_t access_count;
};

class AnalyzerOptions {
 public:
  bool use_get;
  bool use_put;
  bool use_delete;
  bool use_merge;
  bool print_stats;
  uint64_t  output_ignore_count;

  AnalyzerOptions();

  ~AnalyzerOptions();
};

class TraceAnalyzer {
 public:
  TraceAnalyzer(std::string &trace_path, std::string &output_path,
                bool need_output, AnalyzerOptions _analyzer_opts);
  ~TraceAnalyzer();

  Status PrepareProcessing();

  Status StartProcessing();

  Status EndProcessing();

  uint64_t total_requests;
  uint64_t total_keys;
  uint64_t total_get;
  uint64_t total_write_batch;

 private:
  Env *env_;
  unique_ptr<rocksdb::TraceReader> trace_reader_;
  unique_ptr<rocksdb::TraceOutputWriter> trace_output_writer_;
  size_t offset_;
  char *buffer_;
  uint64_t guid_;
  int cf_id_;
  std::string trace_name_;
  std::string output_name_;
  bool need_output_;
  AnalyzerOptions analyzer_opts_;
  std::map<std::string, TraceUnit> trace_map_;
  std::map<uint64_t, uint64_t> count_map_;
  std::map<uint64_t, uint64_t> key_stats_;
  std::map<std::string, int> cf_map_;

  Status TraceMapInsertion(TraceUnit &unit);
  void PrintStatistics();
};

class TraceOutputWriter {
 public:
  TraceOutputWriter(Env *env, std::unique_ptr<WritableFileWriter> &&file_writer)
      : env_(env), file_writer_(std::move(file_writer)) {}
  ~TraceOutputWriter();

  Status WriteHeader();
  Status WriteFooter();
  Status WriteTraceUnit(TraceUnit &unit);
  std::string MicrosdToDate(uint64_t time);
  std::string StringToHex(const std::string &input);

 private:
  Env *env_;
  unique_ptr<WritableFileWriter> file_writer_;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
