//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once
#ifndef ROCKSDB_LITE

#include "rocksdb/trace_analyzer_tool.h"

#include <stdio.h>
#include <fstream>
#include <list>
#include <map>
#include <set>
#include <utility>

#include "rocksdb/env.h"
#include "util/trace_replay.h"

namespace rocksdb {

class DBImpl;
class WriteBatch;
class AnalyzerOptions;
class TraceAnalyzer;

struct TraceUnit {
  uint32_t type;
  std::string key;
  size_t value_size;
  uint64_t ts;
  uint32_t cf_id;
};

struct StatsUnit {
  uint64_t key_id;
  uint32_t cf_id;
  size_t value_size;
  uint64_t access_count;
};

class AnalyzerOptions {
 public:
  bool output_key_stats;
  bool output_access_count_stats;
  bool output_trace_unit;
  bool output_time_serial;
  bool use_get;
  bool use_put;
  bool use_delete;
  bool use_merge;
  bool print_overall_stats;
  bool print_key_distribution;
  bool print_value_distribution;
  bool print_top_k_access;
  uint64_t  output_ignore_count;
  uint64_t start_time;
  int  value_interval;
  int top_k;
  std::string output_prefix;

  AnalyzerOptions();

  ~AnalyzerOptions();
};


struct TraceStats {
  uint32_t cf_id;
  std::string cf_name;
  std::map<std::string, StatsUnit> key_stats;
  std::map<uint64_t, uint64_t> access_count_stats;
  std::map<uint64_t, uint64_t> key_size_stats;
  std::map<uint64_t, uint64_t> value_size_stats;
  FILE *trace_unit_file;
};


class TraceAnalyzer {
 public:
  TraceAnalyzer(std::string &trace_path, std::string &output_path,
                AnalyzerOptions _analyzer_opts);
  ~TraceAnalyzer();

  Status PrepareProcessing();

  Status StartProcessing();

  Status MakeStatistics();

  Status ReProcessing();

  Status EndProcessing();

  Status WriteTraceUnit(TraceUnit &unit);

  uint64_t total_requests;
  uint64_t total_keys;
  uint64_t total_get;
  uint64_t total_write_batch;

 private:
  Env *env_;
  unique_ptr<rocksdb::TraceReader> trace_reader_;
  size_t offset_;
  char *buffer_;
  uint64_t guid_;
  int cf_id_;
  std::string trace_name_;
  std::string output_path_;
  bool need_output_;
  AnalyzerOptions analyzer_opts_;
  std::map<std::string, TraceStats> get_map_;
  std::map<uint32_t, TraceStats> write_map_;

  Status TraceStatsInsertionGet(TraceUnit &unit, TraceStats& stats);
  Status TraceStatsInsertionWrite(TraceUnit &unit, TraceStats& stats);

  void PrintGetStatistics();
  Status TraceUnitWriter(FILE *file_p, TraceUnit &unit);
  std::string MicrosdToDate(uint64_t time);
  std::string StringToHex(const std::string &input);
};

/*
class TraceOutputWriter {
 public:
  TraceOutputWriter(Env *env, std::unique_ptr<WritableFileWriter> &&file_writer)
      : env_(env), file_writer_(std::move(file_writer)) {}
  ~TraceOutputWriter();

  Status WriteHeader();
  Status WriteFooter();
  Status WriteTraceUnit(TraceUnit &unit);

 private:
  Env *env_;
  unique_ptr<WritableFileWriter> file_writer_;
};
*/

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
