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
  uint32_t type;
  std::string key;
  uint64_t value_size;
  uint64_t ts;
  uint32_t cf_id;
};

struct StatsUnit {
  uint64_t key_id;
  uint32_t cf_id;
  uint64_t value_size;
  uint64_t access_count;
};

/*
struct TraceWriteHandler : public WriteBatch::Handler {
  TraceAnalyzer * ta_ptr;
  TraceWriteHandler() { ta_ptr = nullptr; }
  TraceWriteHandler(TraceAnalyzer * _ta_ptr) { ta_ptr = _ta_ptr; }
  ~TraceWriteHandler() {}

  virtual Status PutCF(uint32_t column_family_id, const Slice& key,
                         const Slice& value) override {

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
    }
    virtual Status MarkBeginPrepare() override {
      return Status::OK();
    }
    virtual Status MarkEndPrepare(const Slice& xid) override {
      return Status::OK();
    }
    virtual Status MarkNoop(bool empty_batch) override {
      return Status::OK();
    }
    virtual Status MarkCommit(const Slice& xid) override {
      return Status::OK();
    }
    virtual Status MarkRollback(const Slice& xid) override {
      return Status::OK();
    }
};
*/

class AnalyzerOptions {
 public:
  bool output_key_stats;
  bool output_access_count_stats;
  bool output_trace_unit;
  bool use_get;
  bool use_put;
  bool use_delete;
  bool use_merge;
  bool print_overall_stats;
  bool print_key_distribution;
  bool print_value_distribution;
  uint64_t  output_ignore_count;
  int  value_interval;
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
};


class TraceAnalyzer {
 public:
  TraceAnalyzer(std::string &trace_path, std::string &output_path,
                AnalyzerOptions _analyzer_opts);
  ~TraceAnalyzer();

  Status PrepareProcessing();

  Status StartProcessing();

  Status MakeStatistics();

  Status WriteStatistics();

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
  std::string output_path_;
  bool need_output_;
  AnalyzerOptions analyzer_opts_;
  std::map<std::string, TraceStats> get_map_;
  std::map<uint32_t, TraceStats> write_map_;

  Status TraceStatsInsertionGet(TraceUnit &unit, TraceStats& stats);
  Status TraceStatsInsertionWrite(TraceUnit &unit, TraceStats& stats);

  void PrintGetStatistics();
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
