//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <memory>
#include <utility>

#include "rocksdb/env.h"

namespace rocksdb {

class DBImpl;
class RandomAccessFileReader;
class Slice;
class TraceReader;
class TraceWriter;
class WriteBatch;
class WritableFileWriter;

const std::string kTraceMagic = "feedcafedeadbeef";

enum TraceType : char {
  kTraceBegin = 1,
  kTraceEnd = 2,
  kTraceWrite = 3,
  kTraceGet = 4,
  kTraceMax,
};

struct Trace {
  uint64_t ts;
  TraceType type;
  //uint32_t cf_id;
  std::string cf_name;
  std::string payload;

  void reset() {
    ts = 0;
    type = kTraceMax;
    cf_name.clear();
    payload.clear();
  }
};

class Tracer {
 public:
  Tracer(Env* env, std::unique_ptr<TraceWriter>&& trace_writer);
  ~Tracer();

  Status TraceWrite(WriteBatch* write_batch, const std::string& cf_name,
                    const uint32_t& cf_id);
  Status TraceGet(const Slice& key, const std::string& cf_name,
                  const uint32_t& cf_id);

  Status Close();

 private:
  Env* env_;
  unique_ptr<TraceWriter> trace_writer_;
};

class Replayer {
 public:
  Replayer(DBImpl* db, std::unique_ptr<TraceReader>&& reader);
  ~Replayer();

 private:
  Status Replay();

  DBImpl* db_;
  unique_ptr<TraceReader> trace_reader_;
};

class TraceReader {
 public:
  TraceReader(std::unique_ptr<RandomAccessFileReader>&& reader);
  ~TraceReader();

  Status ReadHeader(Trace& header);
  Status ReadFooter(Trace& footer);
  Status ReadRecord(Trace& trace);
  size_t get_offset() { return offset_; }

 private:
  unique_ptr<RandomAccessFileReader> file_reader_;
  Slice result_;
  size_t offset_;
  char* const buffer_;

  static const unsigned int kBufferSize;
};

class TraceWriter {
 public:
  TraceWriter(Env* env, std::unique_ptr<WritableFileWriter>&& file_writer)
      : env_(env), file_writer_(std::move(file_writer)) {}
  ~TraceWriter();

  Status WriteHeader();
  Status WriteFooter();
  Status WriteRecord(Trace& trace);

 private:
  Env* env_;
  unique_ptr<WritableFileWriter> file_writer_;

  const unsigned int kMetadataSize = 13;
};

}  // namespace rocksdb
