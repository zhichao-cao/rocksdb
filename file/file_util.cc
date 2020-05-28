//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "file/file_util.h"

#include <string>
#include <algorithm>

#include "file/random_access_file_reader.h"
#include "file/sequence_file_reader.h"
#include "file/sst_file_manager_impl.h"
#include "file/writable_file_writer.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

// Utility function to copy a file up to a specified length
Status CopyFile(FileSystem* fs, const std::string& source,
                const std::string& destination, uint64_t size, bool use_fsync) {
  const FileOptions soptions;
  Status s;
  std::unique_ptr<SequentialFileReader> src_reader;
  std::unique_ptr<WritableFileWriter> dest_writer;

  {
    std::unique_ptr<FSSequentialFile> srcfile;
    s = fs->NewSequentialFile(source, soptions, &srcfile, nullptr);
    if (!s.ok()) {
      return s;
    }
    std::unique_ptr<FSWritableFile> destfile;
    s = fs->NewWritableFile(destination, soptions, &destfile, nullptr);
    if (!s.ok()) {
      return s;
    }

    if (size == 0) {
      // default argument means copy everything
      s = fs->GetFileSize(source, IOOptions(), &size, nullptr);
      if (!s.ok()) {
        return s;
      }
    }
    src_reader.reset(new SequentialFileReader(std::move(srcfile), source));
    dest_writer.reset(
        new WritableFileWriter(std::move(destfile), destination, soptions));
  }

  char buffer[4096];
  Slice slice;
  while (size > 0) {
    size_t bytes_to_read = std::min(sizeof(buffer), static_cast<size_t>(size));
    s = src_reader->Read(bytes_to_read, &slice, buffer);
    if (!s.ok()) {
      return s;
    }
    if (slice.size() == 0) {
      return Status::Corruption("file too small");
    }
    s = dest_writer->Append(slice);
    if (!s.ok()) {
      return s;
    }
    size -= slice.size();
  }
  return dest_writer->Sync(use_fsync);
}

// Utility function to create a file with the provided contents
Status CreateFile(FileSystem* fs, const std::string& destination,
                  const std::string& contents, bool use_fsync) {
  const EnvOptions soptions;
  Status s;
  std::unique_ptr<WritableFileWriter> dest_writer;

  std::unique_ptr<FSWritableFile> destfile;
  s = fs->NewWritableFile(destination, soptions, &destfile, nullptr);
  if (!s.ok()) {
    return s;
  }
  dest_writer.reset(
      new WritableFileWriter(std::move(destfile), destination, soptions));
  s = dest_writer->Append(Slice(contents));
  if (!s.ok()) {
    return s;
  }
  return dest_writer->Sync(use_fsync);
}

Status DeleteDBFile(const ImmutableDBOptions* db_options,
                    const std::string& fname, const std::string& dir_to_sync,
                    const bool force_bg, const bool force_fg) {
#ifndef ROCKSDB_LITE
  SstFileManagerImpl* sfm =
      static_cast<SstFileManagerImpl*>(db_options->sst_file_manager.get());
  if (sfm && !force_fg) {
    return sfm->ScheduleFileDeletion(fname, dir_to_sync, force_bg);
  } else {
    return db_options->env->DeleteFile(fname);
  }
#else
  (void)dir_to_sync;
  (void)force_bg;
  (void)force_fg;
  // SstFileManager is not supported in ROCKSDB_LITE
  // Delete file immediately
  return db_options->env->DeleteFile(fname);
#endif
}

bool IsWalDirSameAsDBPath(const ImmutableDBOptions* db_options) {
  bool same = false;
  assert(!db_options->db_paths.empty());
  Status s = db_options->env->AreFilesSame(db_options->wal_dir,
                                           db_options->db_paths[0].path, &same);
  if (s.IsNotSupported()) {
    same = db_options->wal_dir == db_options->db_paths[0].path;
  }
  return same;
}

IOStatus GenerateOneFileChecksum(FileSystem* fs, const std::string& file_path,
                                 FileChecksumGenFactory* checksum_factory,
                                 std::string* checksum) {
  if (checksum_factory == nullptr) {
    return IOStatus::InvalidArgument("Checksum factory is invalid");
  }
  FileChecksumGenContext gen_context;
  std::unique_ptr<FileChecksumGenerator> checksum_generator =
      checksum_factory->CreateFileChecksumGenerator(gen_context);
  const FileOptions soptions;
  size_t size;
  IOStatus io_s;
  std::unique_ptr<SequentialFileReader> reader;
  {
    std::unique_ptr<FSSequentialFile> r_file;
    io_s = fs->NewSequentialFile(file_path, soptions, &r_file, nullptr);
    if (!io_s.ok()) {
      return io_s;
    }
    io_s = fs->GetFileSize(file_path, IOOptions(), &size, nullptr);
    if (!io_s.ok()) {
      return io_s;
    }
    reader.reset(new SequentialFileReader(std::move(r_file), file_path));
  }

  char buffer[4096];
  Slice slice;
  while (size > 0) {
    size_t bytes_to_read = std::min(sizeof(buffer), size);
    io_s = status_to_io_status(reader->Read(bytes_to_read, &slice, buffer));
    if (!io_s.ok()) {
      return io_s;
    }
    if (slice.size() == 0) {
      return IOStatus::Corruption("file too small");
    }
    checksum_generator->Update(buffer, slice.size());
    size -= slice.size();
  }
  checksum_generator->Finalize();
  *checksum = checksum_generator->GetChecksum();
  return IOStatus::OK();
}

}  // namespace ROCKSDB_NAMESPACE
