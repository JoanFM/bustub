//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // TODO(Joan): No need to Protect the pin count increment, because it is atomic, but should we protect the rest?
  std::ostringstream oss;
  oss << std::this_thread::get_id();
  std::string id_str = oss.str();
  shared_lock_ = std::shared_lock<std::shared_mutex>(frame_->rwlatch_);
  std::lock_guard<std::mutex> lock(*bpm_latch_);

  replacer_->SetEvictable(frame_->frame_id_, false);
  replacer_->RecordAccess(frame_->frame_id_);
  frame_->pin_count_.fetch_add(1);
  std::promise<bool> callback;
  auto future = callback.get_future();
  DiskRequest request{/*is_write=*/false, frame_->GetDataMut(), /*page_id=*/page_id_, std::move(callback)};
  disk_scheduler_->Schedule(std::move(request));
  future.get();
  is_valid_ = true;
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  if (this != &that) {
    // Moving shared_ptr already invalidates the moved ones
    this->page_id_ = std::move(that.page_id_);
    this->frame_ = std::move(that.frame_);
    this->replacer_ = std::move(that.replacer_);
    this->bpm_latch_ = std::move(that.bpm_latch_);
    this->disk_scheduler_ = std::move(that.disk_scheduler_);
    this->shared_lock_ = std::move(that.shared_lock_);
    this->is_valid_ = that.is_valid_;
    that.is_valid_ = false;
  }
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    // Moving shared_ptr already invalidates the moved ones
    Drop();
    this->page_id_ = std::move(that.page_id_);
    this->frame_ = std::move(that.frame_);
    this->replacer_ = std::move(that.replacer_);
    this->bpm_latch_ = std::move(that.bpm_latch_);
    this->disk_scheduler_ = std::move(that.disk_scheduler_);
    this->shared_lock_ = std::move(that.shared_lock_);
    this->is_valid_ = that.is_valid_;
    that.is_valid_ = false;
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Flush() {
  std::promise<bool> callback;
  auto future = callback.get_future();
  DiskRequest request{/*is_write=*/true, frame_->GetDataMut(), /*page_id=*/page_id_, std::move(callback)};
  disk_scheduler_->Schedule(std::move(request));
  future.get();
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() {
  if (is_valid_) {
    {
      std::lock_guard<std::mutex> lock(*bpm_latch_);
      frame_->pin_count_.fetch_sub(1);
      is_valid_ = false;
      if (frame_->pin_count_.load() == 0) {
        replacer_->SetEvictable(frame_->frame_id_, true);
      }
    }
    shared_lock_.unlock();
  }
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  unique_lock_ = std::unique_lock<std::shared_mutex>(frame_->rwlatch_);
  std::lock_guard<std::mutex> lock(*bpm_latch_);
  frame_->Reset();
  replacer_->SetEvictable(frame_->frame_id_, false);
  replacer_->RecordAccess(frame_->frame_id_);
  frame_->pin_count_.fetch_add(1);
  frame_->is_dirty_ = true;
  std::promise<bool> callback;
  auto future = callback.get_future();
  DiskRequest request{/*is_write=*/false, frame_->GetDataMut(), /*page_id=*/page_id_, std::move(callback)};
  disk_scheduler_->Schedule(std::move(request));
  future.get();
  is_valid_ = true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  if (this != &that) {
    // Moving shared_ptr already invalidates the moved ones
    this->page_id_ = std::move(that.page_id_);
    this->frame_ = std::move(that.frame_);
    this->replacer_ = std::move(that.replacer_);
    this->bpm_latch_ = std::move(that.bpm_latch_);
    this->disk_scheduler_ = std::move(that.disk_scheduler_);
    this->unique_lock_ = std::move(that.unique_lock_);
    this->is_valid_ = that.is_valid_;
    that.is_valid_ = false;
  }
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    // Moving shared_ptr already invalidates the moved ones
    Drop();
    this->page_id_ = std::move(that.page_id_);
    this->frame_ = std::move(that.frame_);
    this->replacer_ = std::move(that.replacer_);
    this->bpm_latch_ = std::move(that.bpm_latch_);
    this->disk_scheduler_ = std::move(that.disk_scheduler_);
    this->unique_lock_ = std::move(that.unique_lock_);
    this->is_valid_ = that.is_valid_;
    that.is_valid_ = false;
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Flush() {
  std::promise<bool> callback;
  auto future = callback.get_future();
  DiskRequest request{/*is_write=*/true, frame_->GetDataMut(), /*page_id=*/page_id_, std::move(callback)};
  disk_scheduler_->Schedule(std::move(request));
  future.get();
  frame_->is_dirty_ = false;
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() {
  if (is_valid_) {
    {
      std::lock_guard<std::mutex> lock(*bpm_latch_);
      frame_->pin_count_.fetch_sub(1);
      is_valid_ = false;
      // If pin_count_ goes to 0, I need to set it as evictable. Should I also Evict if possible?
      if (frame_->pin_count_.load() == 0) {
        Flush();
        replacer_->SetEvictable(frame_->frame_id_, true);
      }
    }
    unique_lock_.unlock();
  }
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
