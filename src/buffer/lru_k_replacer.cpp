//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

static const size_t INF_TIMESTAMP = std::numeric_limits<size_t>::max();

LRUKNode::LRUKNode(frame_id_t fid, size_t k) : k_(k), fid_(fid) {}

auto LRUKNode::GetFrameId() const -> frame_id_t { return fid_; }

auto LRUKNode::IsEvictable() const -> bool { return is_evictable_; }

auto LRUKNode::SetEvictable(bool set_evictable) -> void { is_evictable_ = set_evictable; }

auto LRUKNode::RecordAccess(size_t timestamp, AccessType type) -> void {
  history_.push_back(timestamp);
  if (history_.size() > k_) {
    history_.pop_front();
  }
}

auto LRUKNode::GetKBackDist(size_t current_timestamp) const -> size_t {
  if (history_.size() < k_) {
    return INF_TIMESTAMP;
  }

  return current_timestamp - history_.front();
}

auto LRUKNode::GetEarliestTimestamp() const -> size_t { return history_.front(); }

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  // TODO: Handle latches and concurrency
  auto lock = std::lock_guard(latch_);

  if (curr_size_ == 0) {
    return std::nullopt;
  }

  std::priority_queue<std::pair<size_t, frame_id_t>, std::vector<std::pair<size_t, frame_id_t>>,
                      std::greater<std::pair<size_t, frame_id_t>>>
      min_heap_inf_distance_timestamp;
  std::priority_queue<std::pair<size_t, frame_id_t>, std::vector<std::pair<size_t, frame_id_t>>,
                      std::less<std::pair<size_t, frame_id_t>>>
      max_heap_k_backward_distance;

  std::unique_ptr<LRUKNode> max_k_dist_node;

  for (auto &it : node_store_) {
    if (it.second.IsEvictable()) {
      auto k_backward_distance = it.second.GetKBackDist(current_timestamp_);
      if (k_backward_distance == INF_TIMESTAMP) {
        min_heap_inf_distance_timestamp.push(
            std::pair<size_t, frame_id_t>(it.second.GetEarliestTimestamp(), it.second.GetFrameId()));
      } else {
        max_heap_k_backward_distance.push(std::pair<size_t, frame_id_t>(k_backward_distance, it.second.GetFrameId()));
      }
    }
  }

  if (min_heap_inf_distance_timestamp.empty() && max_heap_k_backward_distance.empty()) {
    return std::nullopt;
  }

  auto frame_id = min_heap_inf_distance_timestamp.empty() ? max_heap_k_backward_distance.top().second
                                                          : min_heap_inf_distance_timestamp.top().second;

  node_store_.erase(frame_id);
  curr_size_--;

  return std::optional<frame_id_t>(frame_id);
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  // TODO: Handle latches and concurrency
  auto lock = std::lock_guard(latch_);
  BUSTUB_ASSERT(frame_id <= (frame_id_t)replacer_size_, "Invalid Frame ID");
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    node_store_.emplace(std::piecewise_construct, std::forward_as_tuple(frame_id), std::forward_as_tuple(frame_id, k_));
  }
  // Previous iterator may have been invalidated by emplace
  it = node_store_.find(frame_id);
  it->second.RecordAccess(current_timestamp_, access_type);
  current_timestamp_++;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // TODO: Handle latches and concurrency
  auto lock = std::lock_guard(latch_);
  BUSTUB_ASSERT(frame_id <= (frame_id_t)replacer_size_, "Invalid Frame ID");
  auto it = node_store_.find(frame_id);
  // BUSTUB_ASSERT(it != node_store_.end(), "Frame ID not accessed before setting as evictable");
  /*if (it == node_store_.end()) {
      throw std::invalid_argument{"cannot find frame_id"};
  }*/
  if (it != node_store_.end()) {
    auto is_evictable = it->second.IsEvictable();

    if (!is_evictable && set_evictable) {
      curr_size_++;
    } else if (is_evictable && !set_evictable) {
      curr_size_--;
    }
    it->second.SetEvictable(set_evictable);
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  // TODO: Handle latches and concurrency
  auto lock = std::lock_guard(latch_);
  auto it = node_store_.find(frame_id);
  if (it != node_store_.end()) {
    BUSTUB_ASSERT(it->second.IsEvictable(), "Trying to remove a non-evictable Frame ID");
    node_store_.erase(it);
    curr_size_--;
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
