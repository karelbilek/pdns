/*
 * This file is part of PowerDNS or dnsdist.
 * Copyright -- PowerDNS.COM B.V. and its contributors
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * In addition, for the avoidance of any doubt, permission is granted to
 * link this program with OpenSSL and to (re)distribute the binaries
 * produced as the result of such linking.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
#pragma once

#include <atomic>
#include <unordered_map>

enum class CacheInsertState : uint8_t
{
  Inserted,
  Replaced,
  Full,
  Existing,
};

template <typename K, typename V>
class CacheContainer
{
public:
  virtual void init(size_t t) = 0;

  // container should have only one implemented
  virtual std::optional<std::reference_wrapper<const V>> find(const K& key) const = 0;
  virtual std::optional<std::reference_wrapper<const V>> find(const K& key) = 0;

  virtual std::pair<CacheInsertState, std::optional<std::reference_wrapper<V>>> insert(const K& key, const V& value) = 0;
  virtual size_t remove(const std::function<bool(const V&)>& pred, size_t toRemove) = 0;
  virtual void visit(const K& key) = 0;

  virtual size_t size() const = 0;
  virtual void walk(const std::function<void(const K&, const V&)>& fun) const = 0;

  virtual ~CacheContainer() = default;
};

template <typename K, typename V>
class SieveCache : public CacheContainer<K, V>
{
public:
  void init(size_t t) override
  {
    // we reserve maxEntries + 1 to avoid rehashing from occurring
    // when we get to maxEntries, as it means a load factor of 1
    d_maxSize = t;
    d_map.reserve(t + 1);
    d_sieve_hand = d_list.end();
  };

  std::optional<std::reference_wrapper<const V>> find(const K& key) const override
  {
    auto mapIt = d_map.find(key);
    if (mapIt == d_map.end()) {
      return std::nullopt;
    }

    mapIt->second->can_erase.clear(std::memory_order_relaxed);
    return mapIt->second->value;
  };

  [[noreturn]] std::optional<std::reference_wrapper<const V>> find([[maybe_unused]] const K& key) override
  {
    throw std::logic_error("SieveCache does not need lock on reading");
  };

  std::pair<CacheInsertState, std::optional<std::reference_wrapper<V>>> insert(const K& key, const V& value) override
  {
    auto mapIt = d_map.find(key);
    if (mapIt != d_map.end()) {
      std::optional<std::reference_wrapper<V>> res = std::ref(mapIt->second->value);
      return std::make_pair(CacheInsertState::Existing, res);
    }

    auto state = CacheInsertState::Inserted;

    if (d_map.size() == d_maxSize) {
      state = CacheInsertState::Replaced;

      while (!d_sieve_hand->can_erase.test_and_set(std::memory_order_relaxed)) {
        d_sieve_hand++;
        if (d_sieve_hand == d_list.end()) {
          d_sieve_hand = d_list.begin();
        }
      }

      d_map.erase(d_sieve_hand->key);
      d_sieve_hand = d_list.erase(d_sieve_hand);
      if (d_sieve_hand == d_list.end()) {
        d_sieve_hand = d_list.begin();
      }
    }

    d_list.emplace_back(key, value);
    d_map.insert({key, std::prev(d_list.end())});

    if (d_sieve_hand == d_list.end()) {
      d_sieve_hand = d_list.begin();
    }
    return std::make_pair(state, std::nullopt);
  }

  size_t size() const override
  {
    return d_map.size();
  }

  void walk(const std::function<void(const K&, const V&)>& fun) const override
  {
    for (auto it = d_list.begin(); it != d_list.end(); ++it) {
      fun(it->key, it->value);
    }
  }

  size_t remove(const std::function<bool(const V&)>& pred, size_t toRemove) override
  {
    size_t removed = 0;
    size_t walked = 0;
    size_t origsize = d_map.size();

    // expunging does not move the sieve hand, but starts at it
    // we need separate hand
    auto expunge_hand = d_sieve_hand;
    while (d_map.size() > 0 && removed != toRemove && walked != origsize) {
      bool should_remove = pred(expunge_hand->value);
      bool should_move_sieve = false;
      if (!should_remove) {
        ++expunge_hand;
        ++walked;
      }
      else {
        if (!expunge_hand->can_erase.test_and_set(std::memory_order_relaxed)) {
          ++expunge_hand;
        } else {
          should_move_sieve = (d_sieve_hand == expunge_hand);
          d_map.erase(expunge_hand->key);
          expunge_hand = d_list.erase(expunge_hand);
          ++removed;
          ++walked;
        }
      }
      if (expunge_hand == d_list.end()) {
        expunge_hand = d_list.begin();
      }
      if (should_move_sieve) {
        d_sieve_hand = expunge_hand;
      }
    }
    return removed;
  };

  void visit(const K& key) override
  {
    auto mapIt = d_map.find(key);
    if (mapIt == d_map.end()) {
      // should not happen?
      return;
    }
    mapIt->second->can_erase.clear(std::memory_order_relaxed);
  };

private:
  size_t d_maxSize;

  struct SieveNode
  {
    K key;
    V value;
    // inversion of visited in sieve,
    std::atomic_flag can_erase;

    SieveNode(const K& k, const V& v) : key(k), value(v)
    {
      can_erase.test_and_set(std::memory_order_relaxed);
    }
  };

  using sieve_list = std::list<SieveNode>;
  using sieve_iter = typename sieve_list::iterator;

  // front: oldest; back: newest; bool - visited (starts at false)
  sieve_list d_list;
  std::unordered_map<K, sieve_iter> d_map;

  // if std::list is empty - std::list::end; otherwise - always pointing at list item, never at end()
  // hand moves from front to back
  sieve_iter d_sieve_hand;
};

template <typename K, typename V>
class LruCache : public CacheContainer<K, V>
{
public:
  void init(size_t t) override
  {
    // we reserve maxEntries + 1 to avoid rehashing from occurring
    // when we get to maxEntries, as it means a load factor of 1
    d_maxSize = t;
    d_map.reserve(t + 1);
  };

  [[noreturn]] std::optional<std::reference_wrapper<const V>> find([[maybe_unused]] const K& key) const override
  {
    throw std::logic_error("LruCache needs lock on reading");
  };

  std::optional<std::reference_wrapper<const V>> find(const K& key) override
  {
    auto mapIt = d_map.find(key);
    if (mapIt == d_map.end()) {
      return std::nullopt;
    }

    d_list.splice(d_list.end(), d_list, mapIt->second);
    return mapIt->second->second;
  };

  std::pair<CacheInsertState, std::optional<std::reference_wrapper<V>>> insert(const K& key, const V& value) override
  {
    auto mapIt = d_map.find(key);
    if (mapIt != d_map.end()) {
      std::optional<std::reference_wrapper<V>> res = mapIt->second->second;
      return std::make_pair(CacheInsertState::Existing, res);
    }

    auto state = CacheInsertState::Inserted;

    if (d_map.size() == d_maxSize) {
      state = CacheInsertState::Replaced;
      auto& newest = d_list.front();
      d_map.erase(newest.first);
      d_list.pop_front();
    }

    d_list.emplace_back(key, value);
    d_map.insert({key, std::prev(d_list.end())});
    return std::make_pair(state, std::nullopt);
  }

  size_t size() const override
  {
    return d_map.size();
  }

  void walk(const std::function<void(const K&, const V&)>& fun) const override
  {
    for (auto it = d_list.begin(); it != d_list.end(); ++it) {
      fun(it->first, it->second);
    }
  }

  size_t remove(const std::function<bool(const V&)>& pred, size_t toRemove) override
  {
    size_t removed = 0;
    for (auto it = d_list.begin(); it != d_list.end();) {
      if (pred(it->second)) {
        ++removed;
        d_map.erase(it->first);
        it = d_list.erase(it);
        if (removed >= toRemove) {
          return removed;
        }
      }
      else {
        ++it;
      }
    }
    return removed;
  };

  void visit(const K& key) override
  {
    auto mapIt = d_map.find(key);
    if (mapIt == d_map.end()) {
      // should not happen?
      return;
    }
    d_list.splice(d_list.end(), d_list, mapIt->second);
  };

private:
  size_t d_maxSize;

  // front: oldest; back: newest
  std::list<std::pair<K, V>> d_list;
  std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> d_map;
};

template <typename K, typename V>
class NoEvictionCache : public CacheContainer<K, V>
{
public:
  void init(size_t t) override
  {
    // we reserve maxEntries + 1 to avoid rehashing from occurring
    // when we get to maxEntries, as it means a load factor of 1
    d_maxSize = t;
    d_map.reserve(t + 1);
  };

  std::optional<std::reference_wrapper<const V>> find(const K& key) const override
  {
    auto it = d_map.find(key);
    if (it == d_map.end()) {
      return std::nullopt;
    }
    return it->second;
  };

  [[noreturn]] std::optional<std::reference_wrapper<const V>> find([[maybe_unused]] const K& key) override
  {
    throw std::logic_error("NoEvictionCache does not lock on reading");
  };

  std::pair<CacheInsertState, std::optional<std::reference_wrapper<V>>> insert(const K& key, const V& value) override
  {
    if (d_map.size() == d_maxSize) {
      return std::make_pair(CacheInsertState::Full, std::nullopt);
    }

    auto [it, result] = d_map.insert({key, value});
    if (result) {
      return std::make_pair(CacheInsertState::Inserted, std::nullopt);
    }

    return std::make_pair(CacheInsertState::Existing, it->second);
  };

  size_t size() const override
  {
    return d_map.size();
  }

  void walk(const std::function<void(const K&, const V&)>& fun) const override
  {
    for (auto it = d_map.begin(); it != d_map.end(); ++it) {
      fun(it->first, it->second);
    }
  }

  size_t remove(const std::function<bool(const V&)>& pred, size_t toRemove) override
  {
    size_t removed = 0;
    for (auto it = d_map.begin(); it != d_map.end();) {
      if (pred(it->second)) {
        ++removed;
        it = d_map.erase(it);
        if (removed >= toRemove) {
          return removed;
        }
      }
      else {
        ++it;
      }
    }
    return removed;
  };

  void visit([[maybe_unused]] const K& key) override {
    // noop
  };

private:
  size_t d_maxSize;
  std::unordered_map<K, V> d_map;
};
