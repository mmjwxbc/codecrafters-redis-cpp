#include <string>
#include <vector>
#include <unordered_set>
#include <map>
#include <memory>
#include <utility>
#include <iostream>
struct RedisStreamEntry {
    std::string entry_id;
    std::vector<std::pair<std::string, std::string>> fields;
    std::unordered_set<std::string> field_set;

    RedisStreamEntry() = default;
    explicit RedisStreamEntry(const std::string& id) : entry_id(id) {}

    void addField(const std::string& field, const std::string& value) {
        if (field_set.count(field)) return;
        field_set.insert(field);
        fields.emplace_back(field, value);
    }

    const std::vector<std::pair<std::string, std::string>>& getFields() const {
        return fields;
    }
};

class Stream {
public:
    struct RadixTrieNode {
        std::string prefix;
        bool is_key = false;
        RedisStreamEntry entry;
        std::map<unsigned char, std::unique_ptr<RadixTrieNode>> children;
    };

    Stream() : root(std::make_unique<RadixTrieNode>()), lastMillisecondsTime("0"), lastSeqno("0") {}

    void insert(const std::string& id, const std::vector<std::pair<std::string, std::string>>& field_values) {
        RadixTrieNode* curr = root.get();
        std::string_view k = id;

        // Update last ID parts
        auto pos = id.find('-');
        if (pos != std::string::npos) {
            lastMillisecondsTime = id.substr(0, pos);
            lastSeqno = id.substr(pos + 1);
        }

        while (true) {
            size_t common = commonPrefix(k, curr->prefix);

            if (common < curr->prefix.size()) {
                auto child = std::make_unique<RadixTrieNode>();
                child->prefix = curr->prefix.substr(common);
                child->is_key = curr->is_key;
                child->entry = std::move(curr->entry);
                child->children = std::move(curr->children);

                curr->prefix = std::string(k.substr(0, common));
                curr->is_key = false;
                curr->entry = RedisStreamEntry();  // reset
                curr->children.clear();
                curr->children[child->prefix[0]] = std::move(child);
            }

            k.remove_prefix(common);
            if (k.empty()) {
                curr->is_key = true;
                curr->entry = RedisStreamEntry(id);
                for (const auto& [field, value] : field_values) {
                    curr->entry.addField(field, value);
                }
                return;
            }

            unsigned char next_char = k[0];
            if (!curr->children.count(next_char)) {
                auto new_node = std::make_unique<RadixTrieNode>();
                new_node->prefix = std::string(k);
                new_node->is_key = true;
                new_node->entry = RedisStreamEntry(id);
                for (const auto& [field, value] : field_values) {
                    new_node->entry.addField(field, value);
                }
                curr->children[next_char] = std::move(new_node);
                return;
            }

            curr = curr->children[next_char].get();
        }
    }

    const RedisStreamEntry* find(const std::string& id) const {
        const RadixTrieNode* curr = root.get();
        std::string_view k = id;

        while (true) {
            if (k.size() < curr->prefix.size() || curr->prefix != k.substr(0, curr->prefix.size())) {
                return nullptr;
            }

            k.remove_prefix(curr->prefix.size());
            if (k.empty()) {
                return curr->is_key ? &curr->entry : nullptr;
            }

            unsigned char next_char = k[0];
            if (!curr->children.count(next_char)) {
                return nullptr;
            }
            curr = curr->children.at(next_char).get();
        }
    }

    std::string getLastMillisecondsTime() const {
        return lastMillisecondsTime;
    }

    std::string getLastSeqno() const {
        return lastSeqno;
    }

private:
    std::unique_ptr<RadixTrieNode> root;
    std::string lastMillisecondsTime;
    std::string lastSeqno;

    static size_t commonPrefix(std::string_view a, std::string_view b) {
        size_t i = 0;
        while (i < a.size() && i < b.size() && a[i] == b[i]) ++i;
        return i;
    }
};

class SimpleStream {
public:
    std::map<std::string, std::vector<RedisStreamEntry>> entry_key_value;

    void insert(const std::string& id, const std::vector<std::pair<std::string, std::string>>& field_values) {
        auto pos = id.find('-');
        if (pos != std::string::npos) {
            lastMillisecondsTime = id.substr(0, pos);
            lastSeqno = id.substr(pos + 1);
        }
        RedisStreamEntry entry(id);
        for (const auto& [field, value] : field_values) {
            entry.addField(field, value);
        }
        entry_key_value[lastMillisecondsTime].emplace_back(std::move(entry));   
    }

    std::vector<RedisStreamEntry> xrange(std::string start, std::string end, bool is_start, bool is_end) {
        std::vector<RedisStreamEntry> result;

        auto beginIt = entry_key_value.lower_bound(start);
        auto endIt = entry_key_value.upper_bound(end);
        if(is_start) beginIt = entry_key_value.begin();
        if(is_end) endIt = entry_key_value.end();

        for (auto it = beginIt; it != endIt; ++it) {
            std::cout << "it timestamp = " << it->first << std::endl;
            result.insert(result.end(), it->second.begin(), it->second.end());
        }
        std::cout << "xrange size = " << result.size() << std::endl;
        return result;
    }

    SimpleStream() : lastMillisecondsTime("0"), lastSeqno("0") {}


    std::string getLastMillisecondsTime() const {
        return lastMillisecondsTime;
    }

    std::string getLastSeqno() const {
        return lastSeqno;
    }

private:
    std::string lastMillisecondsTime;
    std::string lastSeqno;
};