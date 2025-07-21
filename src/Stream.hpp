#include <memory>
#include <map>
#include <string>
#include <cstring>

class Stream {
public:
    struct RadixTrieNode {
        std::string prefix;
        bool is_key = false;
        std::string key;
        std::string value;
        std::map<unsigned char, std::unique_ptr<RadixTrieNode>> children;
    };

    Stream() : root(std::make_unique<RadixTrieNode>()), lastMillisecondsTime("0"), lastSeqno("0") {}

    void insert(const std::string& id, const std::string& key, const std::string& value) {
        RadixTrieNode* curr = root.get();
        std::string_view k = id;
        std::string::size_type pos = id.find('-');
        std::string timestamp = id.substr(0, pos);
        std::string seqno = id.substr(pos + 1);
        lastMillisecondsTime = timestamp;
        lastSeqno = seqno;
        while (true) {
            size_t common = commonPrefix(k, curr->prefix);

            if (common < curr->prefix.size()) {
                auto child = std::make_unique<RadixTrieNode>();
                child->prefix = curr->prefix.substr(common);
                child->is_key = curr->is_key;
                child->key = key;
                child->value = curr->value;
                child->children = std::move(curr->children);

                curr->prefix = std::string(k.substr(0, common));
                curr->is_key = false;
                curr->key = "";
                curr->value = "";
                curr->children.clear();
                curr->children[child->prefix[0]] = std::move(child);
            }

            k.remove_prefix(common);
            if (k.empty()) {
                curr->is_key = true;
                curr->key = key;
                curr->value = value;
                return;
            }

            unsigned char next_char = k[0];
            if (!curr->children.count(next_char)) {
                auto new_node = std::make_unique<RadixTrieNode>();
                new_node->prefix = std::string(k);
                new_node->is_key = true;
                new_node->key = key;
                new_node->value = value;
                curr->children[next_char] = std::move(new_node);
                return;
            }

            curr = curr->children[next_char].get();
        }
    }

    std::string find(const std::string& key) const {
        const RadixTrieNode* curr = root.get();
        std::string_view k = key;

        while (true) {
            if (k.size() < curr->prefix.size() || curr->prefix != k.substr(0, curr->prefix.size())) {
                return {};
            }

            k.remove_prefix(curr->prefix.size());
            if (k.empty()) {
                if (curr->is_key) {
                    return curr->value;
                } else {
                    return {};
                }
            }

            unsigned char next_char = k[0];
            if (!curr->children.count(next_char)) {
                return {};
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
