#include <cmath>
#include <cstdlib>
#include <unordered_map>
#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include <mutex>
#include <fstream>
#include <unordered_map>
#include <atomic>
#include <memory>
#include <list>
#include <ctime>
#include <shared_mutex>
#include <mutex>
#include <thread>
#include <condition_variable>

#define STORE_FILE "store/dumpFile"   // 定义文件存储路径

std::string delimiter = ":";          // 分隔符


template <typename Key, typename Value>

class Node {
    
private:

    Key _key;
    Value _val;

    int _ttl;
    time_t _end_time; 


public:

    // skiplist 节点指针数组，指向对应层次的后继节点
    Node<Key, Value> **forward;
    int node_level;
 
    bool deleted{false};  // 标记节点是否被删除    
    bool timed{false};    // 标记是否为定时节点
    
 
    Node(){};
    Node(const Key&, const Value&, int);
    Node(const Key&, const Value&, int, int);
    ~Node();
    
    Key get_key() const;
    Value get_value() const;
    void set_value(const Value&);


    void mark_deleted();  // 设置删除标记
    bool is_timeout () const; 
    void set_end_time();  // 设置过期时间

};


template<typename Key, typename Value>
Node<Key, Value>::Node(const Key &key, const Value &val, int level, int ttl) : 
    _key(key),
    _val(val),
    node_level(level),
    _ttl(ttl) {
    
    // level + 1, level if from [0, level].
    forward = new Node<Key, Value> *[level + 1];

    // 初始化为空指针
    memset(forward, 0, sizeof(Node<Key, Value>*) * (level + 1));

    if(_ttl > 0) {
        set_end_time();
        timed = true;
    }

}


template<typename Key, typename Value>
Node<Key, Value>::Node(const Key &key, const Value &val, int level) : Node(key, val, level, -1) {}

template<typename Key, typename Value>
Node<Key, Value>::~Node()
{
    delete[] forward;
}

template<typename Key, typename Value>
Key Node<Key, Value>::get_key() const{
    return _key;
}

template<typename Key, typename Value>
Value Node<Key, Value>::get_value() const{
    return _val;
}


template<typename Key, typename Value>
void Node<Key, Value>::set_value(const Value &val){
    _val = val;
}



template<typename Key, typename Value>
void Node<Key, Value>::mark_deleted() {
    deleted = true;
}

template<typename Key, typename Value>
bool Node<Key, Value>::is_timeout() const {
    return timed && (time(nullptr) > _end_time);
}

template<typename Key, typename Value>
void Node<Key, Value>::set_end_time() {
    if (timed) {
        _end_time = time(nullptr) + _ttl;
    }
        
}




template<typename Key, typename Value>
class LRUCache {

private:

    size_t _capacity;

    std::unordered_map<Key, typename std::list<Node<Key, Value>*>::iterator> _cache;
    std::list<Node<Key, Value>*> _cache_list;

    std::mutex mtx;

public:

    LRUCache() : _capacity(-1) {}
    
    LRUCache(size_t capacity) : _capacity(capacity) {}

    ~LRUCache() { 
        clear(); 
    }

    void get(const Key& key);

    void put(Node<Key, Value> *node);

    void remove(const Key &key);

    void clear() ;

    size_t size() const;

private:

    void clean_expired(); // 删除过期缓存

    void evict();   // 淘汰缓存

};


template<typename Key, typename Value>
void LRUCache<Key, Value>::get(const Key& key) {
    
    std::lock_guard<std::mutex> lock(mtx);
    clean_expired();
    
    if (_cache.find(key) != _cache.end()) {
        auto iter = _cache[key];
        _cache_list.splice(_cache_list.begin(), _cache_list, iter);
        (*iter) -> set_end_time();
    }

}


template<typename Key, typename Value>
void LRUCache<Key, Value>::put(Node<Key, Value> *node) {
    
    std::lock_guard<std::mutex> lock(mtx);
    clean_expired();
    const Key &key = node -> get_key();
    
    if (_cache.find(key) != _cache.end()) {
        // 已存在则先删除旧节点
        _cache_list.erase(_cache[key]);
    }

    _cache_list.push_front(node);
    _cache[key] = _cache_list.begin();

    node -> set_end_time();
}



template<typename Key, typename Value>
void LRUCache<Key, Value>::remove(const Key &key) {
    
    std::lock_guard<std::mutex> lock(mtx);
    clean_expired();
    
    if( _cache.find(key) != _cache.end()) {
        _cache_list.erase(_cache[key]);
        _cache.erase(key);
    }
}


template<typename Key, typename Value>
void LRUCache<Key, Value>::clear() {
    std::lock_guard<std::mutex> lock(mtx);
    _cache_list.clear();
    _cache.clear();
}


template<typename Key, typename Value>
size_t LRUCache<Key, Value>::size() const {
    return _cache_list.size();
}


template<typename Key, typename Value>
void LRUCache<Key, Value>::clean_expired() {
        
    while (!_cache_list.empty()) {
        
        auto last_iter = std::prev(_cache_list.end());
        Node<Key, Value>* node = *last_iter;

        if (node -> is_timeout()) {
        
            _cache.erase(node -> get_key());
            _cache_list.erase(last_iter);
            node -> mark_deleted();
        
        } else {
            break;
        }
    } 
}


template<typename Key, typename Value>
void LRUCache<Key, Value>::evict() {

    auto last_iter = std::prev(_cache_list.end());
    Node<Key, Value>* node = *last_iter;
    _cache.erase(node->get_key());
    _cache_list.erase(last_iter);
    node->mark_deleted();

}



template<typename Key, typename Value>
class Skiplist{

private:
    // maximum level of the skip list
    int _max_level;
    
    // current level of the skip list
    int _skip_list_level;
    
    // pointer to header node 
    Node<Key, Value> *_header;
    
    // current element count
    int _element_count;
    
    // file operator
    std::ofstream _file_writer;
    std::ifstream _file_reader;

    LRUCache<Key, Value> lru;
    std::shared_mutex rw_mtx;

    int _compact_interval_sec;                 // 定时 compact 的间隔（默认60秒）
    std::thread _compact_thread;               // 后台 compact 线程
    std::atomic<bool> _compact_running{false}; // 控制线程启停
    std::condition_variable_any _compact_cv;
    std::mutex _compact_mtx;
public:
    
    Skiplist(int);
    Skiplist(int, int);
    ~Skiplist();

    int get_random_level();
    int insert_element(const Key&, const Value&);
    int insert_element(const Key&, const Value&, int);
    bool search_element(const Key&);
    void delete_element(const Key&);
    int edit_elemnent(const Key&, const Value&);
    void display_list();
    void clear();
    void dump_file();
    void load_file();
    int size() const;
    void stop_compact_scheduler();

private:
    Node<Key, Value> *create_node(const Key&, const Value&, int);
    Node<Key, Value> *create_node(const Key&, const Value&, int, int);
    void get_key_value_from_string(const std::string& str, std::string *key, std::string *val);
    bool is_valid_string(const std::string& str);
    void compact();
    void start_compact_scheduler();

};

template<typename Key, typename Value>
Node<Key, Value>* Skiplist<Key, Value>::create_node(const Key& key, const Value& val, int level){
    Node<Key, Value>* node = new Node<Key, Value>(key, val, level);  
    return node;
}


template<typename Key, typename Value>
Node<Key, Value>* Skiplist<Key, Value>::create_node(const Key& key, const Value& val, int level, int ttl){
    Node<Key, Value>* node = new Node<Key, Value>(key, val, level, ttl);  
    return node;
}



template<typename Key, typename Value>
int Skiplist<Key, Value>::get_random_level(){
    int k = 1;
    while(rand() % 2 == 1){
        ++ k;
    }
    k = (k < _max_level) ? k : _max_level;
    return k;
}


template <typename Key, typename Value>
void Skiplist<Key, Value>::clear(){

    std::unique_lock<std::shared_mutex> lock(rw_mtx);

    Node<Key, Value>* current = _header -> forward[0];
    while(current){
        Node<Key, Value>* tmp = current -> forward[0];
        delete current;
        current = tmp;
    }
    for(int i = 0; i <= this -> _skip_list_level; ++ i){
        _header -> forward[i] = nullptr;
    }
    _skip_list_level = 0;
    _element_count = 0;
    lru.clear();
}



template<typename Key, typename Value>
Skiplist<Key, Value>::Skiplist(const int max_level) : Skiplist(max_level, 5) {}

template<typename Key, typename Value>
Skiplist<Key, Value>::Skiplist(const int max_level, const int compact_interval_sec) : _max_level(max_level), _compact_interval_sec(compact_interval_sec) {

    Key key;
    Value val;
    _header = create_node(key, val, max_level);
    _element_count = 0;
    _skip_list_level = 0;
    start_compact_scheduler(); // 启动定时线程

}


template <typename key, typename value>
Skiplist<key, value>::~Skiplist()
{
    std::cout << "~~~~~" << std::endl;
    stop_compact_scheduler(); // 停止定时线程
    if (_file_writer.is_open()) {
        _file_writer.close();
    }
    if (_file_reader.is_open()) {
        _file_reader.close();
    }
    clear();
    delete _header;
}



template <typename Key, typename Value>
bool Skiplist<Key, Value>::search_element(const Key& key){

    Node<Key, Value> *current = nullptr;

    std::shared_lock<std::shared_mutex> lock(rw_mtx);

    current = _header;
    for(int i = _skip_list_level; i >=0; -- i){
        while(current -> forward[i] != nullptr && current -> forward[i] -> get_key() < key){
            current = current -> forward[i];
        }
    }
    current = current -> forward[0];
    
    while(current && current -> deleted) {
        current = current -> forward[0];
    }

    if (current && current -> timed == true) {
        lru.get(key);
    }

    return current && current -> get_key() == key;
}


template<typename Key, typename Value>
int Skiplist<Key, Value>::edit_elemnent(const Key& key, const Value &val) {


    Node<Key, Value> *current = nullptr;

    std::unique_lock<std::shared_mutex> lock(rw_mtx);

    current = _header;
    for(int i = _skip_list_level; i >=0; -- i){
        while(current -> forward[i] != nullptr && current -> forward[i] -> get_key() < key){
            current = current -> forward[i];
        }
    }
    current = current -> forward[0];
    while(current && current -> deleted) {
        current = current -> forward[0];
    }

    if(current == nullptr || current -> get_key() != key) {
        return 0;
    }

    if (current && current -> get_key() == key){
        current -> set_value(val);
    }


    if (current -> timed) {
        lru.get(key);
    }

    return 1;
}

template<typename Key, typename Value>
int Skiplist<Key, Value>::insert_element(const Key& key, const Value &val){
    
    return insert_element(key, val, -1);

}


template<typename Key, typename Value>
int Skiplist<Key, Value>::insert_element(const Key& key, const Value &val, int ttl){
    
    Node<Key, Value> *node = nullptr;

    std::unique_lock<std::shared_mutex> lock(rw_mtx);
    Node<Key, Value> *current = this -> _header;
    Node<Key, Value> **update = new Node<Key, Value> *[this -> _max_level + 1];

    memset(update, 0, sizeof(Node<Key, Value>*) * (this -> _max_level + 1));

    for(int i = _skip_list_level; i >= 0; -- i){
        while(current -> forward[i] != nullptr && current -> forward[i] -> get_key() < key){
            current = current -> forward[i];
        }
        update[i] = current;
    }

    current = current -> forward[0];
    if(current != nullptr && current -> get_key() == key){
        std::cout << "Key: " << key << ", exists. \n";
        return 1;
    }

    int random_level = get_random_level();
    if(random_level > _skip_list_level){
        for(int i = _skip_list_level + 1; i <= random_level; ++ i){
            update[i] =  _header;
        }
        _skip_list_level = random_level;
    }

    node = create_node(key, val, random_level, ttl);
    for(int i = 0; i <= random_level; ++ i){
        node -> forward[i] = update[i] -> forward[i];
        update[i] -> forward[i] = node;
    }

    delete[] update;

    ++ _element_count;
    

    if (node->timed) {
        lru.put(node);  
    }

    return 0;
}



/* 
* 根据key值，惰性删除节点
* @param key: 待删除节点的key值
*/
template<typename Key, typename Value>
void Skiplist<Key, Value>::delete_element(const Key& key){
    
    std::unique_lock<std::shared_mutex> lock(rw_mtx);
    Node<Key, Value>* current = _header;
    
    for(int i = this -> _skip_list_level; i >= 0; -- i){
        while(current -> forward[i] != nullptr && current -> forward[i] -> get_key() < key){
            current = current -> forward[i];
        }
    }
    
    current = current -> forward[0];
    while (current && current -> deleted) {  // 跳过已标记删除的节点
        current = current -> forward[0];
    }

    if(current != nullptr && current -> get_key() == key) {
        
        current -> mark_deleted();
        if (current -> timed) {
            lru.remove(key);
        }
    }

    return ;
}


int cnt = 0;
template<typename Key, typename Value>
void Skiplist<Key, Value>::compact() {
    
    std::unique_lock<std::shared_mutex> lock(rw_mtx);
    
    // 最底层直接删除节点，其他层将指针跳过节点
    for (int i = _skip_list_level; i >= 0; -- i) {
        
        Node<Key, Value>* current = _header->forward[i];
        Node<Key, Value>* prev = _header;

        while(current) {

            if(current -> deleted) {

                prev -> forward[i] = current -> forward[i];

                if( i == 0) {
                    
                    Node<Key, Value> *tmp = current;
                    current = current -> forward[i];
                    delete tmp;
                    -- _element_count;
                
                } else {
                    current = current -> forward[i];
                }

            } else {

                prev = current;
                current = current->forward[i];
            }
        }
    }

    // 更新层次
    while( _skip_list_level > 0 && _header -> forward[_skip_list_level] == nullptr) {
        -- _skip_list_level;
    }

    std::cout << "in compact: " << ++cnt << std::endl;

}




template<typename Key, typename Value>
int Skiplist<Key, Value>::size() const {
    return this -> _element_count;
}




template <typename Key, typename Value>
void Skiplist<Key, Value>::display_list(){
    
    for(int i = _skip_list_level; i >= 0; -- i){
        Node<Key, Value>* current = this -> _header -> forward[i];
        std::cout << "Level " << i << " ";
        while(current != nullptr){
            if(current -> deleted == false) {
                std::cout << " " <<current -> get_key() << ":" << current -> get_value() << " ";
            }
            current = current -> forward[i];
        } 
        std::cout << "\n";
    }
}




template<typename Key, typename Value>
bool Skiplist<Key, Value>::is_valid_string(const std::string& str){
    
    if (str.empty()) {
        return false;
    }
    if (str.find(delimiter) == std::string::npos) {
        return false;
    }
    return true;
}



template<typename Key, typename Value>
void Skiplist<Key, Value>::get_key_value_from_string(const std::string& str, std::string* key, std::string* val){
    if (!is_valid_string(str)) {
        return ;
    }

    *key = str.substr(0, str.find(delimiter));
    *val = str.substr(str.find(delimiter) + 1, str.length());
}




template <typename Key, typename Value>
void Skiplist<Key, Value>::dump_file() {
    

    std::cout << "dump_file---------------\n";
    _file_writer.open(STORE_FILE);
    Node<Key, Value> *node = this -> _header -> forward[0];

    while (node != nullptr) {

        _file_writer << node -> get_key() << delimiter << node -> get_value() << "\n";      
        std::cout << node -> get_key() << delimiter << node -> get_value() << "\n";        
        node = node -> forward[0];
    }

    _file_writer.flush();
    _file_writer.close();

    return ;
}



template<typename Key, typename Value>
void Skiplist<Key, Value>::load_file() {

    _file_reader.open(STORE_FILE);
    std::cout << "load_file-------------\n";
    std::string line;
    std::string* key = new std::string();
    std::string* val = new std::string();
    while (getline(_file_reader, line)) {
        get_key_value_from_string(line, key, val);
        insert_element(stoi(*key), *val);
        std::cout << "key:" << *key << " value:" << *val << "\n"; 
    }
    delete key;
    delete val;
    _file_reader.close();

}


template<typename Key, typename Value>
void Skiplist<Key, Value>::start_compact_scheduler() {    
    _compact_running.store(true);
    _compact_thread = std::thread([this]() {
        std::unique_lock<std::mutex> lk(_compact_mtx);
        while (_compact_running.load()) {
            // 等待期间释放锁，允许其他线程修改状态
            if (_compact_cv.wait_for(lk, std::chrono::seconds(_compact_interval_sec)) == 
                std::cv_status::timeout) {
                lk.unlock(); // 释放锁后再执行compact
                compact();   // 假设compact不依赖当前锁保护的数据
                lk.lock();   // 重新加锁以继续循环或等待
            }
        }
    });
}


template<typename Key, typename Value>
void Skiplist<Key, Value>::stop_compact_scheduler() {

    std::cout << "stop compact" << std::endl;
    {
        std::lock_guard<std::mutex> lk(_compact_mtx);
        _compact_running.store(false);  // Mark the thread as stopping
    }
    _compact_cv.notify_all();     // 唤醒可能正在等待的线程
    if (_compact_thread.joinable()) {
        _compact_thread.join();
    }

}