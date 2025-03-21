#include <cmath>
#include <cstdlib>
#include <unordered_map>
#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include <mutex>
#include <fstream>
#include "Node.h"

#define STORE_FILE "store/dumpFile"


std::mutex mtx;
std::string delimiter = ":";

template<typename Key, typename Value>
class Skiplist
{

public:
    Skiplist(int);
    ~Skiplist();
    int get_random_level();
    Node<Key, Value> *create_node(Key, Value, int);
    int insert_element(Key, Value);
    bool search_element(const Key&);
    void delete_element(const Key&);
    int edit_elemnent(const Key&, Value);
    void display_list();
    void clear();
    void dump_file();
    void load_file();
    int size() const;


private:
    void get_key_value_from_string(const std::string& str, std::string *key, std::string *val);
    bool is_valid_string(const std::string& str);


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


};

template<typename Key, typename Value>
Node<Key, Value>* Skiplist<Key, Value>::create_node(Key key, Value val, int level){
    Node<Key, Value>* node = new Node<Key, Value>(key, val, level);  
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
    Node<Key, Value>* current = _header -> forward[0];
    while(current){
        Node<Key, Value>* tmp = current -> forward[0];
        delete current;
        current = tmp;
    }
    for(int i = 0; i <= this -> _skip_list_level; ++ i){
        _header -> forward[i] = nullptr;
    }
    this -> _skip_list_level = 0;
    this -> _element_count = 0;
}

template<typename Key, typename Value>
Skiplist<Key, Value>::Skiplist(const int max_level)
{
    Key key;
    Value val;
    this -> _max_level = max_level;
    this -> _header = create_node(key, val, max_level);
    this -> _element_count = 0;
    this -> _skip_list_level = 0;
}

template <typename key, typename value>
Skiplist<key, value>::~Skiplist()
{
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
    Node<Key, Value> *current = _header;
    for(int i = _skip_list_level; i >=0; -- i){
        while(current -> forward[i] != nullptr && current -> forward[i] -> get_key() < key){
            current = current -> forward[i];
        }
    }
    current = current -> forward[0];
    
    return current && current -> get_key() == key;
}


template<typename Key, typename Value>
int Skiplist<Key, Value>::edit_elemnent(const Key& key, Value val) {
    Node<Key, Value> *current = _header;
    for(int i = _skip_list_level; i >=0; -- i){
        while(current -> forward[i] != nullptr && current -> forward[i] -> get_key() < key){
            current = current -> forward[i];
        }
    }
    current = current -> forward[0];
    if(current == nullptr || current -> get_key() != key) {
        return 0;
    }


    if (current && current -> get_key() == key){
        current -> set_value(val);
    }
    return 1;
}

template<typename Key, typename Value>
int Skiplist<Key, Value>::insert_element(Key key, Value val){
    
    mtx.lock();
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
        mtx.unlock();
        return 1;
    }

    int random_level = get_random_level();
    if(random_level > _skip_list_level){
        for(int i = _skip_list_level + 1; i <= random_level; ++ i){
            update[i] =  _header;
        }
        _skip_list_level = random_level;
    }

    Node<Key, Value> *node = create_node(key, val, random_level);
    for(int i = 0; i <= random_level; ++ i){
        node -> forward[i] = update[i] -> forward[i];
        update[i] -> forward[i] = node;
    }

    ++ _element_count;

    mtx.unlock();

    return 0;
}

/* 
* 删除根据key值跳表中的节点
* @param key: 待删除节点的key值
*/
template<typename Key, typename Value>
void Skiplist<Key, Value>::delete_element(const Key& key){
    mtx.lock();
    Node<Key, Value>* current = this -> _header;
    Node<Key, Value>* del = nullptr;
    for(int i = this -> _skip_list_level; i >= 0; -- i){
        while(current -> forward[i] != nullptr && current -> forward[i] -> get_key() < key){
            current = current -> forward[i];
        }
        if(current -> forward[i] != nullptr && current -> forward[i] -> get_key() == key){
            if(del == nullptr){
                del = current -> forward[i];
            }
            current -> forward[i] = del -> forward[i];
        }
    }
    while(_skip_list_level > 0 && _header -> forward[_skip_list_level] == nullptr){
        -- _skip_list_level;
    }
    if(del != nullptr){
        delete del;
        -- this -> _element_count;
    }
    mtx.unlock();
    return;
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
            std::cout << " " <<current -> get_key() << ":" << current -> get_value() << " ";
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

        _file_writer << node -> get_key() << delimiter << node -> get_value() << "\n";        _file_writer << node -> get_key() << delimiter << node -> get_value() << "\n";
        std::cout << node -> get_key() << delimiter << node -> get_value() << "\n";        _file_writer << node -> get_key() << delimiter << node -> get_value() << "\n";
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


