#include <cstdlib>
#include <cstring>

template <typename Key, typename Value>

class Node {

public:
    Node(){};
    Node(Key, Value, int);
    ~Node();
    Key get_key() const;
    Value get_value() const;
    void set_value(Value);
    Node<Key, Value> **forward;
    int node_level;

private:
    Key key;
    Value val;
};

template<typename Key, typename Value>
Node<Key, Value>::Node(const Key key, const Value val, int level)
{
    this -> key = key;
    this -> val = val;
    this -> node_level = level;

    // level + 1, level if from [0, level].
    this -> forward = new Node<Key, Value> *[level + 1];

    // 初始化为空指针
    memset(this -> forward, 0, sizeof(Node<Key, Value>*) * (level + 1));
}

template<typename Key, typename Value>
Node<Key, Value>::~Node()
{
    delete []forward;
}

template<typename Key, typename Value>
Key Node<Key, Value>::get_key() const{
    return key;
}

template<typename Key, typename Value>
Value Node<Key, Value>::get_value() const{
    return val;
}


template<typename Key, typename Value>
void Node<Key, Value>::set_value(Value val){
    val = val;
}



