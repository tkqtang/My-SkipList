#include <iostream>
#include <string>
#include "./src/Skiplist.h"

int main() {
    Skiplist<int, std::string>  skipList(6);
	skipList.insert_element(1, "学"); 
	skipList.insert_element(3, "算法"); 
	skipList.insert_element(7, "认准"); 
	skipList.insert_element(8, "微信公众号：代码随想录"); 
	skipList.insert_element(9, "学习"); 
	skipList.insert_element(19, "算法不迷路"); 
	skipList.insert_element(19, "赶快关注吧你会发现详见很晚！"); 

    std::cout  << "skipList size:" << skipList.size() << std::endl;

    skipList.dump_file();

    Skiplist<int, std::string> skipList2(6);
    skipList2.load_file();

    skipList2.search_element(9);
    skipList2.search_element(18);


    skipList2.display_list();

    skipList2.delete_element(3);
    skipList2.delete_element(7);

    std::cout << "skipList size:" << skipList.size() << std::endl;
    skipList.display_list();


    std::cout << "skipList2 size:" << skipList2.size() << std::endl;
    skipList2.display_list();
    

    return 0;
}