#include <iostream>
#include <chrono>
#include <pthread.h>
#include <time.h>
#include "../src/Skiplist.h"

#define NUM_THREADS 3
#define TEST_COUNT 100000

Skiplist<int, std::string> skiplist(18, 10);

void *insertElement(void* threadid) {
    long tid;
    tid = (long) threadid;  
    std::cout << tid << std::endl;
    int tmp = TEST_COUNT / NUM_THREADS;
    for (int i = tid * tmp, count = 0; count < tmp; ++ i){
        ++ count;
        skiplist.insert_element(rand() % TEST_COUNT, "test");
    }
    pthread_exit(NULL);
}

void *getElement(void* threadid) {
    long tid;
    tid = (long) threadid;
    std::cout << tid << std::endl;
    int tmp = TEST_COUNT / NUM_THREADS;
    for (int i = tid * tmp, count = 0; count < tmp; ++ i) {
        ++ count;
        skiplist.search_element(rand() % TEST_COUNT);
    }
    pthread_exit(NULL);
}

int main() {

    srand (time(NULL));  
    {

        pthread_t threads[NUM_THREADS];
        int rc;
        int i;

        auto start = std::chrono::high_resolution_clock::now();

        for( i = 0; i < NUM_THREADS; i++ ) {
            std::cout << "main() : creating thread, " << i << std::endl;
            rc = pthread_create(&threads[i], NULL, insertElement, (void *)i);

            if (rc) {
                std::cout << "Error:unable to create thread," << rc << std::endl;
                exit(-1);
            }

        }

        void *ret;
        for( i = 0; i < NUM_THREADS; i++ ) {
            if (pthread_join(threads[i], &ret) !=0 )  {
                perror("pthread_create() error"); 
                exit(3);
            }
        }

        auto finish = std::chrono::high_resolution_clock::now(); 
        std::chrono::duration<double> elapsed = finish - start;
        std::cout << "insert elapsed:" << elapsed.count() << std::endl;

    }
    // skiplist.stop_compact_scheduler();
    pthread_exit(NULL);
    
    return 0;
}