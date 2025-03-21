# 生成可执行文件
g++ test/stress_test.cpp -o ./bin/stress  --std=c++11 -pthread  
# 执行
./bin/stress