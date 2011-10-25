#include <pthread.h>
#include <string>
#include <string.h>
#include <iostream>
#include <stack>
#include <vector>
#include <list>
#include <utility>
#include <ei.h>

extern "C" void io_output(int * arg) {
    std::cout << "the src :" << (*arg) << std::endl;
    std::cout << "the end !" << std::endl;
}

extern "C" void io_async(const char* cmd, ei_x_buff* result) {
    std::string s(cmd);
    std::cout << "the cmd ====:" << cmd << std::endl;
    ei_x_encode_tuple_header(result, 2);
    ei_x_encode_atom(result, "ok");
    ei_x_encode_string(result, "success");
}

