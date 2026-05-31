#pragma once

#include <string>

bool GetRpcAddress(const std::string &method_path, std::string *host_data, std::string *error);
