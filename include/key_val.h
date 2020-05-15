#ifndef UNTITLED2_KEY_VAL_H
#define UNTITLED2_KEY_VAL_H

#include <string>

struct element{
    std::string _family_name,_key,_value;
    element(){}
    element(std::string family_name,std::string key,std::string value)
    {
        _family_name=family_name;
        _key=key;
        _value=value;
    }
};
#endif //UNTITLED2_KEY_VAL_H
