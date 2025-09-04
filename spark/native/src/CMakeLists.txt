cmake_minimum_required(VERSION 3.24)

project(bdd VERSION 1.0.0 DESCRIPTION "BDDs")

add_compile_definitions(MALLOC=malloc)
add_compile_definitions(REALLOC=realloc)
add_compile_definitions(FREE=free)

add_library(${PROJECT_NAME} SHARED
    bdd.c
    bdd.h
    dictionary.c
    dictionary.h
    utils.c
    utils.h
    vector.c
    vector.h
    com_doubtless_bdd_Native_00024.c
    com_doubtless_bdd_Native_00024.h
)

set(JAVA_AWT_LIBRARY NotNeeded)
set(JAVA_JVM_LIBRARY NotNeeded)
find_package(JNI REQUIRED)

target_link_libraries(${PROJECT_NAME} PRIVATE JNI::JNI)

target_include_directories(${PROJECT_NAME} PRIVATE .)

set_target_properties(${PROJECT_NAME} PROPERTIES VERSION ${PROJECT_VERSION})

set_target_properties(${PROJECT_NAME} PROPERTIES SOVERSION ${PROJECT_VERSION_MAJOR})

install(TARGETS ${PROJECT_NAME} LIBRARY DESTINATION .)
