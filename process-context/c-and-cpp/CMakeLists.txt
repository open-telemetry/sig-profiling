cmake_minimum_required(VERSION 3.10)
project(otel_process_ctx C CXX)

# Set C standard
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Set C++ standard for C++ variants
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Note: We build here with both a C compiler as well as a C++ compiler because this implementation is expected to be
# used with either, and we want to make sure we build cleanly on either (so we don't accidentally add C-isms that C++
# doesn't like, or vice-versa)

# Compiler flags
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -O2 -fPIC -ggdb3")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2 -fPIC -ggdb3")

# Source files
set(SOURCES otel_process_ctx.c)
set(SOURCES_CXX otel_process_ctx.cpp)

# Create shared library
add_library(otel_process_ctx SHARED ${SOURCES})
set_target_properties(otel_process_ctx PROPERTIES
    OUTPUT_NAME "otel_process_ctx"
)

# Create static library
add_library(otel_process_ctx_static STATIC ${SOURCES})
set_target_properties(otel_process_ctx_static PROPERTIES
    OUTPUT_NAME "otel_process_ctx"
)

# Create shared noop library
add_library(otel_process_ctx_noop SHARED ${SOURCES})
set_target_properties(otel_process_ctx_noop PROPERTIES
    OUTPUT_NAME "otel_process_ctx_noop"
)
target_compile_definitions(otel_process_ctx_noop PRIVATE OTEL_PROCESS_CTX_NOOP=1)

# Create static noop library
add_library(otel_process_ctx_noop_static STATIC ${SOURCES})
set_target_properties(otel_process_ctx_noop_static PROPERTIES
    OUTPUT_NAME "otel_process_ctx_noop"
)
target_compile_definitions(otel_process_ctx_noop_static PRIVATE OTEL_PROCESS_CTX_NOOP=1)

# Create C++ shared library
add_library(otel_process_ctx_cpp SHARED ${SOURCES_CXX})
set_target_properties(otel_process_ctx_cpp PROPERTIES
    OUTPUT_NAME "otel_process_ctx_cpp"
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
)

# Create C++ static library
add_library(otel_process_ctx_cpp_static STATIC ${SOURCES_CXX})
set_target_properties(otel_process_ctx_cpp_static PROPERTIES
    OUTPUT_NAME "otel_process_ctx_cpp"
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
)

# Create C++ shared noop library
add_library(otel_process_ctx_cpp_noop SHARED ${SOURCES_CXX})
set_target_properties(otel_process_ctx_cpp_noop PROPERTIES
    OUTPUT_NAME "otel_process_ctx_cpp_noop"
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
)
target_compile_definitions(otel_process_ctx_cpp_noop PRIVATE OTEL_PROCESS_CTX_NOOP=1)

# Create C++ static noop library
add_library(otel_process_ctx_cpp_noop_static STATIC ${SOURCES_CXX})
set_target_properties(otel_process_ctx_cpp_noop_static PROPERTIES
    OUTPUT_NAME "otel_process_ctx_cpp_noop"
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON
)
target_compile_definitions(otel_process_ctx_cpp_noop_static PRIVATE OTEL_PROCESS_CTX_NOOP=1)

# Set include directories
target_include_directories(otel_process_ctx PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(otel_process_ctx_static PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(otel_process_ctx_noop PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(otel_process_ctx_noop_static PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(otel_process_ctx_cpp PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(otel_process_ctx_cpp_static PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(otel_process_ctx_cpp_noop PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(otel_process_ctx_cpp_noop_static PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Create example executable
add_executable(example_ctx example_ctx.c)
target_link_libraries(example_ctx otel_process_ctx)

# Create example noop executable
add_executable(example_ctx_noop example_ctx.c)
target_link_libraries(example_ctx_noop otel_process_ctx_noop)

# Install rules
install(TARGETS otel_process_ctx otel_process_ctx_static otel_process_ctx_noop otel_process_ctx_noop_static
                otel_process_ctx_cpp otel_process_ctx_cpp_static otel_process_ctx_cpp_noop otel_process_ctx_cpp_noop_static
                example_ctx example_ctx_noop
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)
install(FILES otel_process_ctx.h DESTINATION include)
