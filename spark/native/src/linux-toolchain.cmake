# Specify the target system
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR x86_64)

# Find the toolchain's installation directory
execute_process(
  COMMAND brew --prefix x86_64-unknown-linux-gnu
  OUTPUT_VARIABLE TOOLCHAIN_PREFIX
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Specify the cross-compilers
set(CMAKE_C_COMPILER ${TOOLCHAIN_PREFIX}/bin/x86_64-unknown-linux-gnu-gcc)
set(CMAKE_CXX_COMPILER ${TOOLCHAIN_PREFIX}/bin/x86_64-unknown-linux-gnu-g++)

# Configure how CMake finds resources
set(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
set(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)
set(CMAKE_FIND_ROOT_PATH_MODE_PACKAGE ONLY)
